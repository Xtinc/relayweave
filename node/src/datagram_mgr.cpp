#include "datagram_mgr.h"
#include <algorithm>
#include <vector>

DatagramMgr::Relay::Relay(asio::any_io_executor executor, std::uint64_t uuid, std::uint64_t producer_session_id,
                          std::uint64_t consumer_session_id, std::uint64_t producer_ticket,
                          std::uint64_t consumer_ticket, std::string service, const ControlSessionPtr &producer,
                          const ControlSessionPtr &consumer, std::uint64_t request_id, const TrafficLimitConfig &config,
                          SRVTrafficPtr traffic)
    : uuid(uuid), producer_session_id(producer_session_id), consumer_session_id(consumer_session_id),
      producer_session_header(DatagramHeader::encode(producer_session_id)),
      consumer_session_header(DatagramHeader::encode(consumer_session_id)), producer_ticket(producer_ticket),
      consumer_ticket(consumer_ticket), service(std::move(service)), producer(producer), consumer(consumer),
      request_id(request_id), accessor(consumer->peer()), service_wait_timer(std::move(executor)),
      rx_limiter(config.rx_bytes_per_second, config.rx_burst_bytes),
      tx_limiter(config.tx_bytes_per_second, config.tx_burst_bytes), traffic(std::move(traffic))
{
}

bool DatagramMgr::Relay::belongs_to(const ControlSessionPtr &session) const
{
    return producer.lock() == session || consumer.lock() == session;
}

bool DatagramMgr::Relay::complete() const noexcept
{
    return producer_endpoint && consumer_endpoint;
}

void DatagramMgr::Relay::cancel() noexcept
{
    try
    {
        service_wait_timer.cancel();
    }
    catch (...)
    {
    }
}

std::optional<DatagramMgr::udp::endpoint> &DatagramMgr::Relay::endpoint(Side side)
{
    return side == Side::Producer ? producer_endpoint : consumer_endpoint;
}

const DatagramHeader::Buffer &DatagramMgr::Relay::session_header(Side side) const
{
    return side == Side::Producer ? producer_session_header : consumer_session_header;
}

bool DatagramMgr::Relay::allow(Side side, std::size_t payload_size)
{
    return side == Side::Producer ? rx_limiter.try_consume(payload_size) : tx_limiter.try_consume(payload_size);
}

DatagramMgr::DatagramMgr(asio::any_io_executor executor, std::shared_ptr<RelayIdAllocator> id_allocator,
                         udp::endpoint listen_endpoint, std::size_t capacity,
                         std::chrono::steady_clock::duration service_wait_duration, TrafficLimitConfig config)
    : executor_(std::move(executor)), id_allocator_(std::move(id_allocator)),
      listen_endpoint_(std::move(listen_endpoint)), socket_(executor_), capacity_(capacity),
      service_wait_duration_(service_wait_duration), config_(std::move(config))
{
}

void DatagramMgr::start()
{
    if (started_ || stopped_)
    {
        throw std::logic_error("UDP manager can only be started once");
    }

    socket_.open(listen_endpoint_.protocol());
    try
    {
        socket_.set_option(asio::socket_base::reuse_address(true));
        socket_.bind(listen_endpoint_);
    }
    catch (...)
    {
        asio::error_code ignored;
        socket_.close(ignored);
        throw;
    }

    started_ = true;
    asio::co_spawn(
        executor_, [self = shared_from_this()]() -> asio::awaitable<void> { co_await self->receive_datagram(); },
        asio::detached);
}

void DatagramMgr::open(const ControlSessionPtr &consumer, std::string service, std::uint64_t request_id,
                       ControlSessionPtr producer, SRVTrafficPtr traffic)
{
    try
    {
        do_open(consumer, service, request_id, std::move(producer), std::move(traffic));
    }
    catch (const std::exception &exception)
    {
        consumer->send(CtrlMessage{CtrlCommand::RelayError, njson{{"request_id", request_id},
                                                        {"service", service},
                                                        {"protocol", "udp"},
                                                        {"reason", exception.what()}}});
    }
    catch (...)
    {
        consumer->send(CtrlMessage{CtrlCommand::RelayError, njson{{"request_id", request_id},
                                                        {"service", service},
                                                        {"protocol", "udp"},
                                                        {"reason", "unknown relay error"}}});
    }
}

void DatagramMgr::do_open(const ControlSessionPtr &consumer, std::string service, std::uint64_t request_id,
                          ControlSessionPtr producer, SRVTrafficPtr traffic)
{
    if (stopped_)
    {
        consumer->send(CtrlMessage{CtrlCommand::RelayError, njson{{"request_id", request_id},
                                                        {"service", service},
                                                        {"protocol", "udp"},
                                                        {"reason", "server stopping"}}});
        return;
    }

    if (relays_.size() >= capacity_)
    {
        consumer->send(CtrlMessage{CtrlCommand::RelayError, njson{{"request_id", request_id},
                                                        {"service", service},
                                                        {"protocol", "udp"},
                                                        {"reason", "relay capacity reached"}}});
        return;
    }

    const auto uuid = id_allocator_->allocate();
    ScopeGuard id_rollback([this, uuid]() noexcept { id_allocator_->release(uuid); });
    const auto producer_session_id = allocate_session_id();
    std::uint64_t consumer_session_id;
    do
    {
        consumer_session_id = allocate_session_id();
    } while (consumer_session_id == producer_session_id);

    const auto producer_ticket = generate_random_id();
    std::uint64_t consumer_ticket;
    do
    {
        consumer_ticket = generate_random_id();
    } while (consumer_ticket == producer_ticket);

    auto relay = std::make_shared<Relay>(executor_, uuid, producer_session_id, consumer_session_id, producer_ticket,
                                         consumer_ticket, std::move(service), producer, consumer, request_id, config_,
                                         std::move(traffic));
    relays_.emplace(uuid, relay);
    ScopeGuard relay_rollback([&]() noexcept { erase_relay(relay); });
    id_rollback.dismiss();
    bindings_.emplace(producer_session_id, Binding{uuid, Side::Producer});
    bindings_.emplace(consumer_session_id, Binding{uuid, Side::Consumer});

    if (!producer)
    {
        relay->service_wait_timer.expires_after(service_wait_duration_);
        relay->service_wait_timer.async_wait([weak_self = std::weak_ptr<DatagramMgr>(shared_from_this()),
                                              weak_relay = std::weak_ptr<Relay>(relay)](const asio::error_code &error) {
            if (!error)
            {
                if (auto self = weak_self.lock())
                {
                    if (auto relay = weak_relay.lock(); relay && relay->state == RelayState::WaitingForProducer)
                    {
                        self->terminate_relay(std::move(relay), "service wait timed out");
                    }
                }
            }
        });
    }

    const njson common{
        {"uuid", uuid}, {"service", relay->service}, {"protocol", "udp"}, {"data_port", listen_endpoint_.port()}};
    auto consumer_params = common;
    consumer_params["request_id"] = request_id;
    consumer_params["session_id"] = consumer_session_id;
    consumer_params["ticket"] = consumer_ticket;
    consumer->send(CtrlMessage{CtrlCommand::RelayOpened, std::move(consumer_params)});
    if (producer)
    {
        offer_producer(relay, producer);
    }
    relay_rollback.dismiss();
}

void DatagramMgr::attach_service(const ControlSessionPtr &producer, const std::string &service, SRVTrafficPtr traffic)
{
    if (stopped_)
    {
        return;
    }

    for (const auto &[_, relay] : relays_)
    {
        if (relay->state == RelayState::WaitingForProducer && relay->service == service)
        {
            relay->traffic = traffic;
            offer_producer(relay, producer);
        }
    }
}

void DatagramMgr::offer_producer(const std::shared_ptr<Relay> &relay, const ControlSessionPtr &producer)
{
    if (relay->state != RelayState::WaitingForProducer)
    {
        return;
    }

    if (!relay->traffic)
    {
        throw std::logic_error("UDP relay requires service traffic statistics");
    }

    relay->producer = producer;
    relay->service_wait_timer.cancel();
    relay->state = RelayState::WaitingForAttach;

    producer->send(CtrlMessage{CtrlCommand::RelayOffer, njson{{"uuid", relay->uuid},
                                                    {"service", relay->service},
                                                    {"protocol", "udp"},
                                                    {"data_port", listen_endpoint_.port()},
                                                    {"session_id", relay->producer_session_id},
                                                    {"ticket", relay->producer_ticket}}});
}

bool DatagramMgr::reject(const ControlSessionPtr &session, std::uint64_t uuid, std::string reason)
{
    if (stopped_)
    {
        return false;
    }

    const auto relay = relays_.find(uuid);
    if (relay == relays_.end() || relay->second->state != RelayState::WaitingForAttach ||
        relay->second->producer.lock() != session)
    {
        return false;
    }
    terminate_relay(relay->second, std::move(reason));
    return true;
}

bool DatagramMgr::cancel(const ControlSessionPtr &session, std::optional<std::uint64_t> uuid,
                         std::optional<std::uint64_t> request_id)
{
    if (stopped_)
    {
        return false;
    }

    for (const auto &[relay_uuid, relay] : relays_)
    {
        if (relay->belongs_to(session) &&
            ((uuid && *uuid == relay_uuid) || (request_id && *request_id == relay->request_id)))
        {
            terminate_relay(relay, "relay cancelled");
            return true;
        }
    }
    return false;
}

void DatagramMgr::disconnect(const ControlSessionPtr &session)
{
    if (stopped_)
    {
        return;
    }

    std::vector<std::shared_ptr<Relay>> relays;
    for (const auto &[_, relay] : relays_)
    {
        if (relay->belongs_to(session))
        {
            relays.push_back(relay);
        }
    }
    for (auto &relay : relays)
    {
        terminate_relay(relay, "control session disconnected");
    }
}

void DatagramMgr::stop()
{
    if (stopped_)
    {
        return;
    }

    stopped_ = true;
    asio::error_code ignored;
    socket_.close(ignored);
    bindings_.clear();
    auto relays = std::move(relays_);
    for (auto &[uuid, relay] : relays)
    {
        relay->cancel();
        if (relay->state == RelayState::Active)
        {
            PROXY_INFO_PRINT("Relay [x] udp service=%s uuid=%llu reason=node stopping", relay->service.c_str(),
                             static_cast<unsigned long long>(relay->uuid));
            relay->traffic->remove_accessor(relay->accessor);
        }
        id_allocator_->release(uuid);
    }
}

asio::awaitable<void> DatagramMgr::receive_datagram()
{
    std::array<std::uint8_t, DatagramHeader::maximum_wire_payload + 1> buffer{};
    for (;;)
    {
        udp::endpoint source;
        auto [error, size] = co_await socket_.async_receive_from(asio::buffer(buffer), source, use_nothrow_awaitable);
        if (error)
        {
            if (stopped_ || error == asio::error::operation_aborted)
            {
                co_return;
            }
            PROXY_ERROR_PRINT("Transfer receive failed udp reason=%s", error.message().c_str());
            continue;
        }

        if (size > DatagramHeader::maximum_wire_payload)
        {
            continue;
        }

        auto route = route_datagram(std::span(buffer.data(), size), source);
        if (route)
        {
            co_await forward_datagram(std::span<const std::uint8_t>(buffer.data(), size), std::move(*route));
        }
    }
}

std::optional<DatagramMgr::RoutedDatagram> DatagramMgr::route_datagram(std::span<std::uint8_t> datagram,
                                                                       const udp::endpoint &source)
{
    if (stopped_ || datagram.size() < DatagramHeader::length)
    {
        return std::nullopt;
    }

    const auto session_id = DatagramHeader::decode(datagram.first(DatagramHeader::length));
    if (!session_id)
    {
        return std::nullopt;
    }

    const auto binding = bindings_.find(*session_id);
    if (binding == bindings_.end())
    {
        std::optional<RelayAttach> attach;
        try
        {
            const auto payload_size = WireMessage::decode_length(datagram.first<WireMessage::header_length>());
            if (payload_size != datagram.size() - WireMessage::header_length)
            {
                return std::nullopt;
            }
            attach = RelayAttach::from_msg(
                CtrlMessage::deserialize(datagram.subspan(WireMessage::header_length, payload_size)));
        }
        catch (...)
        {
            return std::nullopt;
        }

        const auto owner = relays_.find(attach->uuid);
        if (owner == relays_.end() || owner->second->state != RelayState::WaitingForAttach)
        {
            return std::nullopt;
        }

        const auto &relay = owner->second;
        const auto side = attach->role == RelayAttach::Producer ? Side::Producer : Side::Consumer;
        auto &source_endpoint = relay->endpoint(side);
        const auto expected_ticket = side == Side::Producer ? relay->producer_ticket : relay->consumer_ticket;
        if (source_endpoint || attach->ticket != expected_ticket)
        {
            return std::nullopt;
        }

        source_endpoint = source;
        if (relay->complete())
        {
            start_relay(relay);
        }
        return std::nullopt;
    }

    const auto owner = relays_.find(binding->second.relay_uuid);
    if (owner == relays_.end())
    {
        return std::nullopt;
    }

    const auto &relay = owner->second;
    const auto side = binding->second.side;
    auto &source_endpoint = relay->endpoint(side);
    if (!source_endpoint)
    {
        return std::nullopt;
    }

    if (*source_endpoint != source || relay->state != RelayState::Active)
    {
        return std::nullopt;
    }

    const auto payload_size = datagram.size() - DatagramHeader::length;
    if (!relay->allow(side, payload_size))
    {
        return std::nullopt;
    }

    const auto other_side = side == Side::Producer ? Side::Consumer : Side::Producer;
    const auto &destination = relay->endpoint(other_side);
    const auto &destination_header = relay->session_header(other_side);
    std::copy(destination_header.begin(), destination_header.end(), datagram.begin());
    if (!relay->traffic)
    {
        return std::nullopt;
    }
    return RoutedDatagram{*destination, relay->traffic, side, payload_size};
}

asio::awaitable<void> DatagramMgr::forward_datagram(std::span<const std::uint8_t> datagram, RoutedDatagram route)
{
    const auto [error, sent] =
        co_await socket_.async_send_to(asio::buffer(datagram), route.destination, use_nothrow_awaitable);
    if (!error && sent == datagram.size())
    {
        auto &counter = route.source_side == Side::Producer ? route.traffic->rx : route.traffic->tx;
        counter.add(route.payload_size);
    }

    if ((error || sent != datagram.size()) && !stopped_)
    {
        PROXY_ERROR_PRINT("Transfer send failed udp -> %s:%u reason=%s", route.destination.address().to_string().c_str(),
                          static_cast<unsigned int>(route.destination.port()),
                          error ? error.message().c_str() : "partial datagram send");
    }
}

void DatagramMgr::start_relay(const std::shared_ptr<Relay> &relay)
{
    if (relay->state != RelayState::WaitingForAttach)
    {
        return;
    }

    auto producer = relay->producer.lock();
    auto consumer = relay->consumer.lock();
    if (!producer || !consumer)
    {
        terminate_relay(relay, "control session disconnected");
        return;
    }
    relay->traffic->add_accessor(relay->accessor);
    relay->state = RelayState::Active;
    PROXY_INFO_PRINT("Relay [+] udp service=%s uuid=%llu", relay->service.c_str(),
                     static_cast<unsigned long long>(relay->uuid));
    const njson ready{{"uuid", relay->uuid}, {"protocol", "udp"}};
    producer->send(CtrlMessage{CtrlCommand::RelayReady, ready});
    consumer->send(CtrlMessage{CtrlCommand::RelayReady, ready});
}

bool DatagramMgr::erase_relay(const std::shared_ptr<Relay> &relay) noexcept
{
    const auto iterator = relays_.find(relay->uuid);
    if (iterator == relays_.end() || iterator->second != relay)
    {
        return false;
    }

    const bool active = relay->state == RelayState::Active;
    relay->state = RelayState::Closing;
    const auto erase_binding = [this, relay](std::uint64_t session_id) {
        const auto binding = bindings_.find(session_id);
        if (binding != bindings_.end() && binding->second.relay_uuid == relay->uuid)
        {
            bindings_.erase(binding);
        }
    };

    erase_binding(relay->producer_session_id);
    erase_binding(relay->consumer_session_id);
    relay->cancel();
    if (active)
    {
        PROXY_INFO_PRINT("Relay [x] udp service=%s uuid=%llu", relay->service.c_str(),
                         static_cast<unsigned long long>(relay->uuid));
        relay->traffic->remove_accessor(relay->accessor);
    }
    relays_.erase(iterator);
    id_allocator_->release(relay->uuid);
    return true;
}

void DatagramMgr::terminate_relay(std::shared_ptr<Relay> relay, std::optional<std::string> reason)
{
    const bool active = relay->state == RelayState::Active;
    if (!erase_relay(relay) || !reason)
    {
        return;
    }

    PROXY_DEBUG_PRINT("Relay closing udp service=%s uuid=%llu reason=%s", relay->service.c_str(),
                      static_cast<unsigned long long>(relay->uuid), reason->c_str());
    const auto failure = std::move(*reason);
    auto consumer = relay->consumer.lock();
    if (auto producer = relay->producer.lock(); producer && producer != consumer)
    {
        producer->send(CtrlMessage{CtrlCommand::RelayClosed, njson{{"request_id", relay->request_id},
                                                         {"uuid", relay->uuid},
                                                         {"service", relay->service},
                                                         {"protocol", "udp"},
                                                         {"reason", failure}}});
    }

    if (consumer)
    {
        const auto command = active ? CtrlCommand::RelayClosed : CtrlCommand::RelayError;
        consumer->send(CtrlMessage{command, njson{{"request_id", relay->request_id},
                                                  {"uuid", relay->uuid},
                                                  {"service", relay->service},
                                                  {"protocol", "udp"},
                                                  {"reason", failure}}});
    }
}

std::uint64_t DatagramMgr::allocate_session_id() const
{
    std::uint64_t session_id;
    do
    {
        session_id = generate_random_id();
    } while (bindings_.contains(session_id));
    return session_id;
}
