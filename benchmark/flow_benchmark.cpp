#include "flow_test_state.h"
#include "xfr_channel.h"
#include <algorithm>
#include <array>
#include <bit>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <future>
#include <iomanip>
#include <iostream>
#include <limits>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <vector>
#ifdef __linux__
#include <sys/resource.h>
#include <unistd.h>
#endif

TEST_MEMBER(FlowProtocol, payload_pool_)
TEST_MEMBER(LnkChannel, acceptor_)
TEST_MEMBER(LnkChannel, connections_)
using PayloadPool = lnk::PayloadPool;
TEST_MEMBER(PayloadPool, live_)
TEST_MEMBER(PayloadPool, retained_)

namespace
{
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;
using tcp = asio::ip::tcp;
namespace fs = std::filesystem;

void require(bool value, const std::string &reason)
{
    if (!value)
        throw std::runtime_error(reason);
}

void publish(const fs::path &file, const njson &value)
{
    {
        std::ofstream out(file.string() + ".tmp");
        out.exceptions(std::ios::failbit | std::ios::badbit);
        out << value.dump(2) << '\n';
    }
    fs::rename(file.string() + ".tmp", file);
}

struct Options
{
    unsigned flows = 100;
    unsigned nodes = 3;
    bool duplex = false;
    std::size_t chunk = 32768;
    double warmup = 2;
    double phase_seconds = 5;
    unsigned pause_ms = 0;
    std::vector<unsigned> rates{0};
    fs::path output = FLOW_BENCHMARK_RESULTS_DIR;
    fs::path network_control;
};

Options options(int argc, char **argv)
{
    Options o;
    for (int i = 1; i < argc; ++i)
    {
        const std::string key = argv[i];
        if (key == "--help")
        {
            std::cout << "benchmark_flow [--flows 100] [--nodes 3] [--chunk-bytes 32768]\n"
                         "  [--warmup-seconds 2] [--phase-seconds 5] [--rates-mbps 1000,100,500]\n"
                         "  [--pause-flow-ms 0] [--duplex 0|1] [--output DIRECTORY]\n"
                         "Rates are aggregate limits per direction; 0 is unlimited.\n"
                         "Use run_flow_network.py for actual per-hop RTT/bandwidth shaping.\n";
            std::exit(0);
        }
        require(i + 1 < argc, key + " requires a value");
        const std::string value = argv[++i];
        std::size_t used = 0;
        if (key == "--output") o.output = value;
        else if (key == "--network-control") o.network_control = value;
        else if (key == "--rates-mbps")
        {
            o.rates.clear();
            std::istringstream input(value);
            std::string item;
            while (std::getline(input, item, ','))
            {
                const auto rate = std::stoul(item, &used);
                require(used == item.size() && rate <= 10000, "invalid rate");
                o.rates.push_back(static_cast<unsigned>(rate));
            }
            require(!o.rates.empty(), "empty rate schedule");
        }
        else
        {
            const auto number = std::stoul(value, &used);
            require(used == value.size(), "invalid integer for " + key);
            if (key == "--flows") { require(number >= 1 && number <= 100, "flows must be 1..100"); o.flows = number; }
            else if (key == "--nodes") { require(number >= 2 && number <= 8, "nodes must be 2..8"); o.nodes = number; }
            else if (key == "--chunk-bytes") { require(number >= 16 && number <= lnk::FlowStream::maximum_chunk_size, "invalid chunk size"); o.chunk = number; }
            else if (key == "--warmup-seconds") { require(number <= 30, "warmup must be <=30"); o.warmup = number; }
            else if (key == "--phase-seconds") { require(number >= 1 && number <= 120, "phase duration must be 1..120"); o.phase_seconds = number; }
            else if (key == "--pause-flow-ms") { require(number <= 10000, "pause must be <=10000 ms"); o.pause_ms = number; }
            else if (key == "--duplex") { require(number <= 1, "duplex must be 0 or 1"); o.duplex = number; }
            else throw std::invalid_argument("unknown option " + key);
        }
    }
    require(o.rates.size() <= 16, "at most 16 phases");
    return o;
}

asio::awaitable<void> until(Clock::time_point deadline)
{
    asio::steady_timer timer(co_await asio::this_coro::executor);
    timer.expires_at(deadline);
    co_await timer.async_wait(asio::use_awaitable);
}

double seconds(Clock::duration duration)
{
    return std::chrono::duration<double>(duration).count();
}

Clock::duration duration(double seconds)
{
    return std::chrono::duration_cast<Clock::duration>(std::chrono::duration<double>(seconds));
}

double cpu_seconds()
{
#ifdef __linux__
    rusage usage{};
    require(getrusage(RUSAGE_SELF, &usage) == 0, "getrusage failed");
    return usage.ru_utime.tv_sec + usage.ru_utime.tv_usec / 1e6 +
           usage.ru_stime.tv_sec + usage.ru_stime.tv_usec / 1e6;
#else
    return 0;
#endif
}

std::size_t rss_bytes()
{
#ifdef __linux__
    std::ifstream stat("/proc/self/statm");
    std::size_t total = 0, resident = 0;
    stat >> total >> resident;
    return resident * static_cast<std::size_t>(sysconf(_SC_PAGESIZE));
#else
    return 0;
#endif
}

struct Phase
{
    std::uint64_t received = 0;
    std::uint64_t sent = 0;
    std::array<std::uint64_t, 32> waits{};
};

struct Pair
{
    std::shared_ptr<lnk::FlowStream> source, sink;
    std::uint64_t sent = 0, received = 0;
    std::vector<Phase> phases;
};

class Benchmark
{
  public:
    Benchmark(asio::io_context &io, Options options) : io_(io), options_(std::move(options)), pairs_(options_.flows * (options_.duplex ? 2 : 1))
    {
        fs::create_directories(options_.output);
        for (auto &pair : pairs_) pair.phases.resize(options_.rates.size());
    }

    asio::awaitable<void> run()
    {
        std::exception_ptr failure;
        try
        {
            co_await setup();
            co_await measure();
        }
        catch (...)
        {
            failure = std::current_exception();
        }
        stopping_ = true;
        for (auto &node : nodes_) co_await node->stop();
        // A test receiver can still be inside its independent limiter or pause timer.
        // Keep pumping the executor before retrieving its result.
        auto pending = [](auto &f) { return f.valid() && f.wait_for(0s) != std::future_status::ready; };
        while (std::ranges::any_of(workers_, pending) || std::ranges::any_of(watchers_, pending))
            co_await until(Clock::now() + 10ms);
        for (auto &task : workers_)
        {
            try { if (task.valid()) task.get(); }
            catch (...) { if (!failure) failure = std::current_exception(); }
        }
        for (auto &task : watchers_)
        {
            try { task.get(); }
            catch (...) { if (!failure) failure = std::current_exception(); }
        }
        bool released = true;
        for (auto &node : nodes_)
        {
            const auto pool = test_access::FlowProtocol_payload_pool_(*node);
            released &= test_access::PayloadPool_live_(*pool) == 0;
        }
        metadata_["payloads_released_after_stop"] = released;
        if (!released && !failure) failure = std::make_exception_ptr(std::runtime_error("stop retained live payloads"));
        write_summary(failure);
        if (failure) std::rethrow_exception(failure);
        std::cout << "Saved " << options_.output << "/{timeline.csv,flows.csv,summary.json}\n";
    }

  private:
    int phase(Clock::time_point now) const
    {
        const double elapsed = seconds(now - start_) - options_.warmup;
        if (elapsed < 0) return -1;
        const auto index = static_cast<std::size_t>(elapsed / options_.phase_seconds);
        return index < options_.rates.size() ? static_cast<int>(index) : -1;
    }

    unsigned rate(Clock::time_point now) const
    {
        const auto index = phase(now);
        return now >= end_ ? options_.rates.back() : options_.rates[index < 0 ? 0 : index];
    }

    asio::awaitable<void> watch(std::shared_ptr<FlowProtocol> node)
    {
        try
        {
            for (;;)
            {
                auto event = co_await node->receive_event();
                if (event.kind == lnk::EventKind::LinkReady) ++ready_;
                if (!stopping_ && (event.kind == lnk::EventKind::LinkError || event.kind == lnk::EventKind::FlowError ||
                                   event.kind == lnk::EventKind::LinkClosed || event.kind == lnk::EventKind::FlowClosed))
                    errors_.push_back(event.stage + ": " + event.reason);
            }
        }
        catch (const asio::system_error &error)
        {
            if (!stopping_) errors_.push_back(error.what());
        }
    }

    asio::awaitable<void> setup()
    {
        for (unsigned i = 0; i < options_.nodes; ++i)
        {
            const auto address = "127.0.0." + std::to_string(i + 1);
            const auto ip = asio::ip::make_address(address);
            tcp::acceptor tcp_probe(io_, {ip, 0});
            const auto tcp_port = tcp_probe.local_endpoint().port();
            tcp_probe.close();
            asio::ip::udp::socket udp_probe(io_, {ip, 0});
            const auto udp_port = udp_probe.local_endpoint().port();
            udp_probe.close();
            auto node = std::make_shared<FlowProtocol>(io_.get_executor(), io_.get_executor(), std::to_string(i),
                                                     address, tcp_port, address, udp_port);
            nodes_.push_back(node);
            node->links().start();
            node->activate();
            watchers_.push_back(asio::co_spawn(io_, watch(node), asio::use_future));
        }
        if (!options_.network_control.empty())
        {
            publish_listeners();
            const auto deadline = Clock::now() + 10s;
            while (!fs::exists(options_.network_control / "network_ready"))
            {
                require(Clock::now() < deadline, "network runner did not configure listeners");
                co_await until(Clock::now() + 10ms);
            }
        }
        for (unsigned i = 0; i + 1 < nodes_.size(); ++i)
        {
            for (const auto [here, peer] : {std::pair{i, i + 1}, std::pair{i + 1, i}})
            {
                const auto peer_port = test_access::LnkChannel_acceptor_(nodes_[peer]->links()).local_endpoint().port();
                nodes_[here]->links().prepare({i + 1, 7, RelayProtocol::Tcp, std::to_string(peer),
                    "127.0.0." + std::to_string(peer + 1), "benchmark-link-" + std::to_string(i), peer_port, 0, here == i});
                nodes_[here]->links().connect(i + 1);
            }
        }
        const auto timeout = Clock::now() + 5s;
        while (ready_ != 2 * (nodes_.size() - 1))
        {
            require(errors_.empty() && Clock::now() < timeout, "NodeLink setup failed");
            co_await until(Clock::now() + 5ms);
        }
        for (unsigned id = 1; id <= options_.flows; ++id)
        {
            for (unsigned i = 0; i < nodes_.size(); ++i)
            {
                lnk::FlowSpec spec{id, 7, RelayProtocol::Tcp};
                if (i) { spec.previous = i; spec.previous_peer = std::to_string(i - 1); }
                if (i + 1 < nodes_.size()) { spec.next = i + 1; spec.next_peer = std::to_string(i + 1); }
                nodes_[i]->prepare_flow(std::move(spec));
                nodes_[i]->commit_flow(id);
            }
            pairs_[id - 1].source = nodes_.front()->bind_stream_flow(7, id, false, io_.get_executor());
            pairs_[id - 1].sink = nodes_.back()->bind_stream_flow(7, id, true, io_.get_executor());
            if (options_.duplex)
            {
                pairs_[options_.flows + id - 1].source = pairs_[id - 1].sink;
                pairs_[options_.flows + id - 1].sink = pairs_[id - 1].source;
            }
        }
        if (!options_.network_control.empty())
        {
            njson sockets = njson::array();
            for (unsigned i = 0; i + 1 < nodes_.size(); ++i)
            {
                auto &socket = *test_access::LnkChannel_connections_(nodes_[i]->links()).at(i + 1)->socket;
                const auto local = socket.local_endpoint(), remote = socket.remote_endpoint();
                sockets.push_back({{"src", local.address().to_string()}, {"sport", local.port()},
                                   {"dst", remote.address().to_string()}, {"dport", remote.port()}});
            }
            fs::create_directories(options_.network_control);
            const auto file = options_.network_control / "ready.json";
            const njson ready{{"connections", sockets}};
            publish(file, ready);
            const auto deadline = Clock::now() + 10s;
            while (!fs::exists(options_.network_control / "start"))
            {
                require(Clock::now() < deadline, "network runner did not start measurement");
                co_await until(Clock::now() + 10ms);
            }
        }
    }

    void publish_listeners()
    {
        njson listeners = njson::array();
        for (unsigned i = 1; i < nodes_.size(); ++i)
        {
            const auto endpoint = test_access::LnkChannel_acceptor_(nodes_[i]->links()).local_endpoint();
            listeners.push_back({{"address", endpoint.address().to_string()}, {"port", endpoint.port()}});
        }
        fs::create_directories(options_.network_control);
        publish(options_.network_control / "listeners.json", listeners);
    }

    asio::awaitable<void> send(unsigned index)
    {
        auto &pair = pairs_[index];
        std::vector<std::uint8_t> pattern(options_.chunk, 0xa5);
        std::uint64_t sequence = 0;
        while (Clock::now() < end_)
        {
            std::memcpy(pattern.data(), &sequence, sizeof(sequence));
            const std::uint64_t id = index;
            std::memcpy(pattern.data() + 8, &id, sizeof(id));
            const auto began = Clock::now();
            co_await pair.source->send(pair.source->copy_payload(pattern));
            const auto now = Clock::now();
            pair.sent += options_.chunk;
            ++sequence;
            if (const auto p = phase(now); p >= 0)
            {
                auto &stats = pair.phases[p];
                stats.sent += options_.chunk;
                const auto us = std::chrono::duration_cast<std::chrono::microseconds>(now - began).count();
                ++stats.waits[std::min<std::size_t>(31, std::bit_width(static_cast<std::uint64_t>(us)))];
            }
        }
        co_await pair.source->finish_send();
    }

    asio::awaitable<void> receive(unsigned index)
    {
        auto &pair = pairs_[index];
        const std::vector<std::uint8_t> pattern(options_.chunk, 0xa5);
        std::uint64_t sequence = 0;
        unsigned previous_rate = std::numeric_limits<unsigned>::max();
        TokenBucket limiter;
        bool paused = false;
        while (auto payload = co_await pair.sink->receive())
        {
            require(payload->size() == options_.chunk, "payload size changed");
            std::uint64_t observed_sequence, observed_id;
            std::memcpy(&observed_sequence, payload->data(), 8);
            std::memcpy(&observed_id, payload->data() + 8, 8);
            require(observed_sequence == sequence++ && observed_id == index &&
                    std::memcmp(payload->data() + 16, pattern.data() + 16, options_.chunk - 16) == 0,
                    "payload corruption, loss or reordering");
            if (index == 0 && !paused && options_.pause_ms && Clock::now() >= start_ + duration(options_.warmup))
            {
                paused = true;
                pause_begin_ = Clock::now();
                pause_end_ = pause_begin_ + std::chrono::milliseconds(options_.pause_ms);
                co_await until(pause_end_);
            }
            if (options_.network_control.empty())
            {
                const auto requested = rate(Clock::now());
                if (previous_rate != requested)
                {
                    // Only replace a test sink's bucket after its prior consume has completed.
                    // Production has no live rate-change API; this models downstream speed changes.
                    limiter = TokenBucket(static_cast<std::uint64_t>(requested) * 125000 / options_.flows, 0);
                    previous_rate = requested;
                }
                co_await limiter.consume(payload->size());
            }
            const auto bytes = payload->size();
            payload.reset();
            pair.sink->complete_receive();
            pair.received += bytes;
            if (const auto p = phase(Clock::now()); p >= 0) pair.phases[p].received += bytes;
        }
    }

    void begin_measurement()
    {
        start_ = Clock::now();
        end_ = start_ + duration(options_.warmup + options_.rates.size() * options_.phase_seconds);
        metadata_ = {{"build", FLOW_BENCHMARK_BUILD}, {"compiler", __VERSION__}, {"flows", options_.flows},
                     {"nodes", options_.nodes}, {"chunk_bytes", options_.chunk}, {"warmup_seconds", options_.warmup},
                     {"phase_seconds", options_.phase_seconds}, {"rates_mbps", options_.rates},
                     {"pause_flow_ms", options_.pause_ms}, {"mode", options_.network_control.empty() ? "sink_consume" : "network_netem"},
                     {"directions", options_.duplex ? 2 : 1},
                     {"execution", "one process, one io_context, one thread"},
                     {"start_monotonic_ns", std::chrono::duration_cast<std::chrono::nanoseconds>(start_.time_since_epoch()).count()}};
        publish(options_.output / "metadata.json", metadata_);
    }

    asio::awaitable<void> measure()
    {
        begin_measurement();
        for (unsigned i = 0; i < pairs_.size(); ++i)
        {
            workers_.push_back(asio::co_spawn(io_, send(i), asio::use_future));
            workers_.push_back(asio::co_spawn(io_, receive(i), asio::use_future));
        }
        std::ofstream csv(options_.output / "timeline.csv");
        csv << "elapsed_s,interval_s,phase,target_mbps,received_bytes,received_mbps,sent_mbps,cpu_pct,rss_bytes,slow_flow_paused,healthy_received_mbps";
        for (unsigned i = 0; i < nodes_.size(); ++i)
            csv << ",buffered_" << i << ",pool_live_" << i << ",pool_retained_" << i;
        csv << '\n' << std::fixed << std::setprecision(6);
        auto previous = start_;
        double previous_cpu = cpu_seconds();
        std::uint64_t previous_received = 0, previous_sent = 0;
        std::uint64_t previous_slow_received = 0;
        peak_buffers_.resize(nodes_.size());
        while (previous < end_)
        {
            const auto p = phase(previous);
            const auto boundary = p < 0 ? start_ + duration(options_.warmup)
                : start_ + duration(options_.warmup + (p + 1) * options_.phase_seconds);
            co_await until(std::min(previous + 100ms, std::min(boundary, end_)));
            const auto now = Clock::now();
            const auto dt = seconds(now - previous);
            const auto cpu = cpu_seconds();
            std::uint64_t received = 0, sent = 0;
            for (const auto &pair : pairs_) { received += pair.received; sent += pair.sent; }
            const auto delta = received - previous_received;
            const auto healthy_delta = delta - (pairs_.front().received - previous_slow_received);
            const bool paused = previous >= pause_begin_ && now <= pause_end_;
            if (paused) paused_healthy_bytes_ += healthy_delta;
            csv << seconds(now - start_) << ',' << dt << ',' << p << ',' << rate(previous) * (options_.duplex ? 2 : 1) << ',' << delta << ','
                << delta * 8.0 / dt / 1e6 << ',' << (sent - previous_sent) * 8.0 / dt / 1e6 << ','
                << (cpu - previous_cpu) / dt * 100 << ',' << rss_bytes() << ',' << paused << ',' << healthy_delta * 8.0 / dt / 1e6;
            for (unsigned i = 0; i < nodes_.size(); ++i)
            {
                const auto buffered = test_access::flow_buffered_bytes(*nodes_[i]);
                const auto pool = test_access::FlowProtocol_payload_pool_(*nodes_[i]);
                peak_buffers_[i] = std::max(peak_buffers_[i], buffered);
                // Each active direction also permits one ordered FIN.
                require(buffered <= pairs_.size() * (test_flow::window_bytes + lnk::Frame::buffer_overhead), "Flow cache exceeded fixed windows");
                for (const auto &[id, flow] : test_access::FlowProtocol_flows_(*nodes_[i]))
                    for (const auto &direction : flow->stream->directions)
                        require(direction.buffered_bytes <= test_flow::window_bytes + lnk::Frame::buffer_overhead,
                                "individual Flow direction exceeded its window");
                csv << ',' << buffered << ',' << test_access::PayloadPool_live_(*pool) << ',' << test_access::PayloadPool_retained_(*pool);
            }
            csv << '\n';
            previous = now; previous_cpu = cpu; previous_received = received; previous_sent = sent;
            previous_slow_received = pairs_.front().received;
            require(errors_.empty(), errors_.empty() ? "" : errors_.front());
            for (auto &task : workers_)
                if (task.valid() && task.wait_for(0s) == std::future_status::ready) task.get();
        }
        const auto drain_deadline = Clock::now() + 10s;
        while (std::ranges::any_of(workers_, [](auto &f) { return f.valid() && f.wait_for(0s) != std::future_status::ready; }))
        {
            require(errors_.empty() && Clock::now() < drain_deadline, "drain failed or timed out");
            co_await until(Clock::now() + 10ms);
        }
        for (auto &task : workers_) if (task.valid()) task.get();
        workers_.clear();
        for (const auto &pair : pairs_) require(pair.sent == pair.received, "drained byte totals differ");
        if (options_.pause_ms && pairs_.size() > 1)
            require(paused_healthy_bytes_ > 0, "other Flows made no progress while the slow Flow was paused");
    }

    void write_summary(std::exception_ptr failure)
    {
        std::ofstream flows(options_.output / "flows.csv");
        flows << "phase,flow,received_bytes,sent_bytes,send_wait_p50_upper_us,send_wait_p95_upper_us,send_wait_p99_upper_us\n";
        for (unsigned p = 0; p < options_.rates.size(); ++p)
            for (unsigned i = 0; i < pairs_.size(); ++i)
            {
                const auto &stats = pairs_[i].phases[p];
                auto quantile = [&](unsigned percent) {
                    const auto count = std::accumulate(stats.waits.begin(), stats.waits.end(), std::uint64_t{});
                    if (!count) return std::uint64_t{};
                    std::uint64_t seen = 0;
                    for (unsigned bin = 0; bin < stats.waits.size(); ++bin)
                    {
                        seen += stats.waits[bin];
                        if (seen >= (count * percent + 99) / 100) return (std::uint64_t{1} << bin) - 1;
                    }
                    return std::uint64_t{};
                };
                flows << p << ',' << i << ',' << stats.received << ',' << stats.sent << ','
                      << quantile(50) << ',' << quantile(95) << ',' << quantile(99) << '\n';
            }
        metadata_["status"] = failure ? "failed" : "passed";
        metadata_["peak_buffered_bytes_by_node"] = peak_buffers_;
        metadata_["errors"] = errors_;
        metadata_["healthy_bytes_while_slow_flow_paused"] = paused_healthy_bytes_;
        if (failure)
        {
            try { std::rethrow_exception(failure); }
            catch (const std::exception &error) { metadata_["failure"] = error.what(); }
        }
        std::uint64_t total_sent = 0, total_received = 0;
        for (const auto &pair : pairs_) { total_sent += pair.sent; total_received += pair.received; }
        metadata_["total_sent_bytes_including_warmup_and_drain"] = total_sent;
        metadata_["total_received_bytes_including_warmup_and_drain"] = total_received;
        std::ofstream out(options_.output / "summary.json"); out << metadata_.dump(2) << '\n';
    }

    asio::io_context &io_;
    Options options_;
    std::vector<std::shared_ptr<FlowProtocol>> nodes_;
    std::vector<Pair> pairs_;
    std::vector<std::future<void>> workers_, watchers_;
    std::vector<std::string> errors_;
    std::vector<std::size_t> peak_buffers_;
    njson metadata_;
    Clock::time_point start_, end_;
    Clock::time_point pause_begin_, pause_end_;
    std::uint64_t paused_healthy_bytes_ = 0;
    unsigned ready_ = 0;
    bool stopping_ = false;
};
} // namespace

int main(int argc, char **argv)
{
    try
    {
        asio::io_context io(1);
        Benchmark benchmark(io, options(argc, argv));
        auto result = asio::co_spawn(io, benchmark.run(), asio::use_future);
        io.run();
        result.get();
    }
    catch (const std::exception &error)
    {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return 1;
    }
}
