#ifndef RELAYWEAVE_ASYNC_EVENT_H
#define RELAYWEAVE_ASYNC_EVENT_H

#include <algorithm>
#include <asio/any_io_executor.hpp>
#include <asio/as_tuple.hpp>
#include <asio/awaitable.hpp>
#include <asio/experimental/channel.hpp>
#include <asio/use_awaitable.hpp>
#include <list>
#include <utility>
#include <vector>

// One-shot broadcast. Access requires one executor, and the event must outlive every wait().
class AsyncEvent
{
    template <typename... Signatures> struct ChannelTraits : asio::experimental::channel_traits<Signatures...>
    {
        template <typename... OtherSignatures> struct rebind
        {
            using other = ChannelTraits<OtherSignatures...>;
        };

        // Asio stores the first error_code inline. Avoid the default deque's
        // empty allocations; this capacity-one notification needs no list nodes.
        template <typename Element> struct container
        {
            using type = std::list<Element>;
        };
    };

    struct Waiter
    {
        explicit Waiter(AsyncEvent &event) : event(event), notification(event.executor_, 1)
        {
            event.waiters_.push_back(this);
        }

        ~Waiter()
        {
            std::erase(event.waiters_, this);
        }

        AsyncEvent &event;
        asio::experimental::basic_channel<asio::any_io_executor, ChannelTraits<void(asio::error_code)>,
                                          void(asio::error_code)>
            notification;
    };

  public:
    explicit AsyncEvent(asio::any_io_executor executor) : executor_(std::move(executor))
    {
    }
    AsyncEvent(const AsyncEvent &) = delete;
    AsyncEvent &operator=(const AsyncEvent &) = delete;
    AsyncEvent(AsyncEvent &&) = delete;
    AsyncEvent &operator=(AsyncEvent &&) = delete;

    // true: notified (including before this wait); false: this wait was cancelled.
    asio::awaitable<bool> wait()
    {
        if (notified_)
        {
            co_return true;
        }
        Waiter waiter(*this);
        auto [error] = co_await waiter.notification.async_receive(asio::as_tuple(asio::use_awaitable));
        co_return !error;
    }

    void notify_all()
    {
        notified_ = true;
        for (auto *waiter : waiters_)
        {
            waiter->notification.try_send(asio::error_code{});
        }
        // This one-shot event never registers another waiter after notification.
        std::vector<Waiter *>{}.swap(waiters_);
    }

  private:
    asio::any_io_executor executor_;
    std::vector<Waiter *> waiters_;
    bool notified_ = false;
};

#endif
