#ifndef RELAYWEAVE_TEST_AGENT_STOP_H
#define RELAYWEAVE_TEST_AGENT_STOP_H

#include "relay_agent.h"

namespace test_agent
{
// Coroutine scenarios keep driving the Agent executors while an external owner blocks in stop().
inline asio::awaitable<void> stop_from_owner(std::shared_ptr<RelayAgent> agent)
{
    asio::thread_pool owner(1);
    co_await asio::co_spawn(owner,
                          [agent]() -> asio::awaitable<void> {
                              agent->stop();
                              co_return;
                          },
                          asio::use_awaitable);
}
}

#endif
