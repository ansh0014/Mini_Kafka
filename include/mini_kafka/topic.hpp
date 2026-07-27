#pragma once

#include "mini_kafka/concurrent_queue.hpp"
#include "mini_kafka/message.hpp"

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>

namespace mini_kafka {

class Topic {
public:
    explicit Topic(std::string name);

    Topic(const Topic&) = delete;
    Topic& operator=(const Topic&) = delete;

    const std::string& name() const noexcept;

    void publish(Message message);
    bool wait_consume(Message& message);

    template <typename Rep, typename Period>
    bool wait_consume_for(Message& message, const std::chrono::duration<Rep, Period>& timeout) {
        return queue_.wait_and_pop_for(message, timeout);
    }

    void close();
    std::size_t size() const;

private:
    std::string name_;
    ConcurrentQueue<Message> queue_;
};

using TopicPtr = std::shared_ptr<Topic>;

}  // namespace mini_kafka