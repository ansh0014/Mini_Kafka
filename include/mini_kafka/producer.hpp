#pragma once

#include "mini_kafka/message.hpp"

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <thread>

namespace mini_kafka {

class Logger;
class Metrics;
class Topic;

class Producer {
public:
    Producer(std::string id, std::shared_ptr<Topic> topic, Metrics& metrics, Logger& logger);
    ~Producer();

    Producer(const Producer&) = delete;
    Producer& operator=(const Producer&) = delete;

    void start(std::size_t message_count, std::chrono::milliseconds interval);
    void join();

    const std::string& id() const noexcept;

private:
    void run(std::size_t message_count, std::chrono::milliseconds interval);

    std::string id_;
    std::shared_ptr<Topic> topic_;
    Metrics* metrics_{nullptr};
    Logger* logger_{nullptr};
    std::thread worker_;
};

}  // namespace mini_kafka