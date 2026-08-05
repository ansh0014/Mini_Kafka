#pragma once

#include "mini_kafka/message.hpp"

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <thread>

namespace mini_kafka {

class Logger;
class Metrics;
class Topic;
class ThreadPool;

class Consumer {
public:
    using Handler = std::function<void(const Message&)>;

    Consumer(std::string id,
             std::shared_ptr<Topic> topic,
             Metrics& metrics,
             Logger& logger,
             Handler handler = {},
             ThreadPool* thread_pool = nullptr);
    ~Consumer();

    Consumer(const Consumer&) = delete;
    Consumer& operator=(const Consumer&) = delete;

    void start();
    void join();

    const std::string& id() const noexcept;
    std::size_t processed_count() const noexcept;

private:
    void run();

    std::string id_;
    std::shared_ptr<Topic> topic_;
    Metrics* metrics_{nullptr};
    Logger* logger_{nullptr};
    Handler handler_;
    ThreadPool* thread_pool_{nullptr};
    std::thread worker_;
    std::atomic<std::size_t> processed_count_{0};
};

}