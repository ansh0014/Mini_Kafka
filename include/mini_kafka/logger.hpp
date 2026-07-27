#pragma once

#include "mini_kafka/concurrent_queue.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <iostream>
#include <mutex>
#include <ostream>
#include <string>
#include <thread>

namespace mini_kafka {

class Logger {
public:
    enum class Level {
        Debug,
        Info,
        Warning,
        Error,
    };

    struct Record {
        Level level{Level::Info};
        std::string message;
        std::chrono::system_clock::time_point timestamp{std::chrono::system_clock::now()};
    };

    explicit Logger(std::ostream& output = std::clog);
    ~Logger();

    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;

    void log(Level level, std::string message);
    void debug(std::string message);
    void info(std::string message);
    void warn(std::string message);
    void error(std::string message);

    void stop();

private:
    void run();
    std::string format(const Record& record) const;
    static std::string level_to_string(Level level);

    std::ostream* output_{nullptr};
    mutable std::mutex output_mutex_;
    ConcurrentQueue<Record> records_;
    std::thread worker_;
    std::atomic<bool> running_{false};
};

}  // namespace mini_kafka