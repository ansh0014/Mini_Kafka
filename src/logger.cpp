#include "mini_kafka/logger.hpp"

#include <chrono>
#include <iomanip>
#include <sstream>

namespace mini_kafka {

Logger::Logger(std::ostream& output)
    : output_(&output), running_(true), worker_([this] { run(); }) {}

Logger::~Logger() {
    stop();
}

void Logger::log(Level level, std::string message) {
    if (!running_.load(std::memory_order_relaxed)) {
        return;
    }

    records_.push(Record{level, std::move(message), std::chrono::system_clock::now()});
}

void Logger::debug(std::string message) {
    log(Level::Debug, std::move(message));
}

void Logger::info(std::string message) {
    log(Level::Info, std::move(message));
}

void Logger::warn(std::string message) {
    log(Level::Warning, std::move(message));
}

void Logger::error(std::string message) {
    log(Level::Error, std::move(message));
}

void Logger::stop() {
    running_.store(false, std::memory_order_relaxed);
    records_.close();

    if (worker_.joinable()) {
        worker_.join();
    }
}

void Logger::run() {
    Record record;

    while (records_.wait_and_pop(record)) {
        const std::string line = format(record);

        {
            std::lock_guard<std::mutex> lock(output_mutex_);
            (*output_) << line << '\n';
            output_->flush();
        }
    }
}

std::string Logger::format(const Record& record) const {
    const auto timestamp = std::chrono::system_clock::to_time_t(record.timestamp);
    std::ostringstream stream;
    stream << '[' << std::put_time(std::localtime(&timestamp), "%F %T") << "] "
           << '[' << level_to_string(record.level) << "] "
           << record.message;
    return stream.str();
}

std::string Logger::level_to_string(Level level) {
    switch (level) {
        case Level::Debug:
            return "DEBUG";
        case Level::Info:
            return "INFO";
        case Level::Warning:
            return "WARN";
        case Level::Error:
            return "ERROR";
    }

    return "INFO";
}

}  // namespace mini_kafka