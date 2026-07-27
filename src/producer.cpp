#include "mini_kafka/producer.hpp"

#include "mini_kafka/logger.hpp"
#include "mini_kafka/metrics.hpp"
#include "mini_kafka/topic.hpp"

#include <chrono>
#include <stdexcept>
#include <thread>
#include <utility>

namespace mini_kafka {

Producer::Producer(std::string id, std::shared_ptr<Topic> topic, Metrics& metrics, Logger& logger)
    : id_(std::move(id)), topic_(std::move(topic)), metrics_(&metrics), logger_(&logger) {
    if (id_.empty()) {
        throw std::invalid_argument("producer id cannot be empty");
    }

    if (!topic_) {
        throw std::invalid_argument("producer topic cannot be null");
    }
}

Producer::~Producer() {
    join();
}

void Producer::start(std::size_t message_count, std::chrono::milliseconds interval) {
    if (worker_.joinable()) {
        throw std::logic_error("producer already started");
    }

    worker_ = std::thread([this, message_count, interval] {
        run(message_count, interval);
    });
}

void Producer::join() {
    if (worker_.joinable()) {
        worker_.join();
    }
}

const std::string& Producer::id() const noexcept {
    return id_;
}

void Producer::run(std::size_t message_count, std::chrono::milliseconds interval) {
    logger_->info("producer " + id_ + " started");

    for (std::size_t index = 0; index < message_count; ++index) {
        try {
            Message message;
            message.sequence = index + 1;
            message.producer_id = id_;
            message.payload = id_ + " payload " + std::to_string(message.sequence);
            message.created_at = std::chrono::system_clock::now();

            topic_->publish(std::move(message));
            metrics_->record_produced();
            logger_->debug("producer " + id_ + " published message " + std::to_string(index + 1));

            if (interval.count() > 0) {
                std::this_thread::sleep_for(interval);
            }
        } catch (const std::exception& exception) {
            metrics_->record_error();
            logger_->error("producer " + id_ + " failed: " + exception.what());
            break;
        }
    }

    logger_->info("producer " + id_ + " finished");
}

}  // namespace mini_kafka