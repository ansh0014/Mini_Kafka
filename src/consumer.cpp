#include "mini_kafka/consumer.hpp"

#include "mini_kafka/logger.hpp"
#include "mini_kafka/metrics.hpp"
#include "mini_kafka/topic.hpp"

#include <chrono>
#include <stdexcept>
#include <utility>

namespace mini_kafka {

Consumer::Consumer(std::string id,
                   std::shared_ptr<Topic> topic,
                   Metrics& metrics,
                   Logger& logger,
                   Handler handler)
    : id_(std::move(id)), topic_(std::move(topic)), metrics_(&metrics), logger_(&logger), handler_(std::move(handler)) {
    if (id_.empty()) {
        throw std::invalid_argument("consumer id cannot be empty");
    }

    if (!topic_) {
        throw std::invalid_argument("consumer topic cannot be null");
    }
}

Consumer::~Consumer() {
    join();
}

void Consumer::start() {
    if (worker_.joinable()) {
        throw std::logic_error("consumer already started");
    }

    worker_ = std::thread([this] {
        run();
    });
}

void Consumer::join() {
    if (worker_.joinable()) {
        worker_.join();
    }
}

const std::string& Consumer::id() const noexcept {
    return id_;
}

std::size_t Consumer::processed_count() const noexcept {
    return processed_count_.load(std::memory_order_relaxed);
}

void Consumer::run() {
    metrics_->consumer_started();
    logger_->info("consumer " + id_ + " started");

    Message message;

    while (topic_->wait_consume(message)) {
        try {
            if (handler_) {
                handler_(message);
            }

            processed_count_.fetch_add(1, std::memory_order_relaxed);
            metrics_->record_consumed();
            logger_->debug("consumer " + id_ + " processed message " + std::to_string(message.sequence));
        } catch (const std::exception& exception) {
            metrics_->record_error();
            logger_->error("consumer " + id_ + " failed: " + exception.what());
        }
    }

    metrics_->consumer_stopped();
    logger_->info("consumer " + id_ + " stopped");
}

}  // namespace mini_kafka