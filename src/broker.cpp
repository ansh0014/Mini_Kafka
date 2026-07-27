#include "mini_kafka/broker.hpp"

#include <chrono>
#include <stdexcept>
#include <utility>

namespace mini_kafka {

Broker::Broker(std::ostream& log_stream)
    : logger_(log_stream) {}

Broker::~Broker() {
    shutdown();
}

TopicPtr Broker::create_topic(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (name.empty()) {
        throw std::invalid_argument("topic name cannot be empty");
    }

    auto [iterator, inserted] = topics_.emplace(name, std::make_shared<Topic>(name));
    if (!inserted) {
        return iterator->second;
    }

    logger_.info("created topic " + name);
    return iterator->second;
}

TopicPtr Broker::topic(const std::string& name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto iterator = topics_.find(name);
    return iterator == topics_.end() ? TopicPtr{} : iterator->second;
}

Producer& Broker::launch_producer(std::string id,
                                  const std::string& topic_name,
                                  std::size_t message_count,
                                  std::chrono::milliseconds interval) {
    auto target = topic_locked(topic_name);
    auto producer = std::make_unique<Producer>(std::move(id), std::move(target), metrics_, logger_);
    Producer& reference = *producer;
    producer->start(message_count, interval);

    std::lock_guard<std::mutex> lock(mutex_);
    producers_.push_back(std::move(producer));
    return reference;
}

Consumer& Broker::launch_consumer(std::string id,
                                  const std::string& topic_name,
                                  Consumer::Handler handler) {
    auto target = topic_locked(topic_name);
    auto consumer = std::make_unique<Consumer>(std::move(id), std::move(target), metrics_, logger_, std::move(handler));
    Consumer& reference = *consumer;
    consumer->start();

    std::lock_guard<std::mutex> lock(mutex_);
    consumers_.push_back(std::move(consumer));
    return reference;
}

void Broker::launch_health_monitor(const std::string& topic_name, std::chrono::milliseconds interval) {
    auto target = topic_locked(topic_name);

    HealthMonitor::SnapshotProvider provider = [this, target] {
        const MetricsSnapshot metrics_snapshot = metrics_.snapshot();
        return HealthSnapshot{target ? target->size() : 0,
                              metrics_snapshot.produced,
                              metrics_snapshot.consumed,
                              metrics_snapshot.active_consumers};
    };

    monitor_ = std::make_unique<HealthMonitor>(logger_, std::move(provider), interval);
    monitor_->start();
}

void Broker::close_topic(const std::string& name) {
    auto target = topic(name);
    if (target) {
        target->close();
    }
}

void Broker::shutdown() {
    wait_for_producers();
    close_all_topics();
    wait_for_consumers();
    stop_health_monitor();
}

void Broker::wait_for_producers() {
    std::vector<std::unique_ptr<Producer>> producers;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        producers.swap(producers_);
    }

    for (auto& producer : producers) {
        if (producer) {
            producer->join();
        }
    }
}

void Broker::wait_for_consumers() {
    std::vector<std::unique_ptr<Consumer>> consumers;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        consumers.swap(consumers_);
    }

    for (auto& consumer : consumers) {
        if (consumer) {
            consumer->join();
        }
    }
}

void Broker::stop_health_monitor() {
    if (monitor_) {
        monitor_->stop();
        monitor_.reset();
    }
}

MetricsSnapshot Broker::metrics() const {
    return metrics_.snapshot();
}

std::size_t Broker::topic_size(const std::string& name) const {
    auto target = topic(name);
    return target ? target->size() : 0;
}

Logger& Broker::logger() {
    return logger_;
}

Metrics& Broker::metrics_store() {
    return metrics_;
}

TopicPtr Broker::topic_locked(const std::string& name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto iterator = topics_.find(name);
    if (iterator == topics_.end()) {
        throw std::out_of_range("topic not found: " + name);
    }

    return iterator->second;
}

void Broker::close_all_topics() {
    std::vector<TopicPtr> topics;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        topics.reserve(topics_.size());
        for (const auto& entry : topics_) {
            topics.push_back(entry.second);
        }
    }

    for (const auto& topic : topics) {
        if (topic) {
            topic->close();
        }
    }
}

}  // namespace mini_kafka