#pragma once

#include "mini_kafka/consumer.hpp"
#include "mini_kafka/health_monitor.hpp"
#include "mini_kafka/logger.hpp"
#include "mini_kafka/metrics.hpp"
#include "mini_kafka/producer.hpp"
#include "mini_kafka/topic.hpp"
#include "mini_kafka/thread_pool.hpp"

#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>

namespace mini_kafka {

class Broker {
public:
    explicit Broker(std::ostream& log_stream = std::clog, std::size_t thread_pool_size = 4);
    ~Broker();

    Broker(const Broker&) = delete;
    Broker& operator=(const Broker&) = delete;

    TopicPtr create_topic(const std::string& name);
    TopicPtr topic(const std::string& name) const;

    Producer& launch_producer(std::string id,
                               const std::string& topic_name,
                               std::size_t message_count,
                               std::chrono::milliseconds interval);

    Consumer& launch_consumer(std::string id,
                              const std::string& topic_name,
                              Consumer::Handler handler = {},
                              bool use_thread_pool = true);

    void launch_health_monitor(const std::string& topic_name,
                               std::chrono::milliseconds interval = std::chrono::milliseconds{500});

    void close_topic(const std::string& name);
    void shutdown();
    void wait_for_producers();
    void wait_for_consumers();
    void stop_health_monitor();

    MetricsSnapshot metrics() const;
    std::size_t topic_size(const std::string& name) const;

    Logger& logger();
    Metrics& metrics_store();
    ThreadPool& thread_pool();

private:
    TopicPtr topic_locked(const std::string& name) const;
    void close_all_topics();

    mutable std::mutex mutex_;
    std::unordered_map<std::string, TopicPtr> topics_;
    std::vector<std::unique_ptr<Producer>> producers_;
    std::vector<std::unique_ptr<Consumer>> consumers_;
    std::unique_ptr<HealthMonitor> monitor_;
    Metrics metrics_;
    Logger logger_;
    ThreadPool thread_pool_;
};

}
