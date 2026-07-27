#include "mini_kafka/consumer.hpp"
#include "mini_kafka/logger.hpp"
#include "mini_kafka/metrics.hpp"
#include "mini_kafka/producer.hpp"
#include "mini_kafka/topic.hpp"
#include "test_framework.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <sstream>

using namespace std::chrono_literals;

int main() {
    mini_kafka_test::TestContext context;

    std::ostringstream sink;
    mini_kafka::Logger logger(sink);
    mini_kafka::Metrics metrics;
    auto topic = std::make_shared<mini_kafka::Topic>("orders");

    std::atomic<std::size_t> processed{0};

    mini_kafka::Consumer consumer(
        "consumer-1",
        topic,
        metrics,
        logger,
        [&processed](const mini_kafka::Message&) {
            processed.fetch_add(1, std::memory_order_relaxed);
        });

    mini_kafka::Producer producer("producer-1", topic, metrics, logger);

    consumer.start();
    producer.start(25, 0ms);

    producer.join();
    topic->close();
    consumer.join();
    logger.stop();

    const mini_kafka::MetricsSnapshot snapshot = metrics.snapshot();
    mini_kafka_test::expect_equal(context, snapshot.produced, static_cast<std::uint64_t>(25), "produced count");
    mini_kafka_test::expect_equal(context, snapshot.consumed, static_cast<std::uint64_t>(25), "consumed count");
    mini_kafka_test::expect_equal(context, processed.load(std::memory_order_relaxed), static_cast<std::size_t>(25), "handler count");
    mini_kafka_test::expect_equal(context, snapshot.active_consumers, static_cast<std::size_t>(0), "consumer shutdown");

    return mini_kafka_test::finish(context);
}