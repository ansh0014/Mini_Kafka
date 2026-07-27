#include "mini_kafka/broker.hpp"

#include <chrono>
#include <iostream>
#include <string>

using namespace std::chrono_literals;

int main() {
    try {
        mini_kafka::Broker broker;
        broker.create_topic("orders");

        broker.launch_consumer("consumer-1", "orders", [](const mini_kafka::Message&) {});
        broker.launch_consumer("consumer-2", "orders", [](const mini_kafka::Message&) {});
        broker.launch_consumer("consumer-3", "orders", [](const mini_kafka::Message&) {});

        broker.launch_health_monitor("orders", 500ms);

        broker.launch_producer("producer-1", "orders", 40, 10ms);
        broker.launch_producer("producer-2", "orders", 40, 10ms);

        broker.shutdown();

        const mini_kafka::MetricsSnapshot snapshot = broker.metrics();
        std::cout << "Mini Kafka finished\n"
                  << "produced: " << snapshot.produced << '\n'
                  << "consumed: " << snapshot.consumed << '\n'
                  << "errors: " << snapshot.errors << '\n'
                  << "active consumers: " << snapshot.active_consumers << '\n';

        return 0;
    } catch (const std::exception& exception) {
        std::cerr << "fatal error: " << exception.what() << '\n';
        return 1;
    }
}