#include "mini_kafka/broker.hpp"
#include "mini_kafka/concurrent_queue.hpp"

#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

struct NullBuffer : std::streambuf {
    int overflow(int c) override { return c; }
};

struct NullStream : std::ostream {
    NullStream() : std::ostream(&buf) {}
private:
    NullBuffer buf;
};

void benchmark_raw_queue(int num_producers, int num_consumers, int num_messages, std::size_t capacity) {
    mini_kafka::ConcurrentQueue<std::string> queue(capacity);
    std::vector<std::thread> producers;
    std::vector<std::thread> consumers;

    std::string payload(100, 'x');

    auto start_time = std::chrono::high_resolution_clock::now();

    std::atomic<std::size_t> consumed_count{0};
    for (int i = 0; i < num_consumers; ++i) {
        consumers.emplace_back([&queue, &consumed_count] {
            std::string val;
            while (queue.wait_and_pop(val)) {
                consumed_count.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    for (int i = 0; i < num_producers; ++i) {
        producers.emplace_back([&queue, num_messages, &payload] {
            for (int m = 0; m < num_messages; ++m) {
                queue.push(payload);
            }
        });
    }

    for (auto& t : producers) {
        t.join();
    }

    queue.close();

    for (auto& t : consumers) {
        t.join();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end_time - start_time;

    double throughput = (static_cast<double>(num_producers) * num_messages) / elapsed.count();
    double data_rate = (throughput * 100) / (1024.0 * 1024.0);

    std::cout << "  \"raw_queue\": {\n"
              << "    \"elapsed_seconds\": " << elapsed.count() << ",\n"
              << "    \"messages_per_sec\": " << throughput << ",\n"
              << "    \"mb_per_sec\": " << data_rate << "\n"
              << "  },\n";
}

void benchmark_broker(int num_producers, int num_consumers, int num_messages, int pool_size) {
    NullStream null_stream;

    auto start_time = std::chrono::high_resolution_clock::now();

    {
        mini_kafka::Broker broker(null_stream, pool_size);
        broker.create_topic("benchmark_topic");

        std::atomic<std::size_t> consumed_count{0};
        for (int i = 0; i < num_consumers; ++i) {
            broker.launch_consumer("consumer-" + std::to_string(i), "benchmark_topic", [&consumed_count](const mini_kafka::Message&) {
                consumed_count.fetch_add(1, std::memory_order_relaxed);
            }, true);
        }

        for (int i = 0; i < num_producers; ++i) {
            broker.launch_producer("producer-" + std::to_string(i), "benchmark_topic", num_messages, std::chrono::milliseconds(0));
        }

        broker.shutdown();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end_time - start_time;

    double throughput = (static_cast<double>(num_producers) * num_messages) / elapsed.count();
    double data_rate = (throughput * 45.0) / (1024.0 * 1024.0);

    std::cout << "  \"broker\": {\n"
              << "    \"elapsed_seconds\": " << elapsed.count() << ",\n"
              << "    \"messages_per_sec\": " << throughput << ",\n"
              << "    \"mb_per_sec\": " << data_rate << "\n"
              << "  }\n";
}

int main(int argc, char* argv[]) {
    int num_producers = 4;
    int num_consumers = 4;
    int messages_per_producer = 50000;
    int queue_capacity = 10000;
    int pool_size = 4;

    if (argc > 1) num_producers = std::stoi(argv[1]);
    if (argc > 2) num_consumers = std::stoi(argv[2]);
    if (argc > 3) messages_per_producer = std::stoi(argv[3]);
    if (argc > 4) queue_capacity = std::stoi(argv[4]);
    if (argc > 5) pool_size = std::stoi(argv[5]);

    std::cout << "{\n"
              << "  \"parameters\": {\n"
              << "    \"producers\": " << num_producers << ",\n"
              << "    \"consumers\": " << num_consumers << ",\n"
              << "    \"messages_per_producer\": " << messages_per_producer << ",\n"
              << "    \"queue_capacity\": " << queue_capacity << ",\n"
              << "    \"pool_size\": " << pool_size << "\n"
              << "  },\n";

    benchmark_raw_queue(num_producers, num_consumers, messages_per_producer, queue_capacity);
    benchmark_broker(num_producers, num_consumers, messages_per_producer, pool_size);

    std::cout << "}\n";

    return 0;
}
