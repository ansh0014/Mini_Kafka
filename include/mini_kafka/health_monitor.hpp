#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <thread>

namespace mini_kafka {

class Logger;

struct HealthSnapshot {
    std::size_t queue_size{0};
    std::uint64_t produced{0};
    std::uint64_t consumed{0};
    std::size_t active_consumers{0};
};

class HealthMonitor {
public:
    using SnapshotProvider = std::function<HealthSnapshot()>;

    HealthMonitor(Logger& logger, SnapshotProvider snapshot_provider, std::chrono::milliseconds interval);
    ~HealthMonitor();

    HealthMonitor(const HealthMonitor&) = delete;
    HealthMonitor& operator=(const HealthMonitor&) = delete;

    void start();
    void stop();
    void join();

private:
    void run();

    Logger* logger_{nullptr};
    SnapshotProvider snapshot_provider_;
    std::chrono::milliseconds interval_{500};
    std::thread worker_;
    std::atomic<bool> running_{false};
};

}  // namespace mini_kafka