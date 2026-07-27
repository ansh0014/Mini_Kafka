#include "mini_kafka/health_monitor.hpp"

#include "mini_kafka/logger.hpp"

#include <chrono>
#include <stdexcept>
#include <thread>
#include <utility>

namespace mini_kafka {

HealthMonitor::HealthMonitor(Logger& logger,
                             SnapshotProvider snapshot_provider,
                             std::chrono::milliseconds interval)
    : logger_(&logger), snapshot_provider_(std::move(snapshot_provider)), interval_(interval) {
    if (!logger_) {
        throw std::invalid_argument("health monitor logger cannot be null");
    }

    if (!snapshot_provider_) {
        throw std::invalid_argument("health monitor snapshot provider cannot be empty");
    }
}

HealthMonitor::~HealthMonitor() {
    stop();
}

void HealthMonitor::start() {
    if (worker_.joinable()) {
        throw std::logic_error("health monitor already started");
    }

    running_.store(true, std::memory_order_relaxed);
    worker_ = std::thread([this] {
        run();
    });
}

void HealthMonitor::stop() {
    running_.store(false, std::memory_order_relaxed);

    if (worker_.joinable()) {
        worker_.join();
    }
}

void HealthMonitor::join() {
    if (worker_.joinable()) {
        worker_.join();
    }
}

void HealthMonitor::run() {
    while (running_.load(std::memory_order_relaxed)) {
        const HealthSnapshot snapshot = snapshot_provider_();

        logger_->info("health queue=" + std::to_string(snapshot.queue_size) +
                      " produced=" + std::to_string(snapshot.produced) +
                      " consumed=" + std::to_string(snapshot.consumed) +
                      " active_consumers=" + std::to_string(snapshot.active_consumers));

        std::this_thread::sleep_for(interval_);
    }
}

}  // namespace mini_kafka