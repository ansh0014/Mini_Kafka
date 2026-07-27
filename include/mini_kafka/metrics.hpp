#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace mini_kafka {

struct MetricsSnapshot {
    std::uint64_t produced{0};
    std::uint64_t consumed{0};
    std::uint64_t errors{0};
    std::size_t active_consumers{0};
};

class Metrics {
public:
    void record_produced() {
        produced_.fetch_add(1, std::memory_order_relaxed);
    }

    void record_consumed() {
        consumed_.fetch_add(1, std::memory_order_relaxed);
    }

    void record_error() {
        errors_.fetch_add(1, std::memory_order_relaxed);
    }

    void consumer_started() {
        active_consumers_.fetch_add(1, std::memory_order_relaxed);
    }

    void consumer_stopped() {
        active_consumers_.fetch_sub(1, std::memory_order_relaxed);
    }

    MetricsSnapshot snapshot() const {
        return MetricsSnapshot{
            produced_.load(std::memory_order_relaxed),
            consumed_.load(std::memory_order_relaxed),
            errors_.load(std::memory_order_relaxed),
            active_consumers_.load(std::memory_order_relaxed)};
    }

private:
    std::atomic<std::uint64_t> produced_{0};
    std::atomic<std::uint64_t> consumed_{0};
    std::atomic<std::uint64_t> errors_{0};
    std::atomic<std::size_t> active_consumers_{0};
};

}  // namespace mini_kafka