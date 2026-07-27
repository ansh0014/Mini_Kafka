#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <utility>

namespace mini_kafka {

template <typename T>
class ConcurrentQueue {
public:
explicit ConcurrentQueue(std::size_t capacity=std::numeric_limits<std::size_t>)
}
}  // namespace mini_kafka