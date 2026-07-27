#pragma once

#include <chrono>
#include <cstdint>
#include <string>

namespace mini_kafka {

struct Message {
    std::uint64_t sequence{0};
    std::string producer_id;
    std::string topic;
    std::string payload;
    std::chrono::system_clock::time_point created_at{std::chrono::system_clock::now()};
};

}  // namespace mini_kafka