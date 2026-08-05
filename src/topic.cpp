#include "mini_kafka/topic.hpp"

#include <stdexcept>
#include <utility>

namespace mini_kafka {

Topic::Topic(std::string name)
    : name_(std::move(name)) {
    if (name_.empty()) {
        throw std::invalid_argument("topic name cannot be empty");
    }
}

const std::string& Topic::name() const noexcept {
    return name_;
}

void Topic::publish(Message message) {
    message.topic = name_;
    queue_.push(std::move(message));
}

bool Topic::wait_consume(Message& message) {
    return queue_.wait_and_pop(message);
}

void Topic::close() {
    queue_.close();
}

std::size_t Topic::size() const {
    return queue_.size();
}

}