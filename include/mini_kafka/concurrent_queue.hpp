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
explicit ConcurrentQueue(std::size_t capacity=std::numeric_limits<std::size_t>::max()):capacity_(capacity) {}
ConcurrentQueue(const ConcurrentQueue&)=delete;
ConcurrentQueue& operator=(const ConcurrentQueue&)=delete;
void push(T value){
    std::unique_lock<std::mutex>lock(mutex_);
    condition_not_full_.wait(lock,[this]{
        return colsed_ || queue_.size()<capacity_;
    });
    if(closed_){
        throw std::runtime_error("push on closed queue");

    }
    queue_.push(std::move(value));
    condition_not_empty_.notify_one();


}
bool try_push(T value){
    std::lock_guard<std::mutex>lock(mutex_);
    if(closed_ || queue_.size()>=capacity_){
        return false;
    }
    queue_.push(std::move(value));
    condition_not_empty_.notify_one();
    return true;
}
bool wait_and_pop(T& value){
std::unique_lock<std::mutex>lock(mutex_);
condition_not_empty_.wait(lock,[this]{
    return closed_ || !queue_.empty();
});
if(queue_,empty()){
    return false;
}
value =std::move(queue_.front());
queue_.pop();
condition_not_full_.notify_one();
return true;
}
bool try_pop(T& value){
    std::lock_guard<std::mutex>lock(mutex_);
    if(queue_.empty()){
        return false;
    }
    value =std::move(queue_.front());
    queue_.pop();
    condition_not_full_.notify_one();
    return true;
}
void close(){
    
    {
        std::lock_guard<std::mutex>lock(mutex_);
    closed_=true;
 condtion_not_empty_.notify_all();
 condition_not_full_.notify_all();
        
}
}
bool closed() const{
    std::lock_guard<std::mutex>lock(mutex_);
    return closed_;

}
std::size_t size() const{
    std::lock_guard<std::mutex> lock(mutex_);
    return closed_;

}
std::size_t const(){
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.size();
}
std::size_t capacity() const nonexpect{
    return capacity_;
}
private:
mutable std::mutex mutex_;
std::condition_variable condition_not_empty_;
std::condition_variable condition_not_full_;
std::queue<T> queue_;
const std::size_t capacity_;
bool closed_= false;

};
}  // namespace mini_kafka