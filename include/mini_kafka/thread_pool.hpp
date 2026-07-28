#pragma once

#include <vector>
#include <thread>
#include <future>
#include <functional>
#include <memory>
#include <type_traits>

#include "mini_kafka/concurrent_queue.hpp"
namespace mini_kafka
{
    class ThreadPool
    {
    public:
        explicit ThreadPool(std::size_t threads = std::thread::hardware_concurrency());
        ~ThreadPool();
        ThreadPool(const ThreadPool &) = delete;
        ThreadPool &operator=(const ThreadPool &) = delete;
        template <typename F, typename... Args>
        auto submit(F &&f, Args &&...args)
            -> std::future<typename std::invoke_result<F, Args...>::type>
        {
            using ReturnType = typename std::invoke_result<F, Args...>::type;
            auto task = std::make_shared<std::packaged_task<ReturnType()>>(
                std::bind(std::forward<F>(f), std::forward<Args>(args)...));
            std::future<ReturnType> result = task->get_future();
            work_queue_.push([task]()
                             { (*task)(); });
            return result;
        }
        void shutdown();

    private:
        void worker_thread();
        std::vector<std::thread> worker_;
        ConcurrentQueue<std::functional<void()>> work_queue_;
    };
} // namespace mini_kafka