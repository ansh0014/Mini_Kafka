#include "mini_kafka/thread_pool.hpp"
namespace mini_kafka{
    ThreadPool::ThreadPool(std::size_t threads){
        worker_.reserve(threads);
        for(std::size_t i=0;i<threads;i++){
            worker_.emplace_back([this](){
                this->worker_thread();
            });
        }
    }
    ThreadPool::~ThreadPool(){
        shutdown();
    }
    void ThreadPool::shutdown(){
        work_queue_.close();
        for(auto &thread:workder_){
            if(worker.joinable()){
                worker.join();
            }
        }
        workers_.clear();
    }
    void ThreadPool::worker_thread(){
        std::function<void()>task;
        while(work_queue_.wait_and_pop(task)){
            try{
                if(task){
                    task();
                }
                
            }catch(...){

            }
        }
    }
} // namespace mini_kafka
