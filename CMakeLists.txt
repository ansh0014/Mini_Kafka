cmake_minimum_required(VERSION 3.16)

project(mini_kafka VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

add_library(mini_kafka
    src/broker.cpp
    src/consumer.cpp
    src/health_monitor.cpp
    src/logger.cpp
    src/main.cpp
    src/producer.cpp
    src/topic.cpp
)

target_include_directories(mini_kafka
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(mini_kafka
    PUBLIC
        Threads::Threads
)

add_executable(mini_kafka_app
    src/main.cpp
)

target_link_libraries(mini_kafka_app
    PRIVATE
        mini_kafka
)

enable_testing()

add_executable(test_concurrent_queue
    tests/test_concurrent_queue.cpp
)

target_link_libraries(test_concurrent_queue
    PRIVATE
        mini_kafka
)

add_test(NAME concurrent_queue COMMAND test_concurrent_queue)

add_executable(test_pipeline
    tests/test_pipeline.cpp
)

target_link_libraries(test_pipeline
    PRIVATE
        mini_kafka
)

add_test(NAME pipeline COMMAND test_pipeline)