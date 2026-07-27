#include "mini_kafka/concurrent_queue.hpp"
#include "test_framework.hpp"

int main() {
    mini_kafka_test::TestContext context;

    mini_kafka::ConcurrentQueue<int> queue;

    queue.push(7);
    queue.push(11);

    int value = 0;
    mini_kafka_test::expect_true(context, queue.try_pop(value), "try_pop should succeed");
    mini_kafka_test::expect_equal(context, value, 7, "first value should match");

    mini_kafka_test::expect_true(context, queue.wait_and_pop(value), "wait_and_pop should succeed");
    mini_kafka_test::expect_equal(context, value, 11, "second value should match");

    queue.close();
    mini_kafka_test::expect_true(context, !queue.wait_and_pop(value), "closed empty queue should end");

    return mini_kafka_test::finish(context);
}