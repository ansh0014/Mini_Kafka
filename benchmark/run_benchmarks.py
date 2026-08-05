import os
import sys
import subprocess
import json


def find_executable():
    paths = [
        os.path.join("build", "Debug", "mini_kafka_benchmark.exe"),
        os.path.join("build", "Release", "mini_kafka_benchmark.exe"),
        os.path.join("build", "mini_kafka_benchmark.exe"),
        os.path.join("build", "Debug", "mini_kafka_benchmark"),
        os.path.join("build", "Release", "mini_kafka_benchmark"),
        os.path.join("build", "mini_kafka_benchmark"),
    ]
    for p in paths:
        if os.path.isfile(p):
            return p
    return None


def main():
    executable = find_executable()
    if not executable:
        print("Error: Could not find mini_kafka_benchmark executable.")
        print("Build the project first: cmake -B build && cmake --build build")
        sys.exit(1)

    print(f"Using benchmark executable: {executable}")

    scenarios = [
        (1,  1,  100000, 50000, 2,  "Single Producer / Single Consumer (1:1)"),
        (4,  4,  50000,  20000, 4,  "Moderate Concurrency (4:4)"),
        (8,  8,  25000,  10000, 8,  "High Concurrency (8:8)"),
        (16, 16, 10000,  5000,  16, "Extreme Contention (16:16)"),
        (1,  8,  100000, 50000, 8,  "Fan-Out Pattern (1 Producer / 8 Consumers)"),
        (8,  1,  25000,  20000, 4,  "Aggregation Pattern (8 Producers / 1 Consumer)"),
    ]

    print("\nStarting Benchmarks...")
    print("=" * 105)
    print(f"{'Scenario Description':<40} | {'Raw Queue (msg/s)':<20} | {'Raw Queue (MB/s)':<16} | {'Broker (msg/s)':<16} | {'Broker (MB/s)':<12}")
    print("=" * 105)

    results = []

    for p, c, msg, cap, pool, desc in scenarios:
        cmd = [executable, str(p), str(c), str(msg), str(cap), str(pool)]
        try:
            process = subprocess.run(cmd, capture_output=True, text=True, check=True)
            output_json = json.loads(process.stdout)

            raw_t = output_json["raw_queue"]["messages_per_sec"]
            raw_d = output_json["raw_queue"]["mb_per_sec"]
            broker_t = output_json["broker"]["messages_per_sec"]
            broker_d = output_json["broker"]["mb_per_sec"]

            print(f"{desc:<40} | {raw_t:>18.2f} | {raw_d:>14.2f} MB/s | {broker_t:>14.2f} | {broker_d:>10.2f} MB/s")
            results.append({"desc": desc, "raw_t": raw_t, "raw_d": raw_d, "broker_t": broker_t, "broker_d": broker_d})
        except Exception as e:
            print(f"{desc:<40} | FAILED: {e}")

    print("=" * 105)

    if results:
        best_raw = max(results, key=lambda x: x["raw_t"])
        best_broker = max(results, key=lambda x: x["broker_t"])

        print("\nBenchmark Summary:")
        print(f"  Peak Raw Queue:   {best_raw['raw_t']:.2f} msg/sec ({best_raw['raw_d']:.2f} MB/sec) — {best_raw['desc']}")
        print(f"  Peak Broker:      {best_broker['broker_t']:.2f} msg/sec ({best_broker['broker_d']:.2f} MB/sec) — {best_broker['desc']}")


if __name__ == "__main__":
    main()
