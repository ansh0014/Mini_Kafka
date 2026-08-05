FROM gcc:13-bookworm AS build

WORKDIR /app

COPY CMakeLists.txt ./
COPY include ./include
COPY src ./src
COPY tests ./tests
COPY benchmark ./benchmark

RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && \
    cmake --build build --config Release

FROM debian:bookworm-slim AS runtime

RUN useradd --create-home --shell /usr/sbin/nologin appuser

WORKDIR /home/appuser

COPY --from=build /app/build/mini_kafka_app /usr/local/bin/mini_kafka
COPY --from=build /app/build/mini_kafka_benchmark /usr/local/bin/mini_kafka_benchmark

USER appuser

ENTRYPOINT ["/usr/local/bin/mini_kafka"]