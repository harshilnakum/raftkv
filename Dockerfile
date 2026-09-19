# docker build -t raftkv .
# docker run --rm raftkv raftkv_sim --seeds 1..500      # deterministic simulation
# docker compose up                                      # 3-node cluster on localhost:7001-7003
FROM ubuntu:24.04 AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ cmake make pkg-config ca-certificates \
      libgrpc++-dev libprotobuf-dev protobuf-compiler protobuf-compiler-grpc && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cmake -S . -B /out -DCMAKE_BUILD_TYPE=Release -DRAFTKV_WERROR=ON && cmake --build /out -j"$(nproc)" && /out/raftkv_tests | tail -2

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends libgrpc++1.51t64 libprotobuf32t64 && rm -rf /var/lib/apt/lists/* || \
    (apt-get update && apt-get install -y --no-install-recommends libgrpc++-dev && rm -rf /var/lib/apt/lists/*)
COPY --from=build /out/raftkv_server /out/raftkv_ctl /out/raftkv_bench /out/raftkv_sim /out/raftkv_tests /usr/local/bin/
WORKDIR /data
ENTRYPOINT []
CMD ["raftkv_tests"]
