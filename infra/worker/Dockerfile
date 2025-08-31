# C++ worker image
FROM gcc:13 as builder

# Install dependencies: cmake, protobuf, gRPC, pkg-config, curl (for MinIO SDK via HTTP S3 compat if needed)
RUN apt-get update && apt-get install -y cmake git pkg-config build-essential autoconf libtool unzip \
    && rm -rf /var/lib/apt/lists/*

# Build and install protobuf and gRPC from source (simplest portable approach)
WORKDIR /deps
RUN git clone --depth 1 -b v1.63.0 https://github.com/grpc/grpc.git && \
    cd grpc && git submodule update --init --recursive && \
    cmake -B build -DgRPC_BUILD_TESTS=OFF -DgRPC_INSTALL=ON -DgRPC_ABSL_PROVIDER=module -DgRPC_CARES_PROVIDER=module -DgRPC_PROTOBUF_PROVIDER=module -DgRPC_RE2_PROVIDER=module -DgRPC_SSL_PROVIDER=module -DgRPC_ZLIB_PROVIDER=module && \
    cmake --build build -j && cmake --install build

# Build worker
WORKDIR /src
COPY . /src
RUN cmake -S cpp/worker -B build && cmake --build build -j

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local /usr/local
COPY --from=builder /src/build/worker /usr/local/bin/worker
ENV MASTER_HOST=master
ENV MASTER_PORT=50051
ENTRYPOINT ["/usr/local/bin/worker"]
