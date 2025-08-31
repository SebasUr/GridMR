FROM debian:bookworm as build
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y \
    build-essential cmake pkg-config git \
    libprotobuf-dev protobuf-compiler protobuf-compiler-grpc \
    libgrpc++-dev ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src
# Copiamos solo lo necesario para maximizar cache en rebuilds
COPY cpp/worker/ cpp/worker/
COPY src/main/proto/ src/main/proto/

RUN cmake -S cpp/worker -B build && cmake --build build -j

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y \
        ca-certificates \
        libprotobuf32 \
    libprotobuf-dev \
        libgrpc++1 \
    libgrpc++-dev \
        libabsl20220623 \
        libre2-9 \
    libc-ares2 \
    zlib1g \
    openssl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/build/worker /usr/local/bin/worker
ENV MASTER_HOST=master
ENV MASTER_PORT=50051
ENV LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu
ENTRYPOINT ["/usr/local/bin/worker"]
