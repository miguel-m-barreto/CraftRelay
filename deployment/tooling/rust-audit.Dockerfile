FROM rust:1.88.0-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        cmake \
        g++ \
        make \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*
