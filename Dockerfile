FROM rust:1.88 AS build

# Install dependencies for the node and LLVM
RUN wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - && \
    echo "deb http://apt.llvm.org/bookworm/ llvm-toolchain-bookworm-20 main" > /etc/apt/sources.list.d/llvm.list && \
    echo "deb-src http://apt.llvm.org/bookworm/ llvm-toolchain-bookworm-20 main" >> /etc/apt/sources.list.d/llvm.list && \
    apt-get update && apt-get install -y \
        libssl-dev \
        zlib1g-dev \
        pkg-config \
        clang-20

# Cache build dependencies first
RUN USER=root cargo new --bin tycho-toncenter
WORKDIR /tycho-toncenter

COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml

RUN cargo build --release
RUN rm src/*.rs

# Build the node itself
COPY ./src ./src
COPY ./build.rs ./build.rs

RUN rm ./target/release/deps/tycho_toncenter*
RUN cargo build --release

# Prepare the final image
FROM debian:12-slim AS runner
RUN mkdir -p /tycho && adduser tycho && chown -hR tycho /tycho
COPY --from=build /tycho-toncenter/target/release/tycho-toncenter /usr/local/bin/tycho-toncenter
USER tycho
WORKDIR /tycho
ENTRYPOINT ["tycho-toncenter"]
