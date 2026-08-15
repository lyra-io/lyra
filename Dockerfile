# Lyra Dockerfile
#   docker build -t lyra-unit:dev .

FROM rust:latest

RUN apt-get update && apt-get install -y protobuf-compiler clang libclang-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY lyrad/ lyrad/
COPY lyra/ lyra/

RUN cargo build --release -p lyra-cli

RUN cp target/release/lyra /usr/local/bin/lyra && \
    rm -rf /build/target

COPY options/ /etc/lyra/options/

RUN mkdir -p /data/wal /data/storage /data/segments /data/lexicon

EXPOSE 7070 7071 8080

ENTRYPOINT ["lyra"]
CMD ["unit", "start", "--config", "/etc/lyra/options/lyra.toml"]
