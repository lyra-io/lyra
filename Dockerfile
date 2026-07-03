# Chronicle Dockerfile
#   docker build -t chronicle-unit:dev .

FROM rust:latest

RUN apt-get update && apt-get install -y protobuf-compiler clang libclang-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY proto/ proto/
COPY chronicled/ chronicled/
COPY chronicles/ chronicles/
COPY clients/ clients/

RUN cargo build --release -p chronicle-cli

RUN cp target/release/chronicle /usr/local/bin/chronicle && \
    rm -rf /build/target

COPY chronicled.toml /etc/chronicle/chronicled.toml

RUN mkdir -p /data/wal /data/storage /data/segments /data/lexicon

EXPOSE 7070 7071 50060 8080

ENTRYPOINT ["chronicle"]
CMD ["unit", "start", "--config", "/etc/chronicle/chronicled.toml"]
