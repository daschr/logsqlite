# syntax=docker/dockerfile:1.7-labs

FROM rust:bookworm as prefetch
WORKDIR /src/logsqlite
RUN apt update && apt install -y build-essential libsqlite3-0 libsqlite3-dev protobuf-compiler && apt clean
COPY Cargo.toml /src/logsqlite/
RUN mkdir /src/logsqlite/src
RUN echo 'fn main() {}' >/src/logsqlite/src/main.rs
RUN cargo b --release 

FROM prefetch as build
WORKDIR /src/logsqlite
COPY Cargo.toml build.rs /src/logsqlite/
COPY src /src/logsqlite/src
RUN cargo b --release && cp target/*/logsqlite .

FROM debian:bookworm as logsqlite_rootfs
RUN apt update && apt install -y libsqlite3-0 && apt clean
RUN mkdir -p /var/spool/logsqlite /etc/logsqlite /run/docker/plugins
COPY --from=build /src/logsqlite/logsqlite /bin/logsqlite
COPY conf.ini /etc/logsqlite/
VOLUME /var/spool/logsqlite
WORKDIR /bin/
ENTRYPOINT [ "/bin/logsqlite", "/etc/logsqlite/conf.ini" ]
