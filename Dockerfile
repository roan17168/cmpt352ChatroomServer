# Build stage: compile the server with the Rust toolchain.
FROM rust:1-slim-bookworm AS builder

WORKDIR /app

# The project has no dependencies, so the manifest and sources are all it needs.
COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release --locked

# Runtime stage: only the compiled binary, no toolchain or source.
FROM debian:bookworm-slim

RUN useradd --system --shell /usr/sbin/nologin chatroom

COPY --from=builder /app/target/release/chatroom_server /usr/local/bin/chatroom_server

# The server binds 0.0.0.0:3500, so it accepts connections from outside the container.
EXPOSE 3500

USER chatroom

ENTRYPOINT ["/usr/local/bin/chatroom_server"]
