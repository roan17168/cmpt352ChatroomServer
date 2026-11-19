# CMPT 352 Chatroom Server

The server half of the CMPT 352 chatroom assignment, written in Rust with no
external dependencies. The desktop client lives in
[cmpt352ChatroomClient](https://github.com/roan17168/cmpt352ChatroomClient).

It accepts TCP connections on port **3500**, speaks the assignment protocol
(UTF-8 lines terminated by CRLF), and handles `JOIN`, `PUBLIC`, `PRIVATE`, and
`EXIT`. Broadcasts go only to members currently in the room; a private message
is echoed only to its sender.

## Layout

| Path | Purpose |
| --- | --- |
| `src/main.rs` | Listener, per-connection sessions, protocol handling. |
| `Cargo.toml` / `Cargo.lock` | Package metadata and the locked build (no dependencies). |
| `Dockerfile` | Multi-stage build: Rust builder, Debian runtime, non-root user, exposes 3500. |
| `compose.yaml` | Builds the image and publishes `3500:3500`. |
| `tests/test_server_protocol.py` | Raw-socket conformance suite against a live server. |

## Run it

Docker (builds `chatroom-server:latest` and starts container `chatroom`):

```bash
docker compose up -d
docker compose logs -f chatroom
docker compose down
```

Native build:

```bash
cargo run --release
```

Only one of the two can own port 3500 at a time. The server stays in the
foreground and prints connection activity.

## Test it

The suite talks to a live server, so start one first:

```bash
python3 -m unittest discover -s tests -v
```

It uses only the Python standard library and defaults to `127.0.0.1:3500`;
override with `CHATROOM_HOST` and `CHATROOM_PORT`.

## Protocol

One command per line, CRLF-terminated:

```
JOIN alice
PUBLIC hello everyone
PRIVATE bob hello bob
EXIT
```

Every command gets a status line. Public messages and joins/leaves are pushed
to the relevant members as `PUBLIC <sender> <message>`, `JOINED <user>`, or
`LEFT <user>`.

| Code | Meaning |
| --- | --- |
| `100` | joined |
| `101` | username already taken |
| `102` | username too long |
| `103` | username has an illegal character |
| `104` | `JOIN` with no username |
| `200` | left the room, connection closed |
| `300` | public message delivered |
| `301` | public message too long (max 1000) |
| `400` | private message delivered |
| `401` | private recipient not in the room |
| `402` | private message too long (max 1000) |
| `403` | private recipient missing or illegal |
| `500` | unknown or malformed command |
