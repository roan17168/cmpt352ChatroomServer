//! A multithreaded chatroom server for the CMPT 352 chatroom protocol.
//!
//! The server listens on TCP port 3500 and handles each client connection on its
//! own thread. Each connection also gets a writer thread draining a queue, so a
//! slow or stuck client cannot block the others.
//!
//! Protocol summary:
//!   * every line, from client or server, ends with CRLF (0x0D 0x0A)
//!   * JOIN <username>, EXIT, PUBLIC <message>, PRIVATE <username> <message>
//!   * a status line is three decimal digits and answers exactly one client line
//!   * JOINED / LEFT / PUBLIC / PRIVATE lines are pushes, not status lines

use std::collections::HashMap;
use std::io::{ErrorKind, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

/// The port the protocol specifies.
const PORT: u16 = 3500;
/// Most bytes buffered while waiting for a line terminator.
const MAX_LINE_BYTES: usize = 8192;
/// Username limit, counted in Unicode scalar values.
const MAX_USERNAME_CHARS: usize = 16;
/// Message limit, counted in Unicode scalar values.
const MAX_MESSAGE_CHARS: usize = 1000;

/// Queue of outbound lines for one connection.
type ClientSender = Sender<Vec<u8>>;

/// The users currently in the chatroom, keyed by username.
#[derive(Default)]
struct Room {
    users: HashMap<String, ClientSender>,
}

fn main() {
    let listener = match TcpListener::bind(("0.0.0.0", PORT)) {
        Ok(listener) => listener,
        Err(err) => {
            eprintln!("Could not listen on port {PORT}: {err}");
            std::process::exit(1);
        }
    };
    println!("Chatroom server listening on port {PORT}");

    let room = Arc::new(Mutex::new(Room::default()));

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let room = Arc::clone(&room);
                thread::spawn(move || {
                    if let Err(err) = handle_client(stream, room) {
                        eprintln!("Connection error: {err}");
                    }
                });
            }
            Err(err) => eprintln!("Failed to accept connection: {err}"),
        }
    }
}

/// Runs one client connection until it exits, disconnects, or overruns a line.
fn handle_client(mut stream: TcpStream, room: Arc<Mutex<Room>>) -> std::io::Result<()> {
    let peer = stream.peer_addr()?;
    println!("Connection from {}:{}", peer.ip(), peer.port());

    let mut writer_stream = stream.try_clone()?;
    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let writer = thread::spawn(move || {
        for line in rx {
            if writer_stream.write_all(&line).is_err() {
                break;
            }
        }
        // Everything queued has been written; close the connection.
        let _ = writer_stream.shutdown(Shutdown::Both);
    });

    // Username of this connection once it has joined, None while not in the room.
    let mut me: Option<String> = None;
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];

    'read: loop {
        let n = match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => n,
            Err(ref err) if err.kind() == ErrorKind::Interrupted => continue,
            Err(_) => break,
        };
        buf.extend_from_slice(&chunk[..n]);

        loop {
            // A line ends at CRLF; a bare LF is just line content.
            let Some(pos) = find_crlf(&buf) else {
                break;
            };

            let mut line: Vec<u8> = std::mem::take(&mut buf);
            let rest = line.split_off(pos + 2);
            line.truncate(pos);
            buf = rest;

            if process_line(&line, &mut me, &tx, &room) {
                break 'read;
            }
        }

        if buf.len() >= MAX_LINE_BYTES {
            println!("No line terminator within {MAX_LINE_BYTES} bytes; closing connection");
            break;
        }
    }

    if let Some(username) = me.take() {
        remove_user(&room, &username);
    }
    drop(tx);
    let _ = writer.join();
    println!("Disconnected from {}:{}", peer.ip(), peer.port());
    Ok(())
}

/// Handles one complete line. Returns true only when the connection must close,
/// which the protocol allows just for EXIT.
fn process_line(
    raw: &[u8],
    me: &mut Option<String>,
    tx: &ClientSender,
    room: &Arc<Mutex<Room>>,
) -> bool {
    let text = match std::str::from_utf8(raw) {
        Ok(text) => text,
        Err(_) => {
            println!("<- ({} bytes that are not valid UTF-8)", raw.len());
            send_status(tx, "500");
            return false;
        }
    };

    println!("<- {text}");

    let (command, argument) = match text.find(' ') {
        Some(index) => (&text[..index], Some(&text[index + 1..])),
        None => (text, None),
    };

    match command {
        "JOIN" => handle_join(argument.unwrap_or(""), me, tx, room),
        "EXIT" => {
            if argument.is_some() {
                send_status(tx, "500");
                return false;
            }
            // Removing the user announces LEFT; closing the socket afterwards
            // does not announce it a second time.
            if let Some(username) = me.take() {
                remove_user(room, &username);
            }
            send_status(tx, "200");
            true
        }
        "PUBLIC" => {
            let Some(sender) = me.as_ref() else {
                send_status(tx, "500");
                return false;
            };
            let message = argument.unwrap_or("");
            if message.contains('\r') || message.contains('\n') {
                send_status(tx, "500");
                return false;
            }
            if message.chars().count() > MAX_MESSAGE_CHARS {
                send_status(tx, "301");
                return false;
            }
            let line = format!("PUBLIC {sender} {message}\r\n").into_bytes();
            let room = lock_room(room);
            for recipient in room.users.values() {
                let _ = recipient.send(line.clone());
            }
            drop(room);
            send_status(tx, "300");
            false
        }
        "PRIVATE" => {
            let Some(sender) = me.as_ref() else {
                send_status(tx, "500");
                return false;
            };
            let (recipient, message) = match argument {
                Some(rest) => match rest.find(' ') {
                    Some(index) => (&rest[..index], &rest[index + 1..]),
                    None => (rest, ""),
                },
                None => ("", ""),
            };
            if recipient.is_empty()
                || recipient.chars().count() > MAX_USERNAME_CHARS
                || !is_legal_username(recipient)
            {
                send_status(tx, "403");
                return false;
            }
            if message.contains('\r') || message.contains('\n') {
                send_status(tx, "500");
                return false;
            }
            if message.chars().count() > MAX_MESSAGE_CHARS {
                send_status(tx, "402");
                return false;
            }
            let line = format!("PRIVATE {sender} {message}\r\n").into_bytes();
            let delivered = {
                let room = lock_room(room);
                match room.users.get(recipient) {
                    Some(recipient_tx) => {
                        let _ = recipient_tx.send(line);
                        true
                    }
                    None => false,
                }
            };
            send_status(tx, if delivered { "400" } else { "401" });
            false
        }
        // Unknown, lowercase, malformed, and client-sent JOINED/LEFT lines.
        _ => {
            send_status(tx, "500");
            false
        }
    }
}

/// Applies a JOIN. Returns false because a JOIN never closes the connection.
fn handle_join(
    username: &str,
    me: &mut Option<String>,
    tx: &ClientSender,
    room: &Arc<Mutex<Room>>,
) -> bool {
    if me.is_some() {
        send_status(tx, "500");
        return false;
    }
    if username.is_empty() {
        send_status(tx, "104");
        return false;
    }
    if username.chars().count() > MAX_USERNAME_CHARS {
        send_status(tx, "102");
        return false;
    }
    if !is_legal_username(username) {
        send_status(tx, "103");
        return false;
    }

    // Membership check and update happen under one lock: two connections racing
    // for the same free username cannot both succeed.
    {
        let mut room = lock_room(room);
        if room.users.contains_key(username) {
            drop(room);
            send_status(tx, "101");
            return false;
        }
        room.users.insert(username.to_string(), tx.clone());

        let joined = format!("JOINED {username}\r\n").into_bytes();
        for recipient in room.users.values() {
            let _ = recipient.send(joined.clone());
        }
        print_members(&room);
    }

    *me = Some(username.to_string());
    send_status(tx, "100");
    false
}

/// Removes a user who was in the room and announces LEFT to everyone remaining.
fn remove_user(room: &Arc<Mutex<Room>>, username: &str) {
    let mut room = lock_room(room);
    if room.users.remove(username).is_none() {
        return;
    }
    let left = format!("LEFT {username}\r\n").into_bytes();
    for recipient in room.users.values() {
        let _ = recipient.send(left.clone());
    }
    print_members(&room);
}

fn send_status(tx: &ClientSender, code: &str) {
    let _ = tx.send(format!("{code}\r\n").into_bytes());
}

fn print_members(room: &Room) {
    let mut names: Vec<&str> = room.users.keys().map(String::as_str).collect();
    names.sort_unstable();
    println!("Members ({}): {}", names.len(), names.join(", "));
}

fn is_legal_username(username: &str) -> bool {
    username.chars().all(|c| c.is_ascii_alphanumeric())
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|pair| pair == b"\r\n")
}

fn lock_room(room: &Arc<Mutex<Room>>) -> std::sync::MutexGuard<'_, Room> {
    room.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}
