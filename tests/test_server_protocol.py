#!/usr/bin/env python3
"""Conformance tests for the chatroom server against the CMPT 352 protocol.

Runs against a live server, normally the Docker container:

    docker compose up -d
    python3 -m unittest discover -s tests -v

Override the endpoint with CHATROOM_HOST / CHATROOM_PORT.

The server may push a PUBLIC/PRIVATE/JOINED/LEFT line before or after the
status line for a command, so this suite never assumes an ordering between the
two kinds of line: pushed lines read while a status was awaited are kept and
matched later by the push assertions.
"""

import itertools
import os
import socket
import threading
import time
import unittest

HOST = os.environ.get("CHATROOM_HOST", "127.0.0.1")
PORT = int(os.environ.get("CHATROOM_PORT", "3500"))
CRLF = b"\r\n"

_sequence = itertools.count(1)


def unique(prefix="u"):
    """A fresh legal username, unique within this process."""
    return f"{prefix}{os.getpid() % 1000:03d}{next(_sequence):03d}"


def is_status(line):
    return len(line) == 3 and line.isdigit()


class Conn:
    """One raw TCP connection with CRLF framing and push stashing."""

    def __init__(self, timeout=3.0):
        self.sock = socket.create_connection((HOST, PORT), timeout=timeout)
        self.buf = b""
        self.stash = []

    # -- sending ---------------------------------------------------------

    def send(self, text):
        self.sock.sendall(text.encode("utf-8") + CRLF)

    def send_bytes(self, data):
        self.sock.sendall(data)

    # -- receiving -------------------------------------------------------

    def read_line(self, timeout=3.0):
        self.sock.settimeout(timeout)
        while CRLF not in self.buf:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise EOFError("connection closed while waiting for a line")
            self.buf += chunk
        raw, self.buf = self.buf.split(CRLF, 1)
        return raw.decode("utf-8")

    def read_status(self, timeout=3.0):
        """Reads until a status line arrives, keeping the pushes it skips."""
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssertionError(f"no status line arrived; stashed {self.stash}")
            line = self.read_line(remaining)
            if is_status(line):
                return line
            self.stash.append(line)

    def read_push(self, prefix, timeout=3.0):
        """Returns the next pushed line starting with prefix, checked first."""
        for index, line in enumerate(self.stash):
            if line.startswith(prefix):
                return self.stash.pop(index)

        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssertionError(f"no {prefix!r} line arrived; stashed {self.stash}")
            line = self.read_line(remaining)
            if line.startswith(prefix):
                return line
            self.stash.append(line)

    def expect_silence(self, seconds=0.3):
        """Asserts that nothing at all arrives in the next window."""
        if self.buf:
            raise AssertionError(f"buffered data arrived: {self.buf!r}")
        self.sock.settimeout(seconds)
        try:
            data = self.sock.recv(4096)
        except socket.timeout:
            return
        raise AssertionError(f"expected no data, received {data!r}")

    def expect_eof(self, timeout=3.0):
        if self.buf:
            raise AssertionError(f"unexpected data before close: {self.buf!r}")
        self.sock.settimeout(timeout)
        try:
            data = self.sock.recv(4096)
        except socket.timeout:
            raise AssertionError("connection stayed open, expected the server to close it")
        if data:
            raise AssertionError(f"unexpected data before close: {data!r}")

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


class BaseTest(unittest.TestCase):
    """Helpers shared by every protocol test."""

    def socket(self):
        conn = Conn()
        self.addCleanup(conn.close)
        return conn

    def join(self, name=None, conn=None):
        """Joins the room and returns (connection, username)."""
        name = name or unique()
        conn = conn or self.socket()
        conn.send(f"JOIN {name}")
        self.expect_status(conn, "100")
        return conn, name

    def expect_status(self, conn, expected, timeout=3.0):
        code = conn.read_status(timeout)
        if code != expected:
            raise AssertionError(f"expected status {expected}, got {code} (stashed {conn.stash})")
        return code

    def expect_push(self, conn, prefix, timeout=3.0):
        return conn.read_push(prefix, timeout)


class JoinTests(BaseTest):
    def test_join_without_username_is_104(self):
        conn = self.socket()
        conn.send("JOIN")
        self.expect_status(conn, "104")

    def test_join_with_trailing_space_is_104(self):
        conn = self.socket()
        conn.send("JOIN ")
        self.expect_status(conn, "104")

    def test_username_of_17_characters_is_102(self):
        conn = self.socket()
        conn.send("JOIN " + "a" * 17)
        self.expect_status(conn, "102")

    def test_username_of_16_characters_is_accepted(self):
        conn = self.socket()
        conn.send("JOIN " + "z" * 16)
        try:
            self.expect_status(conn, "100")
        finally:
            conn.close()

    def test_too_long_and_illegal_is_102_first(self):
        conn = self.socket()
        conn.send("JOIN " + "a" * 16 + "!")
        self.expect_status(conn, "102")

    def test_illegal_character_is_103(self):
        conn = self.socket()
        conn.send("JOIN bad!")
        self.expect_status(conn, "103")

    def test_space_inside_username_is_103(self):
        conn = self.socket()
        conn.send("JOIN bad name")
        self.expect_status(conn, "103")

    def test_duplicate_username_is_101(self):
        first, name = self.join()
        second = self.socket()
        second.send(f"JOIN {name}")
        self.expect_status(second, "101")

    def test_usernames_are_case_sensitive(self):
        lower = unique("ada")
        self.join(lower)
        self.join(lower.upper())

    def test_join_is_announced_to_everyone_including_the_joiner(self):
        alice, alice_name = self.join()
        # Alice's own JOINED line was stashed while her 100 was awaited.
        self.assertEqual(alice.read_push("JOINED "), f"JOINED {alice_name}")

        bob_name = unique("b")
        bob = self.socket()
        bob.send(f"JOIN {bob_name}")

        self.expect_status(bob, "100")
        self.assertEqual(bob.read_push("JOINED "), f"JOINED {bob_name}")
        self.assertEqual(alice.read_push("JOINED "), f"JOINED {bob_name}")

    def test_join_does_not_replay_existing_members(self):
        self.join()
        newcomer_name = unique("n")
        newcomer = self.socket()
        newcomer.send(f"JOIN {newcomer_name}")

        self.expect_status(newcomer, "100")
        self.assertEqual(newcomer.read_push("JOINED "), f"JOINED {newcomer_name}")
        newcomer.expect_silence()

    def test_failed_join_announces_nothing(self):
        alice, name = self.join()
        duplicate = self.socket()
        duplicate.send(f"JOIN {name}")
        self.expect_status(duplicate, "101")
        alice.expect_silence()

    def test_join_is_not_announced_to_connections_outside_the_room(self):
        spectator = self.socket()
        self.join()
        spectator.expect_silence()

    def test_second_join_is_500_and_keeps_the_username(self):
        conn, name = self.join()
        conn.send(f"JOIN {unique()}")
        self.expect_status(conn, "500")

        conn.send("PUBLIC still here")
        self.expect_status(conn, "300")
        self.assertEqual(self.expect_push(conn, "PUBLIC "), f"PUBLIC {name} still here")

    def test_concurrent_join_of_one_username_gives_100_and_101(self):
        name = unique("race")
        codes = []
        barrier = threading.Barrier(2)
        lock = threading.Lock()

        def attempt():
            conn = Conn()
            try:
                barrier.wait(timeout=5)
                conn.send(f"JOIN {name}")
                code = conn.read_status(5)
                with lock:
                    codes.append(code)
            finally:
                conn.close()

        threads = [threading.Thread(target=attempt) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        self.assertEqual(sorted(codes), ["100", "101"], codes)


class ExitTests(BaseTest):
    def test_exit_replies_200_and_closes(self):
        conn, _ = self.join()
        conn.send("EXIT")
        self.expect_status(conn, "200")
        conn.expect_eof()

    def test_left_is_announced_to_the_remaining_users_only(self):
        alice, _ = self.join()
        bob, bob_name = self.join()

        bob.send("EXIT")
        self.expect_status(bob, "200")
        bob.expect_eof()

        self.assertEqual(self.expect_push(alice, "LEFT "), f"LEFT {bob_name}")

    def test_exit_without_join_is_200_and_announces_nothing(self):
        member, _ = self.join()
        outsider = self.socket()
        outsider.send("EXIT")
        self.expect_status(outsider, "200")
        outsider.expect_eof()
        member.expect_silence()

    def test_malformed_exit_is_500_and_keeps_the_user_in_the_room(self):
        conn, name = self.join()

        conn.send("EXIT ")
        self.expect_status(conn, "500")
        conn.send("EXIT now")
        self.expect_status(conn, "500")

        conn.send("PUBLIC still in")
        self.expect_status(conn, "300")
        self.assertEqual(self.expect_push(conn, "PUBLIC "), f"PUBLIC {name} still in")

    def test_username_is_reusable_after_exit(self):
        conn, name = self.join()
        conn.send("EXIT")
        self.expect_status(conn, "200")
        conn.expect_eof()

        again = self.socket()
        again.send(f"JOIN {name}")
        self.expect_status(again, "100")

    def test_closing_the_connection_announces_left(self):
        alice, _ = self.join()
        bob, bob_name = self.join()

        bob.close()
        self.assertEqual(self.expect_push(alice, "LEFT ", timeout=5), f"LEFT {bob_name}")


class PublicTests(BaseTest):
    def test_public_is_echoed_to_sender_and_others(self):
        alice, alice_name = self.join()
        bob, _ = self.join()

        alice.send("PUBLIC hello world")
        self.expect_status(alice, "300")

        expected = f"PUBLIC {alice_name} hello world"
        self.assertEqual(self.expect_push(alice, "PUBLIC "), expected)
        self.assertEqual(self.expect_push(bob, "PUBLIC "), expected)

    def test_empty_public_message_keeps_the_trailing_space(self):
        conn, name = self.join()
        conn.send("PUBLIC")
        self.expect_status(conn, "300")
        self.assertEqual(self.expect_push(conn, "PUBLIC "), f"PUBLIC {name} ")

    def test_public_with_only_a_space_is_an_empty_message(self):
        conn, name = self.join()
        conn.send("PUBLIC ")
        self.expect_status(conn, "300")
        self.assertEqual(self.expect_push(conn, "PUBLIC "), f"PUBLIC {name} ")

    def test_spaces_inside_and_around_the_message_are_preserved(self):
        conn, name = self.join()
        conn.send("PUBLIC   two  spaces ")
        self.expect_status(conn, "300")
        self.assertEqual(self.expect_push(conn, "PUBLIC "), f"PUBLIC {name}   two  spaces ")

    def test_message_of_1000_characters_is_accepted(self):
        conn, name = self.join()
        message = "x" * 1000
        conn.send(f"PUBLIC {message}")
        self.expect_status(conn, "300")
        self.assertEqual(self.expect_push(conn, "PUBLIC "), f"PUBLIC {name} {message}")

    def test_message_of_1001_characters_is_301_and_not_delivered(self):
        alice, _ = self.join()
        bob, _ = self.join()

        alice.send("PUBLIC " + "x" * 1001)
        self.expect_status(alice, "301")
        bob.expect_silence()

    def test_carriage_return_in_message_is_500(self):
        alice, _ = self.join()
        bob, _ = self.join()

        alice.send_bytes(b"PUBLIC bad\rthing" + CRLF)
        self.expect_status(alice, "500")
        bob.expect_silence()

    def test_line_feed_in_message_is_500(self):
        alice, _ = self.join()
        bob, _ = self.join()

        alice.send_bytes(b"PUBLIC bad\nthing" + CRLF)
        self.expect_status(alice, "500")
        bob.expect_silence()

    def test_public_before_join_is_500_and_leaves_the_connection_open(self):
        conn = self.socket()
        conn.send("PUBLIC early")
        self.expect_status(conn, "500")

        conn.send(f"JOIN {unique()}")
        self.expect_status(conn, "100")

    def test_public_is_not_pushed_to_connections_outside_the_room(self):
        spectator = self.socket()
        member, _ = self.join()

        member.send("PUBLIC hello")
        self.expect_status(member, "300")
        spectator.expect_silence()

    def test_pushed_messages_keep_the_order_the_server_wrote_them(self):
        alice, alice_name = self.join()
        bob, _ = self.join()

        for text in ("one", "two", "three"):
            alice.send(f"PUBLIC {text}")
            self.expect_status(alice, "300")

        received = [self.expect_push(bob, "PUBLIC ") for _ in range(3)]
        self.assertEqual(
            received,
            [
                f"PUBLIC {alice_name} one",
                f"PUBLIC {alice_name} two",
                f"PUBLIC {alice_name} three",
            ],
        )


class PrivateTests(BaseTest):
    def test_private_reaches_only_the_recipient(self):
        alice, alice_name = self.join()
        bob, bob_name = self.join()
        carol, _ = self.join()

        alice.send(f"PRIVATE {bob_name} hi bob")
        self.expect_status(alice, "400")
        self.assertEqual(self.expect_push(bob, "PRIVATE "), f"PRIVATE {alice_name} hi bob")
        carol.expect_silence()

    def test_private_to_self_is_400_and_delivered(self):
        conn, name = self.join()
        conn.send(f"PRIVATE {name} note to self")
        self.expect_status(conn, "400")
        self.assertEqual(self.expect_push(conn, "PRIVATE "), f"PRIVATE {name} note to self")

    def test_private_to_unknown_user_is_401(self):
        conn, _ = self.join()
        conn.send(f"PRIVATE {unique('no')} hello")
        self.expect_status(conn, "401")

    def test_private_without_recipient_is_403(self):
        conn, _ = self.join()
        conn.send("PRIVATE")
        self.expect_status(conn, "403")

    def test_private_with_empty_recipient_is_403(self):
        conn, _ = self.join()
        conn.send("PRIVATE  hello")
        self.expect_status(conn, "403")

    def test_private_with_illegal_recipient_is_403(self):
        conn, _ = self.join()
        conn.send("PRIVATE bad! hello")
        self.expect_status(conn, "403")

    def test_private_with_too_long_recipient_is_403(self):
        conn, _ = self.join()
        conn.send("PRIVATE " + "a" * 17 + " hello")
        self.expect_status(conn, "403")

    def test_private_message_of_1001_characters_is_402(self):
        alice, _ = self.join()
        bob, bob_name = self.join()

        alice.send(f"PRIVATE {bob_name} " + "x" * 1001)
        self.expect_status(alice, "402")
        bob.expect_silence()

    def test_too_long_private_message_to_unknown_user_is_402_first(self):
        conn, _ = self.join()
        conn.send(f"PRIVATE {unique('gh')} " + "x" * 1001)
        self.expect_status(conn, "402")

    def test_illegal_recipient_and_long_message_is_403_first(self):
        conn, _ = self.join()
        conn.send("PRIVATE bad! " + "x" * 1001)
        self.expect_status(conn, "403")

    def test_carriage_return_and_long_message_is_500_first(self):
        conn, _ = self.join()
        conn.send_bytes(b"PRIVATE someone bad\r" + b"x" * 1001 + CRLF)
        self.expect_status(conn, "500")

    def test_private_with_empty_message_is_400(self):
        alice, alice_name = self.join()
        bob, bob_name = self.join()

        alice.send(f"PRIVATE {bob_name}")
        self.expect_status(alice, "400")
        self.assertEqual(self.expect_push(bob, "PRIVATE "), f"PRIVATE {alice_name} ")

    def test_private_before_join_is_500(self):
        conn = self.socket()
        conn.send("PRIVATE someone hello")
        self.expect_status(conn, "500")


class FramingTests(BaseTest):
    def test_unknown_command_is_500(self):
        conn, _ = self.join()
        conn.send("HELLO")
        self.expect_status(conn, "500")

    def test_command_word_must_match_exactly(self):
        conn, _ = self.join()
        conn.send("PUBLICX hello")
        self.expect_status(conn, "500")

    def test_lowercase_command_is_500(self):
        conn = self.socket()
        conn.send("join alice")
        self.expect_status(conn, "500")

    def test_client_cannot_send_joined_or_left(self):
        conn, _ = self.join()
        conn.send("JOINED alice")
        self.expect_status(conn, "500")
        conn.send("LEFT alice")
        self.expect_status(conn, "500")

    def test_invalid_utf8_is_500_and_leaves_state_unchanged(self):
        conn, name = self.join()
        conn.send_bytes(b"\xff\xfe bad bytes" + CRLF)
        self.expect_status(conn, "500")

        conn.send("PUBLIC after bad bytes")
        self.expect_status(conn, "300")
        self.assertEqual(self.expect_push(conn, "PUBLIC "), f"PUBLIC {name} after bad bytes")

    def test_status_lines_are_three_digits_and_pushes_are_not(self):
        conn, _ = self.join()
        conn.send("PUBLIC hello")
        code = conn.read_status()
        self.assertTrue(is_status(code), code)
        self.assertEqual(code, "300")
        self.assertFalse(any(is_status(line) for line in conn.stash), conn.stash)

    def test_pipelined_commands_answer_in_order(self):
        conn = self.socket()
        name = unique()
        conn.send_bytes(
            f"JOIN {name}".encode() + CRLF + b"PUBLIC one" + CRLF + b"PUBLIC two" + CRLF
        )
        codes = [conn.read_status(5) for _ in range(3)]
        self.assertEqual(codes, ["100", "300", "300"])

    def test_bare_line_feed_does_not_end_a_line(self):
        alice, _ = self.join()
        bob, _ = self.join()

        alice.send_bytes(b"PUBLIC split\nacross lines" + CRLF)
        self.expect_status(alice, "500")
        bob.expect_silence()

    def test_8192_bytes_without_a_newline_closes_and_announces_left(self):
        alice, _ = self.join()
        bob, bob_name = self.join()

        bob.send_bytes(b"x" * 8192)
        bob.expect_eof(timeout=5)
        self.assertEqual(self.expect_push(alice, "LEFT ", timeout=5), f"LEFT {bob_name}")

    def test_8191_bytes_without_a_newline_is_still_tolerated(self):
        conn, _ = self.join()
        conn.send_bytes(b"x" * 8191)
        conn.expect_silence(0.3)
        conn.send_bytes(CRLF)
        self.expect_status(conn, "500")


class StatusLineTests(BaseTest):
    def test_500_does_not_close_the_connection(self):
        conn = self.socket()
        conn.send("NOPE")
        self.expect_status(conn, "500")
        conn.send(f"JOIN {unique()}")
        self.expect_status(conn, "100")

    def test_every_command_gets_exactly_one_status(self):
        conn, _ = self.join()
        conn.send("PUBLIC one")
        self.expect_status(conn, "300")
        # No second status line should be waiting behind the first.
        conn.expect_silence(0.4)


if __name__ == "__main__":
    unittest.main(verbosity=2)
