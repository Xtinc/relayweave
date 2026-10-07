"""Tests for ordered control-message fragmentation over TCP."""

import unittest
from unittest.mock import patch

import cbor2

from proxy_protocol import (
    CtrlMessage,
    FRAGMENT_PAYLOAD_LENGTH,
    HEADER_LENGTH,
    MAX_MESSAGE_LENGTH,
    MAX_PAYLOAD_LENGTH,
    MessageReceiver,
    from_cbor,
    pack_frame,
)


def payloads(frames: bytes) -> list[bytes]:
    result = []
    offset = 0
    while offset < len(frames):
        length = int.from_bytes(frames[offset:offset + HEADER_LENGTH], "big")
        if not 1 <= length <= MAX_PAYLOAD_LENGTH:
            raise AssertionError("Frame exceeds the payload limit")
        offset += HEADER_LENGTH
        result.append(frames[offset:offset + length])
        offset += length
    return result


class MessageReceiverTest(unittest.TestCase):
    def test_small_wire_format_is_unchanged(self) -> None:
        self.assertEqual(pack_frame(CtrlMessage("ping")), bytes.fromhex("0000000ea167636f6d6d616e646470696e67"))

    def test_plain_message_is_decoded_once(self) -> None:
        message = CtrlMessage("test.message", {"nested": {"values": list(range(1000))}})
        payload = message.to_cbor()
        with patch("proxy_protocol.cbor2.loads", wraps=cbor2.loads) as decode:
            self.assertEqual(MessageReceiver().receive(payload), message)
        self.assertEqual(decode.call_count, 1)

    def test_plain_message_validation_is_preserved(self) -> None:
        for root in ([], {}, {"command": 1}, {"command": "invalid-command"}, {"command": "ping", "params": []}):
            payload = cbor2.dumps(root)
            for decode in (MessageReceiver().receive, from_cbor):
                with self.assertRaises(ValueError):
                    decode(payload)

    def test_decode_failure_resets_assembly(self) -> None:
        message = CtrlMessage("test.large", {"value": "x" * 200000})
        pages = payloads(pack_frame(message))
        receiver = MessageReceiver()
        first = cbor2.loads(pages[0])
        first["__fragment"][2] = b"\xff" + first["__fragment"][2][1:]
        receiver.receive(cbor2.dumps(first))
        for page in pages[1:-1]:
            receiver.receive(page)
        with self.assertRaises(ValueError):
            receiver.receive(pages[-1])
        for page in pages:
            complete = receiver.receive(page)
        self.assertEqual(complete, message)

    def test_encoded_boundaries_and_large_round_trip(self) -> None:
        for size in (MAX_PAYLOAD_LENGTH - 1, MAX_PAYLOAD_LENGTH, MAX_PAYLOAD_LENGTH + 1, 2 * FRAGMENT_PAYLOAD_LENGTH, 200000, MAX_MESSAGE_LENGTH):
            with self.subTest(size=size):
                message = CtrlMessage("test.message", {"value": "x" * (size - 64)})
                for _ in range(2):
                    length = len(message.params["value"]) + size - len(message.to_cbor())
                    message.params["value"] = "x" * length
                pages = payloads(pack_frame(message))
                self.assertEqual(len(pages) == 1, size <= MAX_PAYLOAD_LENGTH)
                receiver = MessageReceiver()
                for index, page in enumerate(pages):
                    complete = receiver.receive(page)
                    self.assertEqual(complete is not None, index + 1 == len(pages))
                self.assertEqual(complete, message)
                if size == MAX_MESSAGE_LENGTH:
                    oversized = MessageReceiver()
                    for page in pages[:-1]:
                        self.assertIsNone(oversized.receive(page))
                    final = cbor2.loads(pages[-1])
                    final["__fragment"][2] += b"x"
                    self.assertIsNone(oversized.receive(cbor2.dumps(final)))

    def test_final_page_with_missing_predecessors_discards_the_message(self) -> None:
        message = CtrlMessage("test.large", {"value": "x" * 200000})
        pages = payloads(pack_frame(message))
        receiver = MessageReceiver()
        self.assertIsNone(receiver.receive(pages[0]))
        self.assertIsNone(receiver.receive(pages[-1]))
        self.assertEqual(receiver.receive(payloads(pack_frame(CtrlMessage("ping")))[0]), CtrlMessage("ping"))
        for page in pages:
            complete = receiver.receive(page)
        self.assertEqual(complete, message)
        self.assertIsNone(MessageReceiver().receive(pages[-1]))

    def test_new_batch_replaces_incomplete_message(self) -> None:
        message = CtrlMessage("test.large", {"value": "x" * 200000})
        pages = payloads(pack_frame(message))
        receiver = MessageReceiver()
        receiver.receive(pages[0])
        for page in pages:
            complete = receiver.receive(page)
        self.assertEqual(complete, message)

    def test_inconsistent_count_discards_the_batch(self) -> None:
        pages = payloads(pack_frame(CtrlMessage("test.large", {"value": "x" * 200000})))
        receiver = MessageReceiver()
        receiver.receive(pages[0])
        changed = cbor2.loads(pages[1])
        changed["__fragment"][1] += 1
        self.assertIsNone(receiver.receive(cbor2.dumps(changed)))
        self.assertIsNone(receiver.receive(pages[-1]))

    def test_empty_final_page_is_rejected(self) -> None:
        pages = payloads(pack_frame(CtrlMessage("test.large", {"value": "x" * 200000})))
        receiver = MessageReceiver()
        for page in pages[:-1]:
            self.assertIsNone(receiver.receive(page))
        final = cbor2.loads(pages[-1])
        final["__fragment"][2] = b""
        self.assertIsNone(receiver.receive(cbor2.dumps(final)))

    def test_invalid_fragment_limits(self) -> None:
        receiver = MessageReceiver()
        for fragment in ([0, 0, b"x"], [0, 100000, b"x"], [0, 2, b"x"], [2, 2, b"x"], [0, 2, b"x" * (FRAGMENT_PAYLOAD_LENGTH + 1)]):
            self.assertIsNone(receiver.receive(cbor2.dumps({"__fragment": fragment})))
        for fragment in ("invalid", [True, 2, b"x"], [0, 2, "text"]):
            with self.assertRaises(ValueError):
                receiver.receive(cbor2.dumps({"__fragment": fragment}))
        with self.assertRaises(ValueError):
            pack_frame(CtrlMessage("test.large", {"value": "x" * MAX_MESSAGE_LENGTH}))


if __name__ == "__main__":
    unittest.main()
