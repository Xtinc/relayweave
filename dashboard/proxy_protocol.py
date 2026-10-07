"""Wire protocol for the TLS proxy control channel.

Mirrors the C++ definitions in protocol/inc/message.h and protocol/src/message.cpp:

  * Frame = 4-byte big-endian length header (1..64 KiB) + CBOR payload.
  * Small payload = CBOR object {"command": str, "params": obj?}.
  * Large messages = consecutive {"__fragment": [index, count, CBOR bytes]} frames.
    TCP assembly delivers only the complete message; a final page with gaps is discarded.
  * Control commands used here:
        server.identify -> server replies server.identified {node_id}
        server.cluster  -> every cluster node replies with one complete server.status.reported message
                              {request_id, node_id, uptime_ms,
                              services:[{service, protocol, rx_bytes, tx_bytes,
                              rx_bytes_per_second, tx_bytes_per_second,
                              accessors:{client: connections, ...}}, ...]}
    Heartbeat: server sends `ping`, client must reply `pong`.

cbor2 is used for (de)serialization so byte output matches nlohmann::json::to_cbor.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Any

import cbor2

# Must match WireMessage::max_payload_length in message.h (64 KiB).
MAX_PAYLOAD_LENGTH = 64 * 1024
HEADER_LENGTH = 4
MAX_MESSAGE_LENGTH = 16 * 1024 * 1024
FRAGMENT_PAYLOAD_LENGTH = MAX_PAYLOAD_LENGTH - 64

# Sentinel the server uses before its first 1-second queue probe completes
# (see RelayNode::schedule_queue_probe -> invalid = uint32_max).
QUEUE_DELAY_UNSET = 0xFFFFFFFF


@dataclass
class CtrlMessage:
    command: str
    params: dict[str, Any] | None = None

    def to_cbor(self) -> bytes:
        if not _valid_command(self.command):
            raise ValueError(f"Invalid command: {self.command!r}")
        root: dict[str, Any] = {"command": self.command}
        if self.params is not None:
            if not isinstance(self.params, dict):
                raise ValueError("Message params must be an object")
            root["params"] = self.params
        return cbor2.dumps(root)


def _from_json(root: Any) -> CtrlMessage:
    if not isinstance(root, dict) or "command" not in root:
        raise ValueError("CBOR root must contain a string field 'command'")
    command = root["command"]
    if not isinstance(command, str) or not _valid_command(command):
        raise ValueError(f"Invalid command: {command!r}")
    params = root.get("params")
    if params is not None and not isinstance(params, dict):
        raise ValueError("'params' must be an object")
    return CtrlMessage(command=command, params=params)


def from_cbor(payload: bytes | bytearray) -> CtrlMessage:
    if not payload:
        raise ValueError("Payload is empty")
    return _from_json(cbor2.loads(payload))


def pack_frame(message: CtrlMessage) -> bytes:
    """Encode one logical message as one or more consecutive wire frames."""
    payload = message.to_cbor()
    if len(payload) > MAX_MESSAGE_LENGTH:
        raise ValueError(f"Control message exceeds 16 MiB: {len(payload)}")
    def frame(data: bytes) -> bytes:
        return struct.pack(">I", len(data)) + data
    if len(payload) <= MAX_PAYLOAD_LENGTH:
        return frame(payload)
    count = (len(payload) + FRAGMENT_PAYLOAD_LENGTH - 1) // FRAGMENT_PAYLOAD_LENGTH
    return b"".join(
        frame(cbor2.dumps({"__fragment": [index, count,
            payload[index * FRAGMENT_PAYLOAD_LENGTH:(index + 1) * FRAGMENT_PAYLOAD_LENGTH]]}))
        for index in range(count)
    )


class MessageReceiver:
    """One ordered assembly buffer per TCP connection, matching C++ MessageReceiver."""

    def __init__(self) -> None:
        self._reset()

    def _reset(self) -> None:
        self._assembled = bytearray()
        self._next_page = 0
        self._page_count = 0

    def receive(self, payload: bytes) -> CtrlMessage | None:
        if not 1 <= len(payload) <= MAX_PAYLOAD_LENGTH:
            raise ValueError("Invalid control frame payload size")
        root = cbor2.loads(payload)
        if not isinstance(root, dict) or "__fragment" not in root:
            self._reset()
            return _from_json(root)
        fragment = root["__fragment"]
        if (len(root) != 1 or not isinstance(fragment, list) or len(fragment) != 3
            or any(type(value) is not int or not 0 <= value <= 0xFFFFFFFFFFFFFFFF for value in fragment[:2])
            or not isinstance(fragment[2], bytes)):
            raise ValueError("Invalid control message fragment envelope")
        index, count, data = fragment
        chunk = FRAGMENT_PAYLOAD_LENGTH
        max_count = (MAX_MESSAGE_LENGTH + chunk - 1) // chunk
        if index == 0:
            self._reset()
            if not 2 <= count <= max_count or len(data) != chunk:
                return None
            self._page_count = count
            self._next_page = 1
            self._assembled.extend(data)
            return None
        if index != self._next_page or count != self._page_count:
            self._reset()
            return None
        final = index == count - 1
        valid_size = (
            0 < len(data) <= min(chunk, MAX_MESSAGE_LENGTH - len(self._assembled))
            if final else len(data) == chunk
        )
        if not valid_size:
            self._reset()
            return None
        self._assembled.extend(data)
        self._next_page += 1
        if not final:
            return None
        assembled = self._assembled
        self._reset()
        return from_cbor(assembled)


def _valid_command(command: str) -> bool:
    return 0 < len(command) <= 32 and all(
        "a" <= char <= "z"
        or "A" <= char <= "Z"
        or "0" <= char <= "9"
        or char == "."
        for char in command
    )
