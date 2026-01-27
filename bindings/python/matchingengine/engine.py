"""
Python bindings for MatchingEngine via ctypes + MessagePack.

All numeric inputs are fixed-point scaled integers:
Decimal = value / DECIMAL_SCALE
"""

from __future__ import annotations

import ctypes
import os
from dataclasses import dataclass
from typing import List, Optional, Sequence, Any

import msgpack

DECIMAL_SCALE = 1_000_000_000


class MFBuffer(ctypes.Structure):
    _fields_ = [
        ("ptr", ctypes.POINTER(ctypes.c_uint8)),
        ("len", ctypes.c_size_t),
    ]


class MFOrder(ctypes.Structure):
    _fields_ = [
        ("order_id", ctypes.c_char_p),
        ("client_order_id", ctypes.c_char_p),
        ("symbol", ctypes.c_char_p),
        ("side", ctypes.c_uint8),
        ("order_type", ctypes.c_uint8),
        ("time_in_force", ctypes.c_uint8),
        ("price", ctypes.c_int64),
        ("qty", ctypes.c_int64),
        ("timestamp_ns", ctypes.c_int64),
    ]


@dataclass
class Order:
    client_order_id: str
    symbol: str
    side: int
    order_type: int
    time_in_force: int
    price: int
    qty: int
    timestamp_ns: int
    order_id: str = ""


def _default_lib_path() -> str:
    # Expect user to build Rust library at ../../../rust/target/release
    # (this file lives in bindings/python/matchingengine/engine.py)
    base = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "rust", "target", "release")
    )
    if sys_platform() == "darwin":
        return os.path.join(base, "libmatching_engine.dylib")
    if sys_platform() == "linux":
        return os.path.join(base, "libmatching_engine.so")
    if sys_platform() == "win32":
        return os.path.join(base, "matching_engine.dll")
    return os.path.join(base, "libmatching_engine.so")


def sys_platform() -> str:
    import sys

    return sys.platform


class Engine:
    def __init__(self, symbols: Sequence[str], lib_path: Optional[str] = None):
        if not symbols:
            raise ValueError("symbols must be non-empty")
        if lib_path is None:
            lib_path = _default_lib_path()

        self._lib = ctypes.CDLL(lib_path)
        self._configure_abi()

        arr = (ctypes.c_char_p * len(symbols))()
        for i, s in enumerate(symbols):
            arr[i] = s.encode("utf-8")

        self._ptr = self._lib.mf_engine_new(arr, ctypes.c_size_t(len(symbols)))
        if not self._ptr:
            raise RuntimeError("mf_engine_new failed")

    def close(self) -> None:
        if getattr(self, "_ptr", None):
            self._lib.mf_engine_free(self._ptr)
            self._ptr = None

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def _configure_abi(self) -> None:
        self._lib.mf_engine_new.argtypes = [ctypes.POINTER(ctypes.c_char_p), ctypes.c_size_t]
        self._lib.mf_engine_new.restype = ctypes.c_void_p

        self._lib.mf_engine_free.argtypes = [ctypes.c_void_p]
        self._lib.mf_engine_free.restype = None

        self._lib.mf_buffer_free.argtypes = [MFBuffer]
        self._lib.mf_buffer_free.restype = None

        self._lib.mf_engine_submit_order_events.argtypes = [ctypes.c_void_p, ctypes.POINTER(MFOrder), ctypes.POINTER(MFBuffer)]
        self._lib.mf_engine_submit_order_events.restype = ctypes.c_int

        self._lib.mf_engine_cancel_order_events.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.c_int64,
            ctypes.POINTER(MFBuffer),
        ]
        self._lib.mf_engine_cancel_order_events.restype = ctypes.c_int

        self._lib.mf_engine_replace_order_events.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.c_int64,
            ctypes.c_int64,
            ctypes.c_int64,
            ctypes.POINTER(MFBuffer),
        ]
        self._lib.mf_engine_replace_order_events.restype = ctypes.c_int

        self._lib.mf_engine_book_snapshot.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t, ctypes.POINTER(MFBuffer)]
        self._lib.mf_engine_book_snapshot.restype = ctypes.c_int

        self._lib.mf_engine_set_market_status_events.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_uint8,
            ctypes.c_int64,
            ctypes.POINTER(MFBuffer),
        ]
        self._lib.mf_engine_set_market_status_events.restype = ctypes.c_int

        self._lib.mf_engine_set_price_rule.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint8]
        self._lib.mf_engine_set_price_rule.restype = ctypes.c_int

        self._lib.mf_engine_open_auction_events.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_int64,
            ctypes.c_int64,
            ctypes.c_uint8,
            ctypes.POINTER(MFBuffer),
        ]
        self._lib.mf_engine_open_auction_events.restype = ctypes.c_int

    def _decode(self, buf: MFBuffer) -> Any:
        try:
            raw = ctypes.string_at(buf.ptr, buf.len)
            return msgpack.unpackb(raw, raw=False)
        finally:
            self._lib.mf_buffer_free(buf)

    def submit_order_events(self, order: Order) -> Any:
        mo = MFOrder()
        mo.order_id = order.order_id.encode("utf-8") if order.order_id else None
        mo.client_order_id = order.client_order_id.encode("utf-8")
        mo.symbol = order.symbol.encode("utf-8")
        mo.side = ctypes.c_uint8(order.side)
        mo.order_type = ctypes.c_uint8(order.order_type)
        mo.time_in_force = ctypes.c_uint8(order.time_in_force)
        mo.price = ctypes.c_int64(order.price)
        mo.qty = ctypes.c_int64(order.qty)
        mo.timestamp_ns = ctypes.c_int64(order.timestamp_ns)

        out = MFBuffer()
        rc = self._lib.mf_engine_submit_order_events(self._ptr, ctypes.byref(mo), ctypes.byref(out))
        if rc != 0:
            raise RuntimeError("mf_engine_submit_order_events failed")
        return self._decode(out)

    def cancel_order_events(self, symbol: str, order_id: str, timestamp_ns: int) -> Any:
        out = MFBuffer()
        rc = self._lib.mf_engine_cancel_order_events(
            self._ptr,
            symbol.encode("utf-8"),
            order_id.encode("utf-8"),
            ctypes.c_int64(timestamp_ns),
            ctypes.byref(out),
        )
        if rc != 0:
            raise RuntimeError("mf_engine_cancel_order_events failed")
        return self._decode(out)

    def replace_order_events(self, symbol: str, order_id: str, new_price: int, new_qty: int, timestamp_ns: int) -> Any:
        out = MFBuffer()
        rc = self._lib.mf_engine_replace_order_events(
            self._ptr,
            symbol.encode("utf-8"),
            order_id.encode("utf-8"),
            ctypes.c_int64(new_price),
            ctypes.c_int64(new_qty),
            ctypes.c_int64(timestamp_ns),
            ctypes.byref(out),
        )
        if rc != 0:
            raise RuntimeError("mf_engine_replace_order_events failed")
        return self._decode(out)

    def book_snapshot(self, symbol: str, depth: int) -> Any:
        out = MFBuffer()
        rc = self._lib.mf_engine_book_snapshot(self._ptr, symbol.encode("utf-8"), ctypes.c_size_t(depth), ctypes.byref(out))
        if rc != 0:
            raise RuntimeError("mf_engine_book_snapshot failed")
        return self._decode(out)

    def set_market_status_events(self, symbol: str, status: int, timestamp_ns: int) -> Any:
        out = MFBuffer()
        rc = self._lib.mf_engine_set_market_status_events(
            self._ptr,
            symbol.encode("utf-8"),
            ctypes.c_uint8(status),
            ctypes.c_int64(timestamp_ns),
            ctypes.byref(out),
        )
        if rc != 0:
            raise RuntimeError("mf_engine_set_market_status_events failed")
        return self._decode(out)

    def set_price_rule(self, symbol: str, rule: int) -> None:
        rc = self._lib.mf_engine_set_price_rule(self._ptr, symbol.encode("utf-8"), ctypes.c_uint8(rule))
        if rc != 0:
            raise RuntimeError("mf_engine_set_price_rule failed")

    def open_auction_events(self, symbol: str, timestamp_ns: int, reference_price: Optional[int] = None) -> Any:
        out = MFBuffer()
        rp = ctypes.c_int64(reference_price or 0)
        rp_set = ctypes.c_uint8(1 if reference_price is not None else 0)
        rc = self._lib.mf_engine_open_auction_events(
            self._ptr,
            symbol.encode("utf-8"),
            ctypes.c_int64(timestamp_ns),
            rp,
            rp_set,
            ctypes.byref(out),
        )
        if rc != 0:
            raise RuntimeError("mf_engine_open_auction_events failed")
        return self._decode(out)

