package matchingengine

/*
#cgo darwin LDFLAGS: -L${SRCDIR}/../../rust/target/release -lmatching_engine
#cgo linux LDFLAGS: -L${SRCDIR}/../../rust/target/release -lmatching_engine

#include <stdlib.h>
#include "../../rust/include/matching_engine.h"
*/
import "C"

import (
	"errors"
	"unsafe"

	"github.com/vmihailenco/msgpack/v5"
)

const DecimalScale int64 = 1_000_000_000

type Engine struct {
	ptr *C.MFEngine
}

type Buffer struct {
	Ptr unsafe.Pointer
	Len int
}

func (b Buffer) Bytes() []byte {
	if b.Ptr == nil || b.Len == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(b.Ptr), b.Len)
}

func (b Buffer) Free() {
	C.mf_buffer_free(C.MFBuffer{ptr: (*C.uint8_t)(b.Ptr), len: C.size_t(b.Len)})
}

func NewEngine(symbols []string) (*Engine, error) {
	if len(symbols) == 0 {
		return nil, errors.New("symbols must be non-empty")
	}
	csyms := make([]*C.char, 0, len(symbols))
	for _, s := range symbols {
		cs := C.CString(s)
		csyms = append(csyms, cs)
	}
	defer func() {
		for _, cs := range csyms {
			C.free(unsafe.Pointer(cs))
		}
	}()

	ptr := C.mf_engine_new((**C.char)(unsafe.Pointer(&csyms[0])), C.size_t(len(csyms)))
	if ptr == nil {
		return nil, errors.New("mf_engine_new failed")
	}
	return &Engine{ptr: ptr}, nil
}

func (e *Engine) Close() {
	if e.ptr != nil {
		C.mf_engine_free(e.ptr)
		e.ptr = nil
	}
}

type Order struct {
	OrderID       string
	ClientOrderID string
	Symbol        string
	Side          uint8 // 0=Buy, 1=Sell
	OrderType     uint8 // 0=Market, 1=Limit
	TimeInForce   uint8 // 0=GTC, 1=Day, 2=IOC, 3=FOK
	Price         int64 // scaled by DecimalScale
	Qty           int64 // scaled by DecimalScale
	TimestampNS   int64
}

func (e *Engine) SubmitOrderEvents(o Order) ([]EngineEvent, error) {
	if e.ptr == nil {
		return nil, errors.New("engine is closed")
	}

	var co C.MFOrder
	var cOrderID *C.char
	if o.OrderID != "" {
		cOrderID = C.CString(o.OrderID)
		defer C.free(unsafe.Pointer(cOrderID))
	}
	co.order_id = cOrderID

	cClient := C.CString(o.ClientOrderID)
	cSymbol := C.CString(o.Symbol)
	defer C.free(unsafe.Pointer(cClient))
	defer C.free(unsafe.Pointer(cSymbol))

	co.client_order_id = cClient
	co.symbol = cSymbol
	co.side = C.uint8_t(o.Side)
	co.order_type = C.uint8_t(o.OrderType)
	co.time_in_force = C.uint8_t(o.TimeInForce)
	co.price = C.int64_t(o.Price)
	co.qty = C.int64_t(o.Qty)
	co.timestamp_ns = C.int64_t(o.TimestampNS)

	var out C.MFBuffer
	rc := C.mf_engine_submit_order_events(e.ptr, &co, &out)
	if rc != 0 {
		return nil, errors.New("mf_engine_submit_order_events failed")
	}
	buf := Buffer{Ptr: unsafe.Pointer(out.ptr), Len: int(out.len)}
	defer buf.Free()

	var events []EngineEvent
	if err := msgpack.Unmarshal(buf.Bytes(), &events); err != nil {
		return nil, err
	}
	return events, nil
}

func (e *Engine) CancelOrderEvents(symbol, orderID string, timestampNS int64) ([]EngineEvent, error) {
	if e.ptr == nil {
		return nil, errors.New("engine is closed")
	}
	cs := C.CString(symbol)
	co := C.CString(orderID)
	defer C.free(unsafe.Pointer(cs))
	defer C.free(unsafe.Pointer(co))

	var out C.MFBuffer
	rc := C.mf_engine_cancel_order_events(e.ptr, cs, co, C.int64_t(timestampNS), &out)
	if rc != 0 {
		return nil, errors.New("mf_engine_cancel_order_events failed")
	}
	buf := Buffer{Ptr: unsafe.Pointer(out.ptr), Len: int(out.len)}
	defer buf.Free()

	var events []EngineEvent
	if err := msgpack.Unmarshal(buf.Bytes(), &events); err != nil {
		return nil, err
	}
	return events, nil
}

func (e *Engine) ReplaceOrderEvents(symbol, orderID string, newPrice, newQty int64, timestampNS int64) ([]EngineEvent, error) {
	if e.ptr == nil {
		return nil, errors.New("engine is closed")
	}
	cs := C.CString(symbol)
	co := C.CString(orderID)
	defer C.free(unsafe.Pointer(cs))
	defer C.free(unsafe.Pointer(co))

	var out C.MFBuffer
	rc := C.mf_engine_replace_order_events(
		e.ptr,
		cs,
		co,
		C.int64_t(newPrice),
		C.int64_t(newQty),
		C.int64_t(timestampNS),
		&out,
	)
	if rc != 0 {
		return nil, errors.New("mf_engine_replace_order_events failed")
	}
	buf := Buffer{Ptr: unsafe.Pointer(out.ptr), Len: int(out.len)}
	defer buf.Free()

	var events []EngineEvent
	if err := msgpack.Unmarshal(buf.Bytes(), &events); err != nil {
		return nil, err
	}
	return events, nil
}

func (e *Engine) BookSnapshot(symbol string, depth int) (BookSnapshot, error) {
	if e.ptr == nil {
		return BookSnapshot{}, errors.New("engine is closed")
	}
	cs := C.CString(symbol)
	defer C.free(unsafe.Pointer(cs))

	var out C.MFBuffer
	rc := C.mf_engine_book_snapshot(e.ptr, cs, C.size_t(depth), &out)
	if rc != 0 {
		return BookSnapshot{}, errors.New("mf_engine_book_snapshot failed")
	}
	buf := Buffer{Ptr: unsafe.Pointer(out.ptr), Len: int(out.len)}
	defer buf.Free()

	var snap BookSnapshot
	if err := msgpack.Unmarshal(buf.Bytes(), &snap); err != nil {
		return BookSnapshot{}, err
	}
	return snap, nil
}

func (e *Engine) SetMarketStatusEvents(symbol string, status uint8, timestampNS int64) ([]EngineEvent, error) {
	if e.ptr == nil {
		return nil, errors.New("engine is closed")
	}
	cs := C.CString(symbol)
	defer C.free(unsafe.Pointer(cs))

	var out C.MFBuffer
	rc := C.mf_engine_set_market_status_events(e.ptr, cs, C.uint8_t(status), C.int64_t(timestampNS), &out)
	if rc != 0 {
		return nil, errors.New("mf_engine_set_market_status_events failed")
	}
	buf := Buffer{Ptr: unsafe.Pointer(out.ptr), Len: int(out.len)}
	defer buf.Free()

	var events []EngineEvent
	if err := msgpack.Unmarshal(buf.Bytes(), &events); err != nil {
		return nil, err
	}
	return events, nil
}

func (e *Engine) OpenAuctionEvents(symbol string, timestampNS int64, referencePrice *int64) ([]EngineEvent, error) {
	if e.ptr == nil {
		return nil, errors.New("engine is closed")
	}
	cs := C.CString(symbol)
	defer C.free(unsafe.Pointer(cs))

	var out C.MFBuffer
	var rp C.int64_t
	var set C.uint8_t
	if referencePrice != nil {
		rp = C.int64_t(*referencePrice)
		set = 1
	}
	rc := C.mf_engine_open_auction_events(e.ptr, cs, C.int64_t(timestampNS), rp, set, &out)
	if rc != 0 {
		return nil, errors.New("mf_engine_open_auction_events failed")
	}
	buf := Buffer{Ptr: unsafe.Pointer(out.ptr), Len: int(out.len)}
	defer buf.Free()

	var events []EngineEvent
	if err := msgpack.Unmarshal(buf.Bytes(), &events); err != nil {
		return nil, err
	}
	return events, nil
}

func (e *Engine) SetPriceRule(symbol string, rule uint8) error {
	if e.ptr == nil {
		return errors.New("engine is closed")
	}
	cs := C.CString(symbol)
	defer C.free(unsafe.Pointer(cs))
	rc := C.mf_engine_set_price_rule(e.ptr, cs, C.uint8_t(rule))
	if rc != 0 {
		return errors.New("mf_engine_set_price_rule failed")
	}
	return nil
}

