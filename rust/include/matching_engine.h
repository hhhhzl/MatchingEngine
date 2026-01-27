// MatchingEngine C ABI
//
// This header defines a stable C interface for the Rust matching engine.
// All returned buffers are MessagePack-encoded and must be freed with mf_buffer_free.
//
// Decimal encoding:
// - price/qty inputs are scaled integers: Decimal = value / MF_DECIMAL_SCALE.

#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Error codes returned by API functions.
enum {
  MF_OK = 0,
  MF_ERR_NULL = 1,
  MF_ERR_INVALID = 2,
  MF_ERR_INTERNAL = 3,
};

// Fixed scale factor for price/qty inputs (1e9).
static const int64_t MF_DECIMAL_SCALE = 1000000000LL;

typedef struct MFBuffer {
  uint8_t* ptr;
  size_t len;
} MFBuffer;

typedef struct MFOrder {
  const char* order_id;        // optional (nullable)
  const char* client_order_id; // required
  const char* symbol;          // required
  uint8_t side;                // 0=Buy, 1=Sell
  uint8_t order_type;          // 0=Market, 1=Limit
  uint8_t time_in_force;       // 0=GTC, 1=Day, 2=IOC, 3=FOK
  int64_t price;               // scaled by MF_DECIMAL_SCALE
  int64_t qty;                 // scaled by MF_DECIMAL_SCALE
  int64_t timestamp_ns;
} MFOrder;

typedef struct MFEngine MFEngine;

void mf_buffer_free(MFBuffer buf);

MFEngine* mf_engine_new(const char** symbols, size_t n);
void mf_engine_free(MFEngine* engine);

int mf_engine_submit_order_events(MFEngine* engine, const MFOrder* order, MFBuffer* out);
int mf_engine_cancel_order_events(MFEngine* engine, const char* symbol, const char* order_id, int64_t timestamp_ns, MFBuffer* out);
int mf_engine_replace_order_events(MFEngine* engine, const char* symbol, const char* order_id, int64_t new_price, int64_t new_qty, int64_t timestamp_ns, MFBuffer* out);
int mf_engine_set_market_status_events(MFEngine* engine, const char* symbol, uint8_t status, int64_t timestamp_ns, MFBuffer* out);
int mf_engine_set_price_rule(MFEngine* engine, const char* symbol, uint8_t rule);
int mf_engine_open_auction_events(MFEngine* engine, const char* symbol, int64_t timestamp_ns, int64_t reference_price, uint8_t reference_price_is_set, MFBuffer* out);
int mf_engine_book_snapshot(MFEngine* engine, const char* symbol, size_t depth, MFBuffer* out);

#ifdef __cplusplus
} // extern "C"
#endif

