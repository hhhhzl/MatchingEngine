package engine

import (
	"math"
	"sync"
	"time"
)

// tokenBucket is a simple leaky-bucket rate limiter.
type tokenBucket struct {
	tokens float64
	last   time.Time
}

type rateLimiter struct {
	mu sync.Mutex
	m  map[string]*tokenBucket
}

func newRateLimiter() *rateLimiter {
	return &rateLimiter{m: map[string]*tokenBucket{}}
}

// Allow consumes 1 token from the bucket identified by key.
// If perSec or burst is zero, the limiter is disabled and always allows.
func (r *rateLimiter) Allow(key string, perSec float64, burst float64) bool {
	if r == nil || perSec <= 0 || burst <= 0 {
		return true
	}
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()

	b, ok := r.m[key]
	if !ok {
		b = &tokenBucket{tokens: burst, last: now}
		r.m[key] = b
	}
	elapsed := now.Sub(b.last).Seconds()
	b.tokens = math.Min(burst, b.tokens+elapsed*perSec)
	b.last = now
	if b.tokens < 1.0 {
		return false
	}
	b.tokens -= 1.0
	return true
}

