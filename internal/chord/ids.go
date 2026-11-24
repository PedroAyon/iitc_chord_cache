package chord

import (
	"crypto/sha1"
	"encoding/hex"
	"math/big"
	"unsafe"
)

const (
	M       = 160
	idBytes = M / 8
)

var (
	ringSize    = new(big.Int).Lsh(big.NewInt(1), M)
	powersOfTwo [M]*big.Int
)

func init() {
	for i := range M {
		powersOfTwo[i] = new(big.Int).Lsh(big.NewInt(1), uint(i))
	}
}

func HashID(s string) *big.Int {
	// Zero-copy conversion; sha1.Sum does not retain or mutate the slice.
	b := unsafe.Slice(unsafe.StringData(s), len(s))
	sum := sha1.Sum(b)
	return new(big.Int).SetBytes(sum[:])
}

func IDBytes(id *big.Int) []byte {
	b := make([]byte, idBytes)
	id.FillBytes(b)
	return b
}

func IDFromBytes(b []byte) *big.Int { return new(big.Int).SetBytes(b) }

func IDHex(id *big.Int) string {
	// Stack-allocate the 20-byte buffer instead of calling IDBytes(id).
	var b [idBytes]byte
	id.FillBytes(b[:])
	return hex.EncodeToString(b[:])
}

func ShortHex(b []byte) string {
	if len(b) > 4 {
		b = b[:4]
	}
	return hex.EncodeToString(b)
}

// inOpen reports whether x lies in the circular interval (a, b).
func inOpen(x, a, b *big.Int) bool {
	switch a.Cmp(b) {
	case -1:
		return x.Cmp(a) > 0 && x.Cmp(b) < 0
	case 1:
		return x.Cmp(a) > 0 || x.Cmp(b) < 0
	default:
		return x.Cmp(a) != 0
	}
}

// inHalfOpen reports whether x lies in the circular interval (a, b].
func inHalfOpen(x, a, b *big.Int) bool {
	// Inline the switch to avoid calling x.Cmp(b) twice on misses.
	switch a.Cmp(b) {
	case -1:
		return x.Cmp(a) > 0 && x.Cmp(b) <= 0
	case 1:
		return x.Cmp(a) > 0 || x.Cmp(b) <= 0
	default:
		return true
	}
}

// fingerStart returns (n + 2^i) mod 2^M for a zero-based finger index i (0 <= i < M).
func fingerStart(n *big.Int, i int) *big.Int {
	s := new(big.Int).Add(n, powersOfTwo[i])
	// Assuming 0 <= n < 2^M and 0 <= i < M, their sum is strictly less than 2^(M+1).
	// Clearing bit M performs mod 2^M in O(1) without big.Int division.
	return s.SetBit(s, M, 0)
}
