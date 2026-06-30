// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstddef>
#include <cstdint>

namespace starrocks {
class faststring;

// Architecture-neutral scalar PFOR (Patched Frame-Of-Reference) codec used by the builtin
// GIN BM25 block posting to encode a block's docid gaps and term frequencies.
//
// It is deliberately a hand-written, fixed byte layout: NO TurboPFor, NO SIMD-tagged format.
// Shared-data segments live on object storage and are read by arbitrary x86/ARM compute nodes,
// so the encoding must decode identically everywhere. x86 and ARM are both little-endian and the
// packing below is pure byte/shift work, so the bytes are reproducible across architectures.
//
// On-disk format for one uint32 stream of `n` values (the frozen format, see
// docs/design/builtin-gin-bm25-pr1-storage-plan.md §3.1):
//
//   [bit_width      : u8]                              low-bit width b in [0, 32]
//   [num_exceptions : u8]                              count of values whose (val >> b) != 0
//   [exceptions     : num_exceptions x {pos:u8, high:varint}]   pos in [0, n), high = val >> b
//   [low_stream     : n values x b bits, LSB-first bit-packed, ceil(n*b/8) bytes]
//
// Decode reconstructs each value as low_bits, then for every exception ORs in (high << b).
// `n` must be <= 255 so that `pos` and `num_exceptions` fit in u8; the GIN block size is 128.
namespace gin_pfor {

// Append the PFOR encoding of vals[0..n) to *out. n may be 0 (emits a 2-byte empty header).
void encode(const uint32_t* vals, size_t n, faststring* out);

// Decode exactly `n` values from data[0..len) into out[0..n).
// Returns the number of bytes consumed, or 0 on malformed / truncated input.
size_t decode(const uint8_t* data, size_t len, size_t n, uint32_t* out);

} // namespace gin_pfor
} // namespace starrocks
