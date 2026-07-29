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

// Spill column codec module: pluggable per-column encoding for the spill serde.
//
// This module externalizes the two roles that were fused inside ColumnarSerde/
// ColumnArraySerde:
//   - encoding EXECUTION: SpillColumnCodec implementations (this file + codec impls),
//     dispatched by a stable on-stream CodecId;
//   - encoding SELECTION: CodecSelector (codec_selector.h), which walks the decision
//     tree "type prior -> sampled evidence -> concrete codec".
//
// Contract:
//   - Payloads are SELF-DESCRIBING: any parameter a codec needs to decode (encode level,
//     bit width, dictionary, ...) is embedded at the head of its own payload. The chunk
//     header stores only the CodecId per column.
//   - Every chunk is INDEPENDENTLY decodable (no cross-chunk state).
//   - Spill data never crosses process/version boundaries (written and read by the same
//     query), so CodecId values only need to stay stable within one binary.
//
// Scope: used exclusively by the spill serde (compute_env/spill). ColumnArraySerde and
// the exchange serialization path are intentionally untouched; ColumnArraySerde is only
// wrapped here as the RAW / LEGACY fallback codecs.

#include <cstdint>
#include <vector>

#include "column/vectorized_fwd.h"
#include "common/statusor.h"

namespace starrocks {
class Column;
}

namespace starrocks::spill {

// On-stream codec ids (u16), grouped by the column family each codec serves. The ids only need
// to be stable WITHIN one binary -- a spill stream is written and read by the same query in the
// same process, and BE startup deletes residual spill directories -- so they carry no
// cross-version compatibility obligation.
enum class CodecId : uint16_t {
    RAW = 0,    // ColumnArraySerde encode_level=0 passthrough (no encoding)
    LEGACY = 1, // ColumnArraySerde with an embedded encode_level (streamvbyte / LZ4)
    // integers (codec_scalar.cpp); these and every codec below use the nullable framing
    INT_RLE = 2,
    INT_DELTA = 3,
    INT_FOR = 4,
    INT_PFOR = 5,
    // booleans
    BOOL_BITPACK = 6,
    BOOL_RLE = 7,
    // strings
    STR_DICT = 8,
    STR_FRONT = 9,
    STR_BLOCK_LZ4 = 10,
    STR_BLOCK_ZSTD = 11,
    // doubles
    FP_ALP = 12,
    // any column type, including complex ones no specialized codec covers
    GENERIC_BLOCK_LZ4 = 13,
    GENERIC_BLOCK_ZSTD = 14,
};

// A concrete codec choice for one column: the codec plus an opaque parameter the codec
// interprets at encode time (e.g. LEGACY's encode_level). `param` is NOT written to the
// stream; codecs embed whatever they need to decode inside their payload.
struct CodecCandidate {
    CodecId id = CodecId::RAW;
    uint32_t param = 0;
};

// Optional per-column encode-side state, owned by the CodecSelector and reused across
// chunks of one Spiller (e.g. FSST's symbol table, whose construction costs ~ms and is
// designed to amortize over many blocks). Decoding never needs it: payloads stay fully
// self-describing per chunk.
struct CodecContext {
    virtual ~CodecContext() = default;
};

class SpillColumnCodec {
public:
    virtual ~SpillColumnCodec() = default;

    virtual CodecId id() const = 0;

    // Upper bound of the encoded payload for `col` under `param`.
    virtual int64_t max_encoded_size(const Column& col, uint32_t param) const = 0;

    // One-time per-column encode state built from a representative column; nullptr if the
    // codec is stateless. Called OUTSIDE the selector's timed trials, so its cost does not
    // pollute the steady-state score (it amortizes over the locked phase).
    virtual StatusOr<std::shared_ptr<CodecContext>> create_context(const Column& col) const {
        return std::shared_ptr<CodecContext>();
    }

    // Encode the whole (possibly nullable/nested) column; returns the advanced cursor.
    // `ctx` may be nullptr; codecs with contexts must also work without one.
    virtual StatusOr<uint8_t*> encode(const Column& col, uint8_t* buf, uint32_t param,
                                      CodecContext* ctx = nullptr) const = 0;

    // Decode into `col` (an empty column of the right type); returns the advanced cursor.
    virtual StatusOr<const uint8_t*> decode(const uint8_t* buf, const uint8_t* end, Column* col) const = 0;
};

// Process-wide registry: id -> codec, and the decision tree's first layer
// (column type -> candidate set).
class CodecRegistry {
public:
    static CodecRegistry* instance();

    // nullptr if the id is unknown (corrupt stream / newer writer).
    const SpillColumnCodec* get(CodecId id) const;

    // Candidate set for one column (decision-tree layer 1: schema prior).
    // `session_encode_level` is the spill_encode_level session value; 0 disables encoding.
    std::vector<CodecCandidate> candidates(const Column& col, int session_encode_level) const;

private:
    CodecRegistry();
    std::vector<const SpillColumnCodec*> _by_id;
};

} // namespace starrocks::spill
