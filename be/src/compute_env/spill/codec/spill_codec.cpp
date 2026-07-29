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

#include "compute_env/spill/codec/spill_codec.h"

#include <memory>

#include "column/column.h"
#include "column/serde/column_array_serde.h"
#include "compute_env/spill/codec/codec_scalar.h"
#include "compute_env/spill/codec/leaf_classify.h"
#include "gutil/port.h"

namespace starrocks::spill {

namespace {

// RAW: ColumnArraySerde at encode_level=0 -- a pure memcpy-shaped layout, and the
// fallback every selector decision can always demote to.
class RawCodec final : public SpillColumnCodec {
public:
    CodecId id() const override { return CodecId::RAW; }

    int64_t max_encoded_size(const Column& col, uint32_t) const override {
        return serde::ColumnArraySerde::max_serialized_size(col, 0);
    }

    StatusOr<uint8_t*> encode(const Column& col, uint8_t* buf, uint32_t, CodecContext*) const override {
        return serde::ColumnArraySerde::serialize(col, buf, false, 0);
    }

    StatusOr<const uint8_t*> decode(const uint8_t* buf, const uint8_t* end, Column* col) const override {
        return serde::ColumnArraySerde::deserialize(buf, end, col, false, 0);
    }
};

// LEGACY: today's spill encoding (streamvbyte for fixed-length, LZ4 for strings) at an
// arbitrary encode_level. The level is embedded at the payload head (u32), because levels
// can exceed 16 bits (LZ4 acceleration packing, e.g. 90004).
class LegacyCodec final : public SpillColumnCodec {
public:
    CodecId id() const override { return CodecId::LEGACY; }

    int64_t max_encoded_size(const Column& col, uint32_t param) const override {
        int64_t n = serde::ColumnArraySerde::max_serialized_size(col, static_cast<int>(param));
        return n > 0 ? n + static_cast<int64_t>(sizeof(uint32_t)) : n;
    }

    StatusOr<uint8_t*> encode(const Column& col, uint8_t* buf, uint32_t param, CodecContext*) const override {
        UNALIGNED_STORE32(buf, param);
        return serde::ColumnArraySerde::serialize(col, buf + sizeof(uint32_t), false, static_cast<int>(param));
    }

    StatusOr<const uint8_t*> decode(const uint8_t* buf, const uint8_t* end, Column* col) const override {
        if (end - buf < static_cast<ptrdiff_t>(sizeof(uint32_t))) {
            return Status::Corruption("legacy spill codec payload too short");
        }
        uint32_t level = UNALIGNED_LOAD32(buf);
        return serde::ColumnArraySerde::deserialize(buf + sizeof(uint32_t), end, col, false, static_cast<int>(level));
    }
};

} // namespace

CodecRegistry::CodecRegistry() {
    static RawCodec raw;
    static LegacyCodec legacy;
    _by_id.resize(2, nullptr);
    _by_id[static_cast<uint16_t>(CodecId::RAW)] = &raw;
    _by_id[static_cast<uint16_t>(CodecId::LEGACY)] = &legacy;
    install_scalar_codecs(&_by_id);
}

CodecRegistry* CodecRegistry::instance() {
    static CodecRegistry registry;
    return &registry;
}

const SpillColumnCodec* CodecRegistry::get(CodecId id) const {
    auto idx = static_cast<uint16_t>(id);
    return idx < _by_id.size() ? _by_id[idx] : nullptr;
}

std::vector<CodecCandidate> CodecRegistry::candidates(const Column& col, int session_encode_level) const {
    // Decision-tree layer 1: the schema prior prunes the candidate set per column type.
    // Layer 2/3 (sampled trial-encoding and the final pick) live in CodecSelector.
    std::vector<CodecCandidate> out;
    if (session_encode_level == 0) {
        // spill_encode_level=0 keeps its historical meaning: no encoding at all.
        out.push_back({CodecId::RAW, 0});
        return out;
    }
    const auto level = static_cast<uint32_t>(session_encode_level);
    LeafView v = classify_leaf(col);
    switch (v.kind) {
    case LeafKind::BOOL:
        out = {{CodecId::BOOL_RLE, 0}, {CodecId::BOOL_BITPACK, 0}, {CodecId::RAW, 0}};
        break;
    case LeafKind::I32:
    case LeafKind::I64:
        // LEGACY (streamvbyte) stays in the set: structurally we can never lose to today.
        out = {{CodecId::INT_RLE, 0},  {CodecId::INT_DELTA, 0},  {CodecId::INT_FOR, 0},
               {CodecId::INT_PFOR, 0}, {CodecId::LEGACY, level}, {CodecId::RAW, 0}};
        break;
    case LeafKind::STR:
        // LEGACY here = the current column-LZ4 path
        out = {{CodecId::STR_DICT, 0},      {CodecId::STR_FRONT, 0},  {CodecId::STR_BLOCK_ZSTD, 0},
               {CodecId::STR_BLOCK_LZ4, 0}, {CodecId::LEGACY, level}, {CodecId::RAW, 0}};
        break;
    case LeafKind::F64:
        out = {{CodecId::FP_ALP, 0}, {CodecId::RAW, 0}};
        break;
    default:
        // float32/complex/int128/large-binary: generic block compression is their first
        // non-LEGACY option; the cost model decides whether it is worth the CPU.
        out = {{CodecId::RAW, 0}, {CodecId::LEGACY, level}};
        break;
    }
    // the fallback compression layer competes everywhere (zstd for ratio, lz4 for speed)
    out.push_back({CodecId::GENERIC_BLOCK_ZSTD, 0});
    out.push_back({CodecId::GENERIC_BLOCK_LZ4, 0});
    return out;
}

} // namespace starrocks::spill
