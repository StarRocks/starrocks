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

#include "compute_env/spill/serde.h"

#include <cstring>
#include <limits>

#include "base/container/raw_container.h"
#include "base/time/time.h"
#include "base/utility/alignment.h"
#include "column/serde/column_array_serde.h"
#include "common/config_exec_flow_fwd.h"
#include "common/statusor.h"
#include "compute_env/spill/codec/codec_selector.h"
#include "compute_env/spill/codec/spill_codec.h"
#include "compute_env/spill/options.h"
#include "compute_env/spill/spiller.h"
#include "gutil/port.h"
#include "runtime/runtime_state.h"
#include "runtime/serde/chunk_encode_context.h"

namespace starrocks::spill {

namespace {
// The cost models' CPU-vs-bytes exchange rate, in bytes/ns: how many bytes of IO one nanosecond
// of encode+decode CPU is worth. A POLICY input, not a measurement of any real device -- see
// spill_codec_disk_bandwidth_mbps. Read on every call because the config is runtime-mutable; the
// <=0 guard covers a process that never ran config::init(), where numeric defaults read as 0.
double cost_model_disk_bytes_per_ns() {
    double mbps = config::spill_codec_disk_bandwidth_mbps;
    if (mbps <= 0) mbps = 200;
    return mbps / 1000.0;
}
} // namespace

class ColumnarSerde : public Serde {
public:
    ColumnarSerde(Spiller* parent, ChunkBuilder chunk_builder)
            : Serde(parent), _chunk_builder(std::move(chunk_builder)) {}
    ~ColumnarSerde() override = default;

    Status prepare() override {
        RACE_DETECT(detect_prepare);
        if (config::spill_enable_codec_selector) {
            // v2 path: pluggable codec module (selection via CodecSelector, execution via
            // CodecRegistry). The legacy EncodeContext is not created in this mode.
            if (_codec_selector == nullptr) {
                auto column_number = _parent->chunk_builder().column_number();
                auto encode_level = _parent->options().encode_level;
                _codec_selector =
                        std::make_shared<CodecSelector>(column_number, encode_level, cost_model_disk_bytes_per_ns());
            }
            return Status::OK();
        }
        if (_encode_context == nullptr) {
            auto column_number = _parent->chunk_builder().column_number();
            auto encode_level = _parent->options().encode_level;
            _encode_context = serde::EncodeContext::get_encode_context_shared_ptr(column_number, encode_level);
        }
        return Status::OK();
    }

    StatusOr<ChunkUniquePtr> deserialize(SerdeContext& ctx, BlockReader* reader) override;
    Status serialize(RuntimeState* state, SerdeContext& ctx, const ChunkPtr& chunk,
                     const SpillOutputDataStreamPtr& output, bool aligned) override;

private:
    Status serialize_v2(RuntimeState* state, SerdeContext& ctx, const ChunkPtr& chunk,
                        const SpillOutputDataStreamPtr& output, bool aligned);
    StatusOr<ChunkUniquePtr> deserialize_v2(SerdeContext& ctx, size_t attachment_size);

    // data format
    // header|encode levels|attachment...
    // header:
    // i32 sequence_id|i64 attachment size
    static constexpr int32_t SEQUENCE_OFFSET = serde_proto::SEQUENCE_OFFSET;
    static constexpr int32_t ATTACHMENT_SIZE_OFFSET = serde_proto::ATTACHMENT_SIZE_OFFSET;
    static constexpr int32_t HEADER_SIZE = serde_proto::HEADER_SIZE;
    static constexpr int32_t SEQUENCE_MAGIC_ID = 0xface;
    // v2 format: header | per-column codec desc (u32 = codec_id u16 << 16 | reserved) | payloads.
    // Distinguished from v1 by the magic so readers auto-detect the version.
    static constexpr int32_t SEQUENCE_MAGIC_ID_V2 = 0xfacd;

    // Writes the v2 prologue -- the header placeholder plus one descriptor per column -- and
    // returns the cursor at the first payload byte. Both serialize_v2 paths go through here so
    // the descriptor layout documented above has exactly one definition to keep in sync with
    // deserialize_v2().
    uint8_t* _write_v2_prologue(uint8_t* buf, const std::vector<CodecCandidate>& chosen) const {
        buf += HEADER_SIZE;
        for (const auto& cand : chosen) {
            UNALIGNED_STORE32(buf, static_cast<uint32_t>(static_cast<uint16_t>(cand.id)) << 16);
            buf += sizeof(uint32_t);
        }
        return buf;
    }

    size_t _max_serialized_size(const ChunkPtr& chunk) const;

    inline const std::vector<uint32_t>& _get_encode_levels() {
        DCHECK(_encode_context != nullptr);
        std::shared_lock l(_mutex);
        return _encode_context->get_encode_levels();
    }

    inline void _update_encode_stats(const std::vector<std::pair<uint64_t, uint64_t>>& column_stats) {
        DCHECK(_encode_context != nullptr);
        std::unique_lock l(_mutex);
        for (size_t i = 0; i < column_stats.size(); i++) {
            _encode_context->update(i, column_stats[i].first, column_stats[i].second);
        }
        _encode_context->adjust_encode_levels();
    }

    ChunkBuilder _chunk_builder;
    // assuming that the chunks processed by the same Spiller are similar,
    // so we maintain a context for each ColumnarSerde, which may be accessed by multiple threads.
    // here a std::shared_mutex is used to ensure concurrency safety.
    std::shared_mutex _mutex;
    std::shared_ptr<serde::EncodeContext> _encode_context;
    // v2 path (config::spill_enable_codec_selector): per-Spiller codec decision state.
    // Same sharing/concurrency model as _encode_context; only one of the two is created.
    std::shared_ptr<CodecSelector> _codec_selector;
    DECLARE_RACE_DETECTOR(detect_prepare)
};

size_t ColumnarSerde::_max_serialized_size(const ChunkPtr& chunk) const {
    size_t total_size = 0;
    const auto& columns = chunk->columns();
    if (_encode_context == nullptr) {
        for (const auto& column : columns) {
            total_size += serde::ColumnArraySerde::max_serialized_size(*column);
        }
    } else {
        for (size_t i = 0; i < columns.size(); i++) {
            total_size +=
                    serde::ColumnArraySerde::max_serialized_size(*columns[i], _encode_context->get_encode_level(i));
        }
    }
    return total_size;
}

Status ColumnarSerde::serialize(RuntimeState* state, SerdeContext& ctx, const ChunkPtr& chunk,
                                const SpillOutputDataStreamPtr& output, bool aligned) {
    if (_codec_selector != nullptr) {
        return serialize_v2(state, ctx, chunk, output, aligned);
    }
    // Defense in depth: prepare() must have created the encode context before any serialize()
    // call. Return an error instead of dereferencing a null context in _get_encode_levels()
    // (which would crash the process), mirroring the existing null guard in _max_serialized_size().
    if (_encode_context == nullptr) {
        return Status::InternalError("ColumnarSerde::serialize() called before prepare(): encode context is null");
    }
    raw::RawStringPage& serialize_buffer = ctx.serialize_buffer;
    {
        SCOPED_TIMER(_parent->metrics().serialize_timer);
        size_t ALIGNED_SIZE = 1;
        if (aligned) {
            ALIGNED_SIZE = AlignedBuffer::kPageSize;
        }
        ctx.serialize_buffer.clear();
        const auto& columns = chunk->columns();
        // header|attachment...
        // i32 sequence_id|i64 chunk size|encode level|attachment(column data)...
        char header_buffer[HEADER_SIZE];
        UNALIGNED_STORE32(header_buffer + SEQUENCE_OFFSET, SEQUENCE_MAGIC_ID);

        size_t encode_level_sizes = columns.size() * sizeof(int32_t);
        size_t max_serialized_size = _max_serialized_size(chunk);
        ctx.serialize_buffer.resize(ALIGN_UP(HEADER_SIZE + encode_level_sizes + max_serialized_size, ALIGNED_SIZE));
        uint8_t* buf = reinterpret_cast<uint8_t*>(serialize_buffer.data());
        const uint8_t* head = buf;

        // acquire encode level
        auto encode_levels = _get_encode_levels();
        {
            buf = buf + HEADER_SIZE;
            for (auto encode_level : encode_levels) {
                UNALIGNED_STORE32(buf, encode_level);
                buf += sizeof(uint32_t);
            }
        }

        // used to record raw_bytes and encoded_bytes for each column
        std::vector<std::pair<uint64_t, uint64_t>> column_stats;
        column_stats.reserve(columns.size());
        // serialize to io buffer
        int padding_size = 0;
        if (UNLIKELY(config::pipeline_enable_large_column_checker)) {
            if (chunk->has_capacity_limit_reached()) {
                return Status::CapacityLimitExceed(fmt::format("Large column detected in spill serialize phase "));
            }
        }
        for (size_t i = 0; i < columns.size(); i++) {
            uint8_t* begin = buf;
            ASSIGN_OR_RETURN(buf, serde::ColumnArraySerde::serialize(*columns[i], buf, false, encode_levels[i]));
            column_stats.emplace_back(columns[i]->byte_size(), buf - begin);
            if (serde::EncodeContext::enable_encode_integer(encode_levels[i])) {
                padding_size = serde::EncodeContext::STREAMVBYTE_PADDING_SIZE;
            }
        }
        _update_encode_stats(column_stats);
        // total serialized size
        size_t content_length = buf - head;
        auto align_size = ALIGN_UP(content_length + padding_size, ALIGNED_SIZE);
        serialize_buffer.resize(align_size);
        UNALIGNED_STORE64(header_buffer + ATTACHMENT_SIZE_OFFSET, align_size - HEADER_SIZE);
        memcpy(serialize_buffer.data(), header_buffer, HEADER_SIZE);
    }
    size_t written_bytes = serialize_buffer.size();
    RETURN_IF_ERROR(
            output->append(state, {Slice(serialize_buffer.data(), written_bytes)}, written_bytes, chunk->num_rows()));
    return Status::OK();
}

StatusOr<ChunkUniquePtr> ColumnarSerde::deserialize(SerdeContext& ctx, BlockReader* reader) {
    char header_buffer[HEADER_SIZE];
    RETURN_IF_ERROR(reader->read_fully(header_buffer, HEADER_SIZE));

    int32_t sequence_id = UNALIGNED_LOAD32(header_buffer + SEQUENCE_OFFSET);
    size_t attachment_size = UNALIGNED_LOAD64(header_buffer + ATTACHMENT_SIZE_OFFSET);
    if (sequence_id != SEQUENCE_MAGIC_ID && sequence_id != SEQUENCE_MAGIC_ID_V2) {
        return Status::InternalError(fmt::format("sequence id mismatch {} vs {}", sequence_id, SEQUENCE_MAGIC_ID));
    }

    auto chunk = _chunk_builder();
    auto& columns = chunk->columns();

    auto& serialize_buffer = ctx.serialize_buffer;
    serialize_buffer.resize(attachment_size);

    auto buf = reinterpret_cast<uint8_t*>(serialize_buffer.data());
    const uint8_t* end = buf + serialize_buffer.size();
    {
        auto st = reader->read_fully(buf, attachment_size);
        RETURN_IF(st.is_end_of_file(), Status::InternalError("not found enough data in block"));
        RETURN_IF_ERROR(st);
    }

    if (sequence_id == SEQUENCE_MAGIC_ID_V2) {
        return deserialize_v2(ctx, attachment_size);
    }

    const uint32_t* encode_levels = nullptr;
    const uint8_t* read_cursor = buf;
    encode_levels = reinterpret_cast<uint32_t*>(serialize_buffer.data());

    read_cursor += columns.size() * sizeof(uint32_t);
    SCOPED_TIMER(_parent->metrics().deserialize_timer);
    for (size_t i = 0; i < columns.size(); i++) {
        ASSIGN_OR_RETURN(read_cursor,
                         serde::ColumnArraySerde::deserialize(read_cursor, end, columns[i]->as_mutable_raw_ptr(), false,
                                                              encode_levels[i]));
    }

    TRACE_SPILL_LOG << "deserialize chunk from block: " << reader->debug_string()
                    << ", encoded size: " << attachment_size << ", original size: " << chunk->bytes_usage();
    return chunk;
}

// v2 serialize: per-column codec chosen by CodecSelector (decision tree), executed via
// CodecRegistry. Layout: header | codec desc u32 x ncol | self-describing payloads.
// Sampling chunks (first kSamples of each kWindow) trial-encode every candidate to feed
// the selector, then emit the smallest; locked chunks encode straight into the buffer.
Status ColumnarSerde::serialize_v2(RuntimeState* state, SerdeContext& ctx, const ChunkPtr& chunk,
                                   const SpillOutputDataStreamPtr& output, bool aligned) {
    raw::RawStringPage& serialize_buffer = ctx.serialize_buffer;
    {
        SCOPED_TIMER(_parent->metrics().serialize_timer);
        size_t ALIGNED_SIZE = 1;
        if (aligned) {
            ALIGNED_SIZE = AlignedBuffer::kPageSize;
        }
        if (UNLIKELY(config::pipeline_enable_large_column_checker)) {
            if (chunk->has_capacity_limit_reached()) {
                return Status::CapacityLimitExceed(fmt::format("Large column detected in spill serialize phase "));
            }
        }
        const auto& columns = chunk->columns();
        const size_t ncol = columns.size();
        auto* registry = CodecRegistry::instance();

        char header_buffer[HEADER_SIZE];
        UNALIGNED_STORE32(header_buffer + SEQUENCE_OFFSET, SEQUENCE_MAGIC_ID_V2);
        const size_t desc_bytes = ncol * sizeof(uint32_t);
        // trailing pad so streamvbyte-family decoders may safely overread the last payload
        const size_t decode_pad = serde::EncodeContext::STREAMVBYTE_PADDING_SIZE;

        const bool sampling = _codec_selector->begin_chunk();
        std::vector<CodecCandidate> chosen(ncol);
        size_t content_length = 0;

        if (sampling) {
            // Trial-encode candidates to feed the selector. Trials run on a PREFIX SLICE
            // for big columns (estimates scaled by the row ratio), so a sampled chunk
            // costs little more than a regular one; only the per-chunk winner is encoded
            // in full for the output payload.
            constexpr size_t kTrialSliceBytes = 256 * 1024;
            std::vector<std::string> payloads(ncol);
            std::string scratch;
            for (size_t i = 0; i < ncol; ++i) {
                auto cands = registry->candidates(*columns[i], _codec_selector->session_encode_level());

                const Column* probe = columns[i].get();
                MutableColumnPtr probe_holder;
                size_t rows = columns[i]->size();
                size_t probe_rows = rows;
                size_t col_bytes = columns[i]->byte_size();
                if (col_bytes > kTrialSliceBytes && rows >= 64) {
                    probe_rows = std::max<size_t>(64, rows * kTrialSliceBytes / col_bytes);
                    probe_holder = columns[i]->clone_empty();
                    probe_holder->append(*columns[i], 0, probe_rows);
                    probe = probe_holder.get();
                }

                uint64_t best_score = std::numeric_limits<uint64_t>::max();
                bool found = false;
                for (const auto& cand : cands) {
                    if (!_codec_selector->should_try(i, cand)) continue; // pruned loser
                    const auto* codec = registry->get(cand.id);
                    int64_t cap = codec->max_encoded_size(*probe, cand.param);
                    if (cap <= 0) continue;
                    // per-column context (e.g. FSST symbol table) is built once per window,
                    // OUTSIDE the timed section: its cost amortizes over the locked phase
                    std::shared_ptr<CodecContext> codec_ctx = _codec_selector->trial_context(i, cand);
                    if (codec_ctx == nullptr) {
                        ASSIGN_OR_RETURN(codec_ctx, codec->create_context(*probe));
                        if (codec_ctx != nullptr) {
                            _codec_selector->put_trial_context(i, cand, codec_ctx);
                        }
                    }
                    scratch.resize(cap + serde::EncodeContext::STREAMVBYTE_PADDING_SIZE);
                    auto* base = reinterpret_cast<uint8_t*>(scratch.data());
                    int64_t t0 = MonotonicNanos();
                    ASSIGN_OR_RETURN(auto* cur, codec->encode(*probe, base, cand.param, codec_ctx.get()));
                    auto n = static_cast<uint64_t>(cur - base);
                    // trial-decode too: restore CPU is part of a codec's true cost (e.g.
                    // front-coding decodes serially; ignoring it picks decode-heavy codecs)
                    {
                        MutableColumnPtr target = probe->clone_empty();
                        auto d = codec->decode(base, base + scratch.size(), target.get());
                        if (!d.ok()) {
                            return d.status();
                        }
                    }
                    auto codec_ns = static_cast<uint64_t>(MonotonicNanos() - t0);
                    // scale prefix estimates back to full-chunk magnitude
                    if (probe_rows != rows) {
                        n = n * rows / probe_rows;
                        codec_ns = codec_ns * rows / probe_rows;
                    }
                    _codec_selector->record_sample(i, cand, n, codec_ns);
                    if (n < best_score) {
                        best_score = n;
                        chosen[i] = cand;
                        found = true;
                    }
                }
                if (!found) {
                    return Status::InternalError("no applicable spill codec for column");
                }
                // encode the winner on the FULL column for the output payload
                const auto* codec = registry->get(chosen[i].id);
                int64_t cap = codec->max_encoded_size(*columns[i], chosen[i].param);
                if (cap <= 0) {
                    chosen[i] = {CodecId::RAW, 0};
                    codec = registry->get(CodecId::RAW);
                    cap = codec->max_encoded_size(*columns[i], 0);
                    if (cap <= 0) {
                        return Status::InternalError("no applicable spill codec for column");
                    }
                }
                scratch.resize(cap);
                auto* base = reinterpret_cast<uint8_t*>(scratch.data());
                auto win_ctx = _codec_selector->trial_context(i, chosen[i]);
                ASSIGN_OR_RETURN(auto* cur, codec->encode(*columns[i], base, chosen[i].param, win_ctx.get()));
                payloads[i].assign(scratch.data(), static_cast<size_t>(cur - base));
            }
            _codec_selector->finalize_sampling();

            size_t total = HEADER_SIZE + desc_bytes;
            for (const auto& p : payloads) total += p.size();
            serialize_buffer.resize(ALIGN_UP(total + decode_pad, ALIGNED_SIZE));
            auto* buf = reinterpret_cast<uint8_t*>(serialize_buffer.data());
            const uint8_t* head = buf;
            buf = _write_v2_prologue(buf, chosen);
            for (size_t i = 0; i < ncol; ++i) {
                memcpy(buf, payloads[i].data(), payloads[i].size());
                buf += payloads[i].size();
            }
            content_length = buf - head;
        } else {
            size_t max_total = HEADER_SIZE + desc_bytes;
            for (size_t i = 0; i < ncol; ++i) {
                chosen[i] = _codec_selector->chosen(i);
                const auto* codec = registry->get(chosen[i].id);
                int64_t cap = codec->max_encoded_size(*columns[i], chosen[i].param);
                if (cap <= 0) {
                    // defensive demotion: the chosen codec cannot handle this chunk
                    // (e.g. row count out of range) -- fall back to RAW
                    chosen[i] = {CodecId::RAW, 0};
                    cap = registry->get(CodecId::RAW)->max_encoded_size(*columns[i], 0);
                    if (cap <= 0) {
                        return Status::InternalError("no applicable spill codec for column");
                    }
                }
                max_total += cap;
            }
            serialize_buffer.resize(ALIGN_UP(max_total + decode_pad, ALIGNED_SIZE));
            auto* buf = reinterpret_cast<uint8_t*>(serialize_buffer.data());
            const uint8_t* head = buf;
            buf = _write_v2_prologue(buf, chosen);
            for (size_t i = 0; i < ncol; ++i) {
                const auto* codec = registry->get(chosen[i].id);
                auto codec_ctx = _codec_selector->chosen_context(i);
                const uint8_t* col_start = buf;
                ASSIGN_OR_RETURN(buf, codec->encode(*columns[i], buf, chosen[i].param, codec_ctx.get()));
                // Close the loop on context reuse: trials always score a freshly built context, so
                // report what the LOCKED codec actually achieved -- the selector re-samples early
                // once a reused symbol table / (e,f) pair drifts out of tune (kRatioDriftFactor).
                _codec_selector->report_locked_ratio(i, static_cast<uint64_t>(buf - col_start),
                                                     columns[i]->byte_size());
            }
            content_length = buf - head;
        }

        auto align_size = ALIGN_UP(content_length + decode_pad, ALIGNED_SIZE);
        serialize_buffer.resize(align_size);
        UNALIGNED_STORE64(header_buffer + ATTACHMENT_SIZE_OFFSET, align_size - HEADER_SIZE);
        memcpy(serialize_buffer.data(), header_buffer, HEADER_SIZE);
    }
    size_t written_bytes = serialize_buffer.size();
    RETURN_IF_ERROR(
            output->append(state, {Slice(serialize_buffer.data(), written_bytes)}, written_bytes, chunk->num_rows()));
    return Status::OK();
}

// v2 deserialize: ctx.serialize_buffer already holds the attachment (descs + payloads).
StatusOr<ChunkUniquePtr> ColumnarSerde::deserialize_v2(SerdeContext& ctx, size_t attachment_size) {
    auto chunk = _chunk_builder();
    auto& columns = chunk->columns();
    auto* registry = CodecRegistry::instance();

    auto& serialize_buffer = ctx.serialize_buffer;
    const auto* buf = reinterpret_cast<const uint8_t*>(serialize_buffer.data());
    const auto* end = buf + attachment_size;
    if (attachment_size < columns.size() * sizeof(uint32_t)) {
        return Status::Corruption("spill v2 attachment shorter than codec descriptors");
    }

    SCOPED_TIMER(_parent->metrics().deserialize_timer);
    const uint8_t* read_cursor = buf + columns.size() * sizeof(uint32_t);
    for (size_t i = 0; i < columns.size(); i++) {
        uint32_t desc = UNALIGNED_LOAD32(buf + i * sizeof(uint32_t));
        auto codec_id = static_cast<CodecId>(desc >> 16);
        const auto* codec = registry->get(codec_id);
        if (codec == nullptr) {
            return Status::Corruption(fmt::format("unknown spill codec id {}", desc >> 16));
        }
        ASSIGN_OR_RETURN(read_cursor, codec->decode(read_cursor, end, columns[i]->as_mutable_raw_ptr()));
    }

    TRACE_SPILL_LOG << "deserialize v2 chunk, attachment size: " << attachment_size
                    << ", original size: " << chunk->bytes_usage();
    return chunk;
}

StatusOr<SerdePtr> Serde::create_serde(Spiller* parent) {
    return std::make_shared<ColumnarSerde>(parent, parent->chunk_builder());
}
} // namespace starrocks::spill
