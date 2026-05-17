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

#include "formats/avro/cpp/avro_reader.h"

#include <fmt/format.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <avrocpp/Compiler.hh>
#include <avrocpp/NodeImpl.hh>
#include <avrocpp/Types.hh>
#include <avrocpp/ValidSchema.hh>
#include <limits>

#include "column/adaptive_nullable_column.h"
#include "column/chunk.h"
#include "exec/file_scanner/file_scanner.h"
#include "formats/avro/cpp/avro_schema_builder.h"
#include "formats/avro/cpp/direct_column_reader.h"
#include "formats/avro/cpp/utils.h"
#include "fs/fs.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/runtime_state_helper.h"

namespace starrocks {

namespace {

// Build an Avro record schema that contains only the columns in `needed_cols`.
// Fields present in `writer_schema` but absent from `needed_cols` will be skipped
// by the ResolvingDecoder, saving CPU deserialization work for unused columns.
// Returns `writer_schema` unchanged if projection is not beneficial or if
// JSON round-trip fails (safe fallback).
avro::ValidSchema build_projected_schema(const avro::ValidSchema& writer_schema,
                                         const std::set<std::string>& needed_cols) {
    std::string json = writer_schema.toJson(false);

    rapidjson::Document doc;
    if (doc.Parse(json.c_str()).HasParseError()) {
        return writer_schema;
    }

    if (!doc.HasMember("fields") || !doc["fields"].IsArray()) {
        return writer_schema;
    }

    auto& alloc = doc.GetAllocator();
    rapidjson::Value projected(rapidjson::kArrayType);
    for (auto& field : doc["fields"].GetArray()) {
        if (field.HasMember("name") && field["name"].IsString() && needed_cols.count(field["name"].GetString())) {
            projected.PushBack(rapidjson::Value(field, alloc), alloc);
        }
    }

    if (static_cast<size_t>(projected.Size()) >= writer_schema.root()->leaves()) {
        return writer_schema;
    }

    doc["fields"] = std::move(projected);

    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
    doc.Accept(writer);

    try {
        return avro::compileJsonSchemaFromString(buf.GetString());
    } catch (const avro::Exception&) {
        return writer_schema;
    }
}

// ---- Avro block-level record counting ----
// Parses only block headers (object_count + byte_count), skips compressed
// data via seek. No decompression — O(number_of_blocks) instead of O(records).

static bool try_read_byte(AvroBufferInputStream& s, uint8_t& b) {
    const uint8_t* p;
    size_t len;
    if (!s.next(&p, &len)) return false;
    b = *p;
    if (len > 1) s.backup(len - 1);
    return true;
}

static bool try_read_avro_long(AvroBufferInputStream& s, int64_t& out) {
    constexpr int kMaxAvroLongBytes = 10;
    uint64_t n = 0;
    int shift = 0;
    int bytes_read = 0;
    uint8_t b;
    if (!try_read_byte(s, b)) return false;
    for (;;) {
        ++bytes_read;
        if (bytes_read > kMaxAvroLongBytes || shift >= 64) {
            throw avro::Exception("Avro long is too long");
        }
        if (shift == 63 && (b & 0x7F) > 1) {
            throw avro::Exception("Avro long overflow");
        }

        n |= static_cast<uint64_t>(b & 0x7F) << shift;
        if (!(b & 0x80)) break;

        shift += 7;
        if (!try_read_byte(s, b)) throw avro::Exception("truncated Avro long");
    }
    out = static_cast<int64_t>((n >> 1) ^ -(n & 1));
    return true;
}

static void read_exact(AvroBufferInputStream& s, uint8_t* buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        const uint8_t* p;
        size_t len;
        if (!s.next(&p, &len)) throw avro::Exception("unexpected EOF reading exact bytes");
        size_t take = std::min(len, n - done);
        memcpy(buf + done, p, take);
        if (take < len) s.backup(len - take);
        done += take;
    }
}

static void skip_avro_bytes(AvroBufferInputStream& s) {
    int64_t len;
    if (try_read_avro_long(s, len) && len > 0) s.skip(static_cast<size_t>(len));
}

static void skip_avro_metadata(AvroBufferInputStream& s) {
    int64_t block_count;
    while (try_read_avro_long(s, block_count) && block_count != 0) {
        int64_t n = block_count < 0 ? -block_count : block_count;
        if (block_count < 0) {
            int64_t ignored;
            try_read_avro_long(s, ignored);
        }
        for (int64_t i = 0; i < n; ++i) {
            skip_avro_bytes(s);
            skip_avro_bytes(s);
        }
    }
}

static int64_t count_avro_blocks(RandomAccessFile* file, size_t buffer_size, ScannerCounter* counter,
                                 int64_t split_offset, int64_t split_end) {
    // Always read the file header from byte 0 to get the sync marker.
    auto st = file->seek(0);
    if (!st.ok()) throw avro::Exception("seek failed: " + st.to_string());

    AvroBufferInputStream stream(file, buffer_size, counter);

    uint8_t magic[4];
    read_exact(stream, magic, 4);
    static const uint8_t kMagic[] = {'O', 'b', 'j', 1};
    if (memcmp(magic, kMagic, 4) != 0) throw avro::Exception("not a valid Avro file");

    skip_avro_metadata(stream);

    uint8_t sync[avro::SyncSize];
    read_exact(stream, sync, avro::SyncSize);

    // If split_offset > 0, reuse the same sync-scan logic as DataFileReaderBase::sync():
    // seek to split_offset then slide forward one byte at a time until we match the sync marker.
    if (split_offset > 0 && static_cast<int64_t>(stream.byteCount()) < split_offset) {
        stream.seek(split_offset);
        std::vector<uint8_t> window(avro::SyncSize, 0);
        size_t filled = 0;
        while (filled < static_cast<size_t>(avro::SyncSize)) {
            const uint8_t* p;
            size_t len;
            if (!stream.next(&p, &len)) return 0;
            size_t take = std::min(len, static_cast<size_t>(avro::SyncSize) - filled);
            memcpy(window.data() + filled, p, take);
            filled += take;
            if (take < len) stream.backup(len - take);
        }
        bool found = false;
        for (;;) {
            if (memcmp(window.data(), sync, avro::SyncSize) == 0) {
                found = true;
                break;
            }
            uint8_t b;
            if (!try_read_byte(stream, b)) break;
            memmove(window.data(), window.data() + 1, avro::SyncSize - 1);
            window[avro::SyncSize - 1] = b;
        }
        if (!found) return 0;
    }

    int64_t total = 0;
    int64_t obj_count;
    while (try_read_avro_long(stream, obj_count)) {
        int64_t byte_count;
        if (!try_read_avro_long(stream, byte_count)) throw avro::Exception("truncated block header");
        if (obj_count < 0 || byte_count < 0) throw avro::Exception("invalid Avro block header");

        // Stop if we have moved past the split end.  The boundary check uses the position
        // after reading both varints, which corresponds to DataFileReaderBase::previousSync()
        // semantics (the sync marker that introduced this block is at a position <= split_end).
        if (split_end > 0 && static_cast<int64_t>(stream.byteCount()) > split_end) {
            break;
        }

        stream.skip(static_cast<size_t>(byte_count));

        uint8_t block_sync[avro::SyncSize];
        read_exact(stream, block_sync, avro::SyncSize);
        if (memcmp(sync, block_sync, avro::SyncSize) != 0) throw avro::Exception("sync marker mismatch");

        total += obj_count;
    }
    return total;
}

// Build a FieldPlanEntry that skips one Avro field without materializing any column value.
//
// Avro binary wire formats for the types we handle:
//
//   NULL      — 0 bytes on wire.  decodeNull() is a no-op in BinaryDecoder.
//               Represented as SKIP_FIXED(0); consecutive nulls can be merged
//               away by the batching pass that follows.
//
//   BOOL      — 1 byte (0x00 = false, 0x01 = true).
//   FLOAT     — 4 bytes little-endian IEEE-754.
//   DOUBLE    — 8 bytes little-endian IEEE-754.
//   FIXED(n)  — exactly n bytes, no length prefix.
//               All four are truly fixed-width → SKIP_FIXED(n).
//               Adjacent SKIP_FIXED entries are merged by the caller into a
//               single skipFixed(total) call, saving decoder invocations.
//               Example: 4 consecutive skipped DOUBLEs → skipFixed(32).
//
//   INT/LONG  — zigzag-encoded varint, 1-10 bytes depending on value.
//   ENUM      — index encoded as a varint (same as LONG wire format).
//               Variable width, cannot be batched → SKIP_VARINT.
//               Uses decodeLong() which reads the full zigzag sequence.
//
//   STRING    — varint length prefix + raw UTF-8 bytes.
//   BYTES     — varint length prefix + raw bytes.
//               Variable width, cannot be batched → SKIP_STRING.
//               skipString() decodes only the length and advances the stream
//               pointer by that many bytes without copying the payload.
//
//   UNION     — varint branch index + the value for that branch.
//               The common case in Hive-generated Avro schemas is a nullable
//               union ["null", T] or [T, "null"].  We detect this at plan-build
//               time and specialise into SKIP_NULLABLE_{FIXED,VARINT,STRING}
//               based on the inner type T, avoiding virtual node->type() and
//               node->leafAt() calls during execution.
//               Non-nullable or multi-branch unions fall back to SKIP_NODE.
//
//   RECORD / ARRAY / MAP / complex UNION
//               Not handled inline.  SKIP_NODE stores the avro NodePtr and
//               delegates to DirectColumnReader::skip_node() which recurses
//               through the schema at execution time (accepts virtual dispatch
//               cost in exchange for correctness on arbitrary nested types).
//
// Caller note: after building entries for all fields, merge consecutive
// SKIP_FIXED entries to maximise the batching benefit.
static FieldPlanEntry make_skip_plan_entry(const avro::NodePtr& node) {
    FieldPlanEntry e;
    switch (node->type()) {
    // ── fixed-width non-nullable types ──────────────────────────────────────
    case avro::AVRO_NULL:
        // No bytes on wire; SKIP_FIXED(0) is a no-op that the merge pass
        // harmlessly folds into any adjacent fixed-width entry.
        e.kind = FieldPlanEntry::Kind::SKIP_FIXED;
        e.skip_bytes = 0;
        return e;
    case avro::AVRO_BOOL:
        e.kind = FieldPlanEntry::Kind::SKIP_FIXED;
        e.skip_bytes = 1;
        return e;
    case avro::AVRO_FLOAT:
        e.kind = FieldPlanEntry::Kind::SKIP_FIXED;
        e.skip_bytes = 4;
        return e;
    case avro::AVRO_DOUBLE:
        e.kind = FieldPlanEntry::Kind::SKIP_FIXED;
        e.skip_bytes = 8;
        return e;
    case avro::AVRO_FIXED:
        // Schema-defined size; no wire-level length prefix.
        e.kind = FieldPlanEntry::Kind::SKIP_FIXED;
        e.skip_bytes = static_cast<uint32_t>(node->fixedSize());
        return e;
    // ── variable-width non-nullable types ────────────────────────────────────
    case avro::AVRO_INT:
    case avro::AVRO_LONG:
    case avro::AVRO_ENUM:
        // Zigzag varint: 1-10 bytes depending on magnitude.  Must call
        // decodeLong() to consume the full encoding; cannot batch.
        e.kind = FieldPlanEntry::Kind::SKIP_VARINT;
        return e;
    case avro::AVRO_STRING:
    case avro::AVRO_BYTES:
        // Length-prefixed: varint(len) + len bytes.  skipString() reads the
        // varint and advances the stream pointer without copying the payload.
        e.kind = FieldPlanEntry::Kind::SKIP_STRING;
        return e;
    // ── union ────────────────────────────────────────────────────────────────
    case avro::AVRO_UNION:
        if (node->leaves() == 2) {
            // Attempt to classify as a simple nullable union ["null", T] or
            // [T, "null"] — the standard Hive nullable column encoding.
            // Wire format: varint(branch_index) + value_for_that_branch.
            // We pre-record null_branch so execution reduces to:
            //   branch = decodeUnionIndex();
            //   if (branch != null_branch) skip_inner_value();
            size_t null_branch = static_cast<size_t>(-1);
            avro::NodePtr inner;
            for (size_t i = 0; i < 2; ++i) {
                const auto& leaf = node->leafAt(i);
                if (leaf->type() == avro::AVRO_NULL) {
                    null_branch = i;
                } else {
                    inner = leaf;
                }
            }
            if (null_branch != static_cast<size_t>(-1) && inner != nullptr) {
                e.null_branch = static_cast<uint8_t>(null_branch);
                switch (inner->type()) {
                case avro::AVRO_BOOL:
                    e.kind = FieldPlanEntry::Kind::SKIP_NULLABLE_FIXED;
                    e.skip_bytes = 1;
                    return e;
                case avro::AVRO_FLOAT:
                    e.kind = FieldPlanEntry::Kind::SKIP_NULLABLE_FIXED;
                    e.skip_bytes = 4;
                    return e;
                case avro::AVRO_DOUBLE:
                    e.kind = FieldPlanEntry::Kind::SKIP_NULLABLE_FIXED;
                    e.skip_bytes = 8;
                    return e;
                case avro::AVRO_FIXED:
                    e.kind = FieldPlanEntry::Kind::SKIP_NULLABLE_FIXED;
                    e.skip_bytes = static_cast<uint32_t>(inner->fixedSize());
                    return e;
                case avro::AVRO_INT:
                case avro::AVRO_LONG:
                case avro::AVRO_ENUM:
                    e.kind = FieldPlanEntry::Kind::SKIP_NULLABLE_VARINT;
                    return e;
                case avro::AVRO_STRING:
                case avro::AVRO_BYTES:
                    e.kind = FieldPlanEntry::Kind::SKIP_NULLABLE_STRING;
                    return e;
                default:
                    // Nullable union with a complex inner type (record, array,
                    // map, nested union …).  Fall through to SKIP_NODE.
                    break;
                }
            }
            // Non-nullable two-branch union (both branches have values).
            // Fall through to SKIP_NODE.
        }
        // Multi-branch union (>2 branches) or unhandled inner type.
        [[fallthrough]];
    default:
        // RECORD, ARRAY, MAP, or any other complex type.  Store the NodePtr
        // and let skip_node() handle recursive traversal at execution time.
        e.kind = FieldPlanEntry::Kind::SKIP_NODE;
        e.node = node;
        return e;
    }
}

// Execute the skip action for one plan entry.
//
// Called in two contexts:
//   1. Normal path: entry.kind != READ — skip an unneeded field.
//   2. Error-recovery path: a read_field() failed; drain remaining entries
//      to keep the BinaryDecoder byte-aligned for the next record.
//
// Why each case is correct:
//   SKIP_FIXED          — skip_bytes may cover multiple merged fields (e.g.
//                         4 × DOUBLE → skipFixed(32)).  skip_bytes == 0 is a
//                         no-op (from a NULL field folded by the merge pass).
//   SKIP_VARINT         — decodeLong() consumes exactly one zigzag varint.
//                         INT/ENUM share the same wire encoding as LONG.
//   SKIP_STRING         — skipString() reads the varint length then advances
//                         the stream pointer without copying the payload.
//                         BYTES shares the same wire format.
//   SKIP_NULLABLE_*     — first consumes the union branch varint, then
//                         conditionally skips the value.  The null branch
//                         carries no payload (decodeNull() is a no-op), so
//                         we simply do nothing when branch == null_branch.
//   SKIP_NODE / READ    — delegate to skip_node() for complex types or for
//                         READ entries in the error-recovery path.  Both
//                         always have entry.node set.
static inline void skip_field_plan_entry(avro::Decoder& decoder, const FieldPlanEntry& entry) {
    switch (entry.kind) {
    case FieldPlanEntry::Kind::SKIP_FIXED:
        // May cover multiple merged fields; skip_bytes == 0 is a no-op.
        if (entry.skip_bytes > 0) decoder.skipFixed(entry.skip_bytes);
        break;
    case FieldPlanEntry::Kind::SKIP_VARINT:
        (void)decoder.decodeLong();
        break;
    case FieldPlanEntry::Kind::SKIP_STRING:
        decoder.skipString();
        break;
    case FieldPlanEntry::Kind::SKIP_NULLABLE_FIXED: {
        size_t branch = decoder.decodeUnionIndex();
        if (branch != entry.null_branch) decoder.skipFixed(entry.skip_bytes);
        break;
    }
    case FieldPlanEntry::Kind::SKIP_NULLABLE_VARINT: {
        size_t branch = decoder.decodeUnionIndex();
        if (branch != entry.null_branch) (void)decoder.decodeLong();
        break;
    }
    case FieldPlanEntry::Kind::SKIP_NULLABLE_STRING: {
        size_t branch = decoder.decodeUnionIndex();
        if (branch != entry.null_branch) decoder.skipString();
        break;
    }
    case FieldPlanEntry::Kind::SKIP_NODE:
    case FieldPlanEntry::Kind::READ:
        // READ entries reach here only in the error-recovery path; entry.node
        // is always valid for both SKIP_NODE and READ.
        avrocpp::DirectColumnReader::skip_node(decoder, entry.node);
        break;
    }
}

} // namespace

bool AvroBufferInputStream::next(const uint8_t** data, size_t* len) {
    if (_available == 0 && !fill()) {
        return false;
    }

    *data = _next;
    *len = _available;
    _next += _available;
    _byte_count += _available;
    _available = 0;
    return true;
}

void AvroBufferInputStream::backup(size_t len) {
    _next -= len;
    _available += len;
    _byte_count -= len;
}

void AvroBufferInputStream::skip(size_t len) {
    while (len > 0) {
        if (_available == 0) {
            auto st = _file->seek(_byte_count + len);
            if (!st.ok()) {
                throw avro::Exception(fmt::format("Avro input stream skip failed. error: {}", st.to_string()));
            }

            _byte_count += len;
            return;
        }

        size_t n = std::min(_available, len);
        _available -= n;
        _next += n;
        len -= n;
        _byte_count += n;
    }
}

void AvroBufferInputStream::seek(int64_t position) {
    auto st = _file->seek(position);
    if (!st.ok()) {
        throw avro::Exception(fmt::format("Avro input stream seek failed. error: {}", st.to_string()));
    }

    _byte_count = position;
    _available = 0;
}

bool AvroBufferInputStream::fill() {
    ++_counter->file_read_count;
    SCOPED_RAW_TIMER(&_counter->file_read_ns);
    auto ret = _file->read(_buffer, _buffer_size);
    if (!ret.ok() || ret.value() == 0) {
        return false;
    }

    _next = _buffer;
    _available = ret.value();
    return true;
}

AvroReader::~AvroReader() {
    if (_datum != nullptr) {
        _datum.reset();
    }
    if (_file_reader != nullptr) {
        _file_reader->close();
        _file_reader.reset();
    }
    if (_base_reader != nullptr) {
        _base_reader->close();
        _base_reader.reset();
    }
}

Status AvroReader::init(std::unique_ptr<avro::InputStream> input_stream, const std::string& filename,
                        RuntimeState* state, ScannerCounter* counter, const std::vector<SlotDescriptor*>* slot_descs,
                        const std::vector<avrocpp::ColumnReaderUniquePtr>* column_readers, bool col_not_found_as_null,
                        RandomAccessFile* raw_file, size_t buffer_size, int64_t split_offset, int64_t split_length,
                        const std::string& reader_schema_json, bool invalid_as_null, bool allow_direct_path) {
    if (_is_inited) {
        return Status::OK();
    }

    _stats = {};
    _filename = filename;
    _slot_descs = slot_descs;
    _column_readers = column_readers;
    _num_of_columns_from_file = _column_readers->size();
    _col_not_found_as_null = col_not_found_as_null;

    _state = state;
    _counter = counter;

    try {
        // Count queries materialize no physical Avro columns.  Keep this path
        // ahead of reader-schema setup so FE-provided schemas do not force
        // row-reader construction or resolving-decoder initialization.
        _split_end = (split_length > 0) ? split_offset + split_length : 0;
        if (_num_of_columns_from_file == 0 && raw_file != nullptr) {
            try {
                _total_count = count_avro_blocks(raw_file, buffer_size, counter, split_offset, _split_end);
                _count_remaining = _total_count;
                _is_inited = true;
                return Status::OK();
            } catch (const avro::Exception&) {
                (void)raw_file->seek(0);
                // fall back to record-level reading if block counting fails
            }
        }

        // Read the Avro file header once via DataFileReaderBase (schema + sync marker).
        // init() is intentionally NOT called yet so we can inspect the writer schema
        // and apply column projection before setting up the decoder.
        auto base = std::make_unique<avro::DataFileReaderBase>(std::move(input_stream));
        const auto& writer_schema = base->dataSchema();
        avro::ValidSchema reader_schema =
                reader_schema_json.empty() ? writer_schema : avro::compileJsonSchemaFromString(reader_schema_json);

        // Collect the column names that actually need to be decoded.
        std::set<std::string> needed_cols;
        if (slot_descs != nullptr) {
            for (const auto* slot : *slot_descs) {
                if (slot != nullptr) {
                    needed_cols.insert(std::string(slot->col_name()));
                }
            }
        }

        _data_schema = writer_schema;
        // Direct path: reads binary fields straight from the BinaryDecoder without materializing
        // a GenericDatum, which avoids heap allocation and ResolvingDecoder overhead per row.
        // Consequence: _datum stays nullptr throughout — error reporting cannot serialize the row
        // to JSON.  Callers that need row context in error messages (e.g. LOAD / AvroCppScanner)
        // must pass allow_direct_path=false to fall back to the GenericDatum path.
        _use_direct_path = allow_direct_path && reader_schema_json.empty() && !invalid_as_null &&
                           try_init_direct_readers(writer_schema);
        if (_use_direct_path) {
            _stats.direct_path_used = 1;
            _base_reader = std::move(base);
            _base_reader->init();
        } else {
            // Build a projected reader schema that contains only the needed columns.
            // The ResolvingDecoder will then skip unwanted fields at the binary level,
            // saving CPU deserialization work — the Trino-equivalent of maskColumnsFromTableSchema.
            avro::ValidSchema projected = build_projected_schema(reader_schema, needed_cols);
            bool using_projection = !needed_cols.empty() && projected.root()->leaves() < reader_schema.root()->leaves();

            if (using_projection) {
                _file_reader = std::make_unique<avro::DataFileReader<avro::GenericDatum>>(std::move(base), projected);
            } else if (!reader_schema_json.empty()) {
                _file_reader =
                        std::make_unique<avro::DataFileReader<avro::GenericDatum>>(std::move(base), reader_schema);
            } else {
                _file_reader = std::make_unique<avro::DataFileReader<avro::GenericDatum>>(std::move(base));
            }

            // The datum must match the schema the decoder will produce values for.
            const auto& effective_schema =
                    (using_projection || !reader_schema_json.empty()) ? _file_reader->readerSchema() : writer_schema;
            _datum = std::make_unique<avro::GenericDatum>(effective_schema);

            // Map each slot to its field index in the effective (possibly projected) schema.
            _field_indexes.resize(_num_of_columns_from_file, -1);
            for (size_t i = 0; i < _num_of_columns_from_file; ++i) {
                const auto& desc = (*_slot_descs)[i];
                if (desc == nullptr) {
                    continue;
                }
                size_t index = 0;
                if (effective_schema.root()->nameIndex(std::string(desc->col_name()), index)) {
                    _field_indexes[i] = static_cast<int64_t>(index);
                }
            }
        }

        // Split handling.
        // DataFileReader::sync(pos) advances to the first sync marker at or after pos,
        // which is exactly the same strategy used by Hadoop's AvroInputFormat.
        // split_offset == 0 means "start of file" — leave the reader positioned after
        // the file header as opened above (no extra seek needed).
        if (split_offset > 0) {
            if (_use_direct_path) {
                _base_reader->sync(split_offset);
            } else {
                _file_reader->sync(split_offset);
            }
        }
        // split_end > 0 enables the pastSync() check in read_chunk().
        _split_end = (split_length > 0) ? split_offset + split_length : 0;
    } catch (const avro::Exception& ex) {
        auto err_msg = fmt::format("Avro reader init throws exception: {}", ex.what());
        LOG(WARNING) << err_msg;
        return Status::InternalError(err_msg);
    }

    _is_inited = true;
    return Status::OK();
}

void AvroReader::TEST_init(const std::vector<SlotDescriptor*>* slot_descs,
                           const std::vector<avrocpp::ColumnReaderUniquePtr>* column_readers,
                           bool col_not_found_as_null) {
    _slot_descs = slot_descs;
    _column_readers = column_readers;
    _num_of_columns_from_file = _column_readers->size();
    _col_not_found_as_null = col_not_found_as_null;

    const auto& schema = _file_reader->dataSchema();
    _datum = std::make_unique<avro::GenericDatum>(schema);

    _field_indexes.resize(_num_of_columns_from_file, -1);
    for (size_t i = 0; i < _num_of_columns_from_file; ++i) {
        const auto& desc = (*_slot_descs)[i];
        if (desc == nullptr) {
            continue;
        }

        size_t index = 0;
        if (schema.root()->nameIndex(std::string(desc->col_name()), index)) {
            _field_indexes[i] = index;
        }
    }

    _is_inited = true;
}

Status AvroReader::read_chunk(ChunkPtr& chunk, int rows_to_read, int64_t* rows_counted_out) {
    if (!_is_inited) {
        return Status::Uninitialized("Avro reader is not initialized");
    }

    // get column raw ptrs before reading chunk
    std::vector<AdaptiveNullableColumn*> column_raw_ptrs;
    column_raw_ptrs.resize(_num_of_columns_from_file, nullptr);
    for (size_t i = 0; i < _num_of_columns_from_file; ++i) {
        const auto& desc = (*_slot_descs)[i];
        if (desc == nullptr) {
            continue;
        }

        column_raw_ptrs[i] = down_cast<AdaptiveNullableColumn*>(chunk->get_column_raw_ptr_by_slot_id(desc->id()));
    }

    // Fast path: block-level precomputed count (no file reads at all).
    if (_total_count >= 0) {
        int64_t batch = std::min(static_cast<int64_t>(rows_to_read), _count_remaining);
        if (rows_counted_out != nullptr) *rows_counted_out = batch;
        _stats.block_count_rows += batch;
        _count_remaining -= batch;
        return batch > 0 ? Status::OK() : Status::EndOfFile("No more data to read");
    }

    try {
        int64_t row_count = 0;
        while (rows_to_read > 0) {
            // Check split boundary BEFORE reading the next record.
            // pastSync(pos) returns true once previousSync() — the sync marker that
            // introduced the current block — is past pos.  Checking before read()
            // mirrors Hadoop AvroInputFormat: if the block we are about to read from
            // started after split_end, stop now rather than reading one extra record.
            if (!_use_direct_path && _split_end > 0 && _file_reader->pastSync(_split_end)) {
                break;
            }

            auto num_rows = chunk->num_rows();

            Status st;
            if (_use_direct_path) {
                if (_split_end > 0 && _base_reader->pastSync(_split_end)) {
                    break;
                }
                if (!_base_reader->hasMore()) {
                    break;
                }
                // hasMore() + decr() + decoder() replicates what DataFileReaderBase::read()
                // does internally: hasMore() loads the next block when the current one is
                // exhausted and returns false at EOF; decr() decrements the per-block item
                // counter that hasMore() uses; decoder() returns the live BinaryDecoder
                // positioned at the current record.  We bypass read() so we can decode
                // directly into columns instead of materializing a GenericDatum.
                _base_reader->decr();
                st = read_direct_row(_base_reader->decoder(), column_raw_ptrs);
            } else {
                if (!_file_reader->read(*_datum)) {
                    break;
                }

                DCHECK(_datum->type() == avro::AVRO_RECORD);
                const auto& record = _datum->value<avro::GenericRecord>();
                st = read_row(record, column_raw_ptrs);
            }
            if (st.is_data_quality_error()) {
                if (_counter->num_rows_filtered++ < MAX_ERROR_LINES_IN_FILE) {
                    std::string json_str;
                    if (_datum != nullptr) {
                        (void)AvroUtils::datum_to_json(*_datum, &json_str);
                    } else {
                        // Direct path: no GenericDatum is allocated, so row context is unavailable.
                        json_str = "(row context unavailable in direct decode mode)";
                    }
                    RuntimeStateHelper::append_error_msg_to_file(_state, json_str, std::string(st.message()));
                    LOG(WARNING) << "Failed to read row. error: " << st;
                }
                // Rollback the partially-written row before processing the next record.
                chunk->set_num_rows(num_rows);
                continue;
            } else if (!st.ok()) {
                return st;
            }
            ++_stats.rows_decoded;
            if (_use_direct_path) {
                ++_stats.direct_rows_decoded;
            } else {
                ++_stats.generic_rows_decoded;
            }
            ++row_count;
            --rows_to_read;
        }

        if (rows_counted_out != nullptr) *rows_counted_out = row_count;
        return row_count > 0 ? Status::OK() : Status::EndOfFile("No more data to read");
    } catch (const avro::Exception& ex) {
        auto err_msg = fmt::format("Avro reader read chunk throws exception: {}", ex.what());
        LOG(WARNING) << err_msg;
        return Status::InternalError(err_msg);
    }
}

Status AvroReader::read_row(const avro::GenericRecord& record,
                            const std::vector<AdaptiveNullableColumn*>& column_raw_ptrs) {
    for (size_t i = 0; i < _num_of_columns_from_file; ++i) {
        const auto& desc = (*_slot_descs)[i];
        if (desc == nullptr) {
            continue;
        }

        DCHECK(column_raw_ptrs[i] != nullptr);

        if (_field_indexes[i] >= 0) {
            DCHECK((*_column_readers)[i] != nullptr);
            const auto& field = record.fieldAt(_field_indexes[i]);
            RETURN_IF_ERROR((*_column_readers)[i]->read_datum_for_adaptive_column(field, column_raw_ptrs[i]));
        } else if (!_col_not_found_as_null) {
            return Status::NotFound(
                    fmt::format("Column: {} is not found in file: {}. Consider setting "
                                "'fill_mismatch_column_with' = 'null' property",
                                desc->col_name(), _filename));
        } else {
            column_raw_ptrs[i]->append_nulls(1);
        }
    }
    return Status::OK();
}

bool AvroReader::try_init_direct_readers(const avro::ValidSchema& writer_schema) {
    const auto& root = writer_schema.root();
    if (root->type() != avro::AVRO_RECORD) {
        return false;
    }

    std::vector<int64_t> field_to_slot(root->leaves(), -1);
    _direct_column_readers.clear();
    _direct_column_readers.resize(_num_of_columns_from_file);

    if (_slot_descs == nullptr) {
        return false;
    }

    for (size_t i = 0; i < _num_of_columns_from_file; ++i) {
        const auto* desc = (*_slot_descs)[i];
        if (desc == nullptr) {
            continue;
        }

        size_t field_index = 0;
        if (!root->nameIndex(std::string(desc->col_name()), field_index)) {
            if (_col_not_found_as_null) {
                continue;
            }
            return false;
        }

        const auto& timezone = _state != nullptr ? _state->timezone_obj() : cctz::local_time_zone();
        auto reader =
                avrocpp::DirectColumnReader::make(desc->col_name(), desc->type(), root->leafAt(field_index), timezone);
        if (reader == nullptr) {
            return false;
        }

        field_to_slot[field_index] = static_cast<int64_t>(i);
        _direct_column_readers[i] = std::move(reader);
    }

    // Build the per-field dispatch plan: one entry per non-null schema field.
    // Skip entries pre-resolve the avro type so read_direct_row avoids virtual
    // dispatch (root->leafAt / node->type) on every row.
    _direct_field_plan.clear();
    _direct_field_plan.reserve(root->leaves());

    for (size_t fi = 0; fi < root->leaves(); ++fi) {
        const auto& field_node = root->leafAt(fi);
        int64_t slot = field_to_slot[fi];

        if (slot < 0) {
            if (field_node->type() == avro::AVRO_NULL) {
                continue; // zero bytes on wire; nothing to do
            }
            _direct_field_plan.push_back(make_skip_plan_entry(field_node));
        } else {
            FieldPlanEntry e;
            e.kind = FieldPlanEntry::Kind::READ;
            e.slot_index = static_cast<int32_t>(slot);
            e.node = field_node; // retained only for the error-recovery skip path
            _direct_field_plan.push_back(std::move(e));
        }
    }

    // Merge consecutive SKIP_FIXED entries into a single skipFixed(N) call to
    // reduce decoder invocations when multiple fixed-width fields are skipped.
    {
        std::vector<FieldPlanEntry> merged;
        merged.reserve(_direct_field_plan.size());
        for (auto& entry : _direct_field_plan) {
            if (!merged.empty() && merged.back().kind == FieldPlanEntry::Kind::SKIP_FIXED &&
                entry.kind == FieldPlanEntry::Kind::SKIP_FIXED &&
                static_cast<uint64_t>(merged.back().skip_bytes) + entry.skip_bytes <=
                        std::numeric_limits<uint32_t>::max()) {
                merged.back().skip_bytes += entry.skip_bytes;
            } else {
                merged.push_back(std::move(entry));
            }
        }
        _direct_field_plan = std::move(merged);
    }

    _stats.direct_plan_entries = _direct_field_plan.size();
    _stats.direct_read_entries = 0;
    _stats.direct_skip_entries = 0;
    _stats.direct_fast_skip_entries = 0;
    _stats.direct_fallback_skip_entries = 0;
    for (const auto& entry : _direct_field_plan) {
        if (entry.kind == FieldPlanEntry::Kind::READ) {
            ++_stats.direct_read_entries;
        } else {
            ++_stats.direct_skip_entries;
            if (entry.kind == FieldPlanEntry::Kind::SKIP_NODE) {
                ++_stats.direct_fallback_skip_entries;
            } else {
                ++_stats.direct_fast_skip_entries;
            }
        }
    }

    return true;
}

Status AvroReader::read_direct_row(avro::Decoder& decoder,
                                   const std::vector<AdaptiveNullableColumn*>& column_raw_ptrs) {
    const size_t n = _direct_field_plan.size();
    for (size_t i = 0; i < n; ++i) {
        const auto& entry = _direct_field_plan[i];
        if (entry.kind != FieldPlanEntry::Kind::READ) {
            skip_field_plan_entry(decoder, entry);
            continue;
        }

        DCHECK_GE(entry.slot_index, 0);
        DCHECK_LT(entry.slot_index, static_cast<int32_t>(_direct_column_readers.size()));
        DCHECK(_direct_column_readers[entry.slot_index] != nullptr);
        DCHECK(column_raw_ptrs[entry.slot_index] != nullptr);
        auto st = _direct_column_readers[entry.slot_index]->read_field(decoder, column_raw_ptrs[entry.slot_index]);
        if (!st.ok()) {
            // read_field fully consumed its avro field; drain the rest to keep the decoder in sync.
            for (size_t j = i + 1; j < n; ++j) {
                skip_field_plan_entry(decoder, _direct_field_plan[j]);
            }
            return st;
        }
    }

    for (size_t i = 0; i < _num_of_columns_from_file; ++i) {
        if ((*_slot_descs)[i] != nullptr && _direct_column_readers[i] == nullptr) {
            DCHECK(_col_not_found_as_null);
            column_raw_ptrs[i]->append_nulls(1);
        }
    }
    return Status::OK();
}

Status AvroReader::get_schema(std::vector<SlotDescriptor>* schema) {
    if (!_is_inited) {
        return Status::Uninitialized("Avro reader is not initialized");
    }

    try {
        const auto& avro_schema = _use_direct_path ? _data_schema : _file_reader->dataSchema();
        VLOG(2) << "avro data schema: " << avro_schema.toJson(false);

        const auto& node = avro_schema.root();
        if (node->type() != avro::AVRO_RECORD) {
            return Status::NotSupported(fmt::format("Root node is not record. type: {}", avro::toString(node->type())));
        }

        for (size_t i = 0; i < node->leaves(); ++i) {
            auto field_name = node->nameAt(i);
            auto field_node = node->leafAt(i);
            TypeDescriptor desc;
            RETURN_IF_ERROR(get_avro_type(field_node, &desc));
            schema->emplace_back(i, field_name, desc);
        }
        return Status::OK();
    } catch (const avro::Exception& ex) {
        auto err_msg = fmt::format("Avro reader get schema throws exception: {}", ex.what());
        LOG(WARNING) << err_msg;
        return Status::InternalError(err_msg);
    }
}

} // namespace starrocks
