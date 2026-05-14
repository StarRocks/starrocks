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

#include "column/adaptive_nullable_column.h"
#include "column/chunk.h"
#include "exec/file_scanner/file_scanner.h"
#include "formats/avro/cpp/avro_schema_builder.h"
#include "formats/avro/cpp/utils.h"
#include "fs/fs.h"
#include "runtime/descriptors.h"
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
}

Status AvroReader::init(std::unique_ptr<avro::InputStream> input_stream, const std::string& filename,
                        RuntimeState* state, ScannerCounter* counter, const std::vector<SlotDescriptor*>* slot_descs,
                        const std::vector<avrocpp::ColumnReaderUniquePtr>* column_readers, bool col_not_found_as_null,
                        RandomAccessFile* raw_file, size_t buffer_size, int64_t split_offset, int64_t split_length) {
    if (_is_inited) {
        return Status::OK();
    }

    _filename = filename;
    _slot_descs = slot_descs;
    _column_readers = column_readers;
    _num_of_columns_from_file = _column_readers->size();
    _col_not_found_as_null = col_not_found_as_null;

    _state = state;
    _counter = counter;

    try {
        // Read the Avro file header once via DataFileReaderBase (schema + sync marker).
        // init() is intentionally NOT called yet so we can inspect the writer schema
        // and apply column projection before setting up the decoder.
        auto base = std::make_unique<avro::DataFileReaderBase>(std::move(input_stream));
        const auto& writer_schema = base->dataSchema();

        // Collect the column names that actually need to be decoded.
        std::set<std::string> needed_cols;
        if (slot_descs != nullptr) {
            for (const auto* slot : *slot_descs) {
                if (slot != nullptr) {
                    needed_cols.insert(std::string(slot->col_name()));
                }
            }
        }

        // Build a projected reader schema that contains only the needed columns.
        // The ResolvingDecoder will then skip unwanted fields at the binary level,
        // saving CPU deserialization work — the Trino-equivalent of maskColumnsFromTableSchema.
        avro::ValidSchema projected = build_projected_schema(writer_schema, needed_cols);
        bool using_projection = !needed_cols.empty() && projected.root()->leaves() < writer_schema.root()->leaves();

        if (using_projection) {
            _file_reader = std::make_unique<avro::DataFileReader<avro::GenericDatum>>(std::move(base), projected);
        } else {
            _file_reader = std::make_unique<avro::DataFileReader<avro::GenericDatum>>(std::move(base));
        }

        // The datum must match the schema the decoder will produce values for.
        const auto& effective_schema = using_projection ? _file_reader->readerSchema() : writer_schema;
        _datum = std::make_unique<avro::GenericDatum>(effective_schema);

        // Split handling.
        // DataFileReader::sync(pos) advances to the first sync marker at or after pos,
        // which is exactly the same strategy used by Hadoop's AvroInputFormat.
        // split_offset == 0 means "start of file" — leave the reader positioned after
        // the file header as opened above (no extra seek needed).
        if (split_offset > 0) {
            _file_reader->sync(split_offset);
        }
        // split_end > 0 enables the pastSync() check in read_chunk().
        _split_end = (split_length > 0) ? split_offset + split_length : 0;

        // When no columns are needed (count(*) path), count records by reading only
        // Avro block headers — no decompression. raw_file is used independently of
        // the stream owned by _file_reader; _file_reader is never read in this path.
        if (_num_of_columns_from_file == 0 && raw_file != nullptr) {
            try {
                _total_count = count_avro_blocks(raw_file, buffer_size, counter, split_offset, _split_end);
                _count_remaining = _total_count;
            } catch (const avro::Exception&) {
                // fall back to record-level reading if block counting fails
            }
        }

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
            if (_split_end > 0 && _file_reader->pastSync(_split_end)) {
                break;
            }

            if (!_file_reader->read(*_datum)) {
                break;
            }

            auto num_rows = chunk->num_rows();

            DCHECK(_datum->type() == avro::AVRO_RECORD);
            const auto& record = _datum->value<avro::GenericRecord>();

            auto st = read_row(record, column_raw_ptrs);
            if (st.is_data_quality_error()) {
                if (_counter->num_rows_filtered++ < MAX_ERROR_LINES_IN_FILE) {
                    std::string json_str;
                    (void)AvroUtils::datum_to_json(*_datum, &json_str);
                    RuntimeStateHelper::append_error_msg_to_file(_state, json_str, std::string(st.message()));
                    LOG(WARNING) << "Failed to read row. error: " << st;
                }
                // Rollback the partially-written row before processing the next record.
                chunk->set_num_rows(num_rows);
                continue;
            } else if (!st.ok()) {
                return st;
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

Status AvroReader::get_schema(std::vector<SlotDescriptor>* schema) {
    if (!_is_inited) {
        return Status::Uninitialized("Avro reader is not initialized");
    }

    try {
        const auto& avro_schema = _file_reader->dataSchema();
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
