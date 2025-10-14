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

#include <avrocpp/NodeImpl.hh>
#include <avrocpp/Types.hh>
#include <avrocpp/ValidSchema.hh>

#include "column/adaptive_nullable_column.h"
#include "exec/file_scanner/file_scanner.h"
#include "formats/avro/cpp/avro_schema_builder.h"
#include "formats/avro/cpp/utils.h"
#include "fs/fs.h"
#include "runtime/runtime_state.h"

namespace starrocks {

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
                        const std::vector<avrocpp::ColumnReaderUniquePtr>* column_readers, bool col_not_found_as_null) {
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
        _file_reader = std::make_unique<avro::DataFileReader<avro::GenericDatum>>(std::move(input_stream));

        const auto& schema = _file_reader->dataSchema();
        _datum = std::make_unique<avro::GenericDatum>(schema);

        _field_indexes.resize(_num_of_columns_from_file, -1);
        for (size_t i = 0; i < _num_of_columns_from_file; ++i) {
            const auto& desc = (*_slot_descs)[i];
            if (desc == nullptr) {
                continue;
            }

            size_t index = 0;
            if (schema.root()->nameIndex(desc->col_name(), index)) {
                _field_indexes[i] = index;
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
        if (schema.root()->nameIndex(desc->col_name(), index)) {
            _field_indexes[i] = index;
        }
    }

    _is_inited = true;
}

Status AvroReader::read_chunk(ChunkPtr& chunk, int rows_to_read) {
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

        column_raw_ptrs[i] = down_cast<AdaptiveNullableColumn*>(chunk->get_mutable_column_by_slot_id(desc->id()).get());
    }

    try {
        while (rows_to_read > 0 && _file_reader->read(*_datum)) {
            auto num_rows = chunk->num_rows();

            DCHECK(_datum->type() == avro::AVRO_RECORD);
            const auto& record = _datum->value<avro::GenericRecord>();

            auto st = read_row(record, column_raw_ptrs);
            if (st.is_data_quality_error()) {
                if (_counter->num_rows_filtered++ < MAX_ERROR_LINES_IN_FILE) {
                    std::string json_str;
                    (void)AvroUtils::datum_to_json(*_datum, &json_str);
                    _state->append_error_msg_to_file(json_str, std::string(st.message()));
                    LOG(WARNING) << "Failed to read row. error: " << st;
                }

                // before continuing to process other rows, we need to first clean the fail parsed row.
                chunk->set_num_rows(num_rows);
            } else if (!st.ok()) {
                return st;
            } else {
                --rows_to_read;
            }
        }

        if (chunk->is_empty()) {
            return Status::EndOfFile("No more data to read");
        } else {
            return Status::OK();
        }
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
