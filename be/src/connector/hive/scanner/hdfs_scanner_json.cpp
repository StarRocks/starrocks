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

#include "connector/hive/scanner/hdfs_scanner_json.h"

#include "base/compression/compression_utils.h"
#include "base/string/utf8.h"
#include "common/simdjson_util.h"
#include "formats/avro/nullable_column.h"
#include "formats/json/json_utils.h"
#include "formats/json/nullable_column.h"
#include "runtime/runtime_state.h"

namespace starrocks {

// static
std::map<std::string, std::string> HdfsJsonReader::_parse_column_name_mapping(
        const std::map<std::string, std::string>& serde_properties) {
    std::map<std::string, std::string> column_to_json_field;
    const std::string prefix = "mapping.";
    for (const auto& [key, value] : serde_properties) {
        if (key.size() <= prefix.size() || key.compare(0, prefix.size(), prefix) != 0 || value.empty()) {
            continue;
        }
        column_to_json_field[key.substr(prefix.size())] = value;
    }
    return column_to_json_field;
}

HdfsJsonReader::HdfsJsonReader(RandomAccessFile* file, const std::vector<SlotDescriptor*>& slot_descs,
                               const std::map<std::string, std::string>& serde_properties)
        : _column_to_json_field(_parse_column_name_mapping(serde_properties)) {
    _file = file;

    for (const auto* slot_desc : slot_descs) {
        if (slot_desc == nullptr) {
            continue;
        }
        std::string_view key = slot_desc->col_name();
        if (auto iter = _column_to_json_field.find(std::string(key)); iter != _column_to_json_field.end()) {
            key = iter->second;
        }
        _desc_dict[key].emplace_back(slot_desc, JsonUtils::construct_json_type(slot_desc->type()));
    }
}

Status HdfsJsonReader::_validate_fan_out_targets() const {
    for (const auto& [key, targets] : _desc_dict) {
        for (size_t i = 1; i < targets.size(); i++) {
            if (!(targets[i].second == targets[0].second)) {
                return Status::NotSupported(
                        fmt::format("Json field '{}' is mapped to columns '{}' and '{}' with different types", key,
                                    targets[0].first->col_name(), targets[i].first->col_name()));
            }
        }
    }
    return Status::OK();
}

Status HdfsJsonReader::init() {
    RETURN_IF_ERROR(_validate_fan_out_targets());
    ASSIGN_OR_RETURN(_buf, ByteBuffer::allocate_with_tracker(INIT_BUF_SIZE));
    return Status::OK();
}

Status HdfsJsonReader::next_record(Chunk* chunk, int32_t rows_to_read) {
    int32_t rows_read = 0;
    while (rows_read < rows_to_read) {
        if (_empty_parser) {
            RETURN_IF_ERROR(_read_and_parse_json());
        }

        int32_t cur_rows_read = 0;
        if (Status st = _read_rows(chunk, rows_to_read - rows_read, &cur_rows_read); !st.ok()) {
            if (st.is_end_of_file()) {
                rows_read += cur_rows_read;
                // The parser only saw [0, limit - _utf8_partial_tail); the partial UTF-8
                // tail held back in _read_and_parse_json must be carried over as well, so
                // add it back to the truncated (unconsumed) byte count.
                size_t truncated_bytes = _parser->truncated_bytes() + _utf8_partial_tail;
                if (truncated_bytes == _buf->limit) {
                    // TODO: support later
                    return Status::NotSupported(fmt::format(
                            "Currently one json record size larger than buf size {} is not supported", _buf->capacity));
                }
                _buf->pos = _buf->limit - truncated_bytes;
                _buf->flip_to_write();
                _empty_parser = true;
            } else {
                return st;
            }
        } else {
            rows_read += cur_rows_read;
        }
    }
    return Status::OK();
}

Status HdfsJsonReader::_read_and_parse_json() {
    RETURN_IF_ERROR(_read_file_stream());
    _parser = std::make_unique<JsonDocumentStreamParser>(&_simdjson_parser);
    _empty_parser = false;
    _buf->flip_to_read();
    // simdjson validates UTF-8 across the whole buffer and raises UTF8_ERROR when the
    // physical end of the buffer falls in the middle of a multi-byte character, which
    // happens when a fixed-size read boundary splits a character. Hold such a trailing
    // partial sequence back from this parse; it is carried over (see next_record) and
    // completed by the following bytes on the next read.
    _utf8_partial_tail = incomplete_trailing_utf8_len(_buf->ptr, _buf->limit);
    return _parser->parse(_buf->ptr, _buf->limit - _utf8_partial_tail, _buf->capacity);
}

Status HdfsJsonReader::_read_file_stream() const {
    size_t try_read_bytes = _buf->limit - _buf->pos;
    ASSIGN_OR_RETURN(int64_t read_size, _file->read(_buf->write_ptr(), try_read_bytes))
    if (read_size == 0) {
        return Status::EndOfFile("");
    }

    _buf->pos += read_size;
    return Status::OK();
}

Status HdfsJsonReader::_read_rows(Chunk* chunk, int32_t rows_to_read, int32_t* rows_read) {
    simdjson::ondemand::object row;
    while (*rows_read < rows_to_read) {
        if (auto st = _parser->get_current(&row); !st.ok()) {
            return st;
        }
        RETURN_IF_ERROR(_construct_row(&row, chunk));
        (*rows_read)++;
        RETURN_IF_ERROR(_parser->advance());
    }

    return Status::OK();
}

Status HdfsJsonReader::_construct_row(simdjson::ondemand::object* row, Chunk* chunk) {
    _parsed_columns.assign(chunk->num_columns(), false);
    faststring buf;

    try {
        uint32_t key_index = 0;
        for (auto field : *row) {
            std::string_view key = field_unescaped_key_safe(field, &buf);

            if (!(_prev_parsed_position.size() > key_index && _prev_parsed_position[key_index].key == key)) {
                auto iter = _desc_dict.find(key);
                std::vector<ColumnTarget> targets;
                if (iter != _desc_dict.end()) {
                    targets.reserve(iter->second.size());
                    for (const auto& [slot_desc, type_desc] : iter->second) {
                        targets.push_back({chunk->get_index_by_slot_id(slot_desc->id()), type_desc});
                    }
                }

                if (_prev_parsed_position.size() <= key_index) {
                    _prev_parsed_position.emplace_back(key, std::move(targets));
                } else {
                    _prev_parsed_position[key_index].key = key;
                    _prev_parsed_position[key_index].targets = std::move(targets);
                }
            }

            const auto& targets = _prev_parsed_position[key_index].targets;
            if (targets.empty()) {
                key_index++;
                continue;
            }

            // A json key mapped to several columns (fan-out) is decoded once into the first
            // not-yet-populated target, and cloned into the rest: they were validated in
            // init() to share the same type, and simdjson's on-demand `value` can only be
            // consumed once, so it can't be re-decoded per target.
            Column* primary_column = nullptr;
            for (const auto& target : targets) {
                if (_parsed_columns[target.column_index]) {
                    continue;
                }
                _parsed_columns[target.column_index] = true;
                auto* column = chunk->get_column_raw_ptr_by_index(target.column_index);
                if (primary_column == nullptr) {
                    auto value = field.value().value();
                    RETURN_IF_ERROR(
                            _construct_column(value, column, target.type, _prev_parsed_position[key_index].key));
                    primary_column = column;
                } else {
                    column->append(*primary_column, primary_column->size() - 1, 1);
                }
            }
            key_index++;
        }

    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("construct row in object order failed, error: $0",
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }

    for (int i = 0; i < chunk->num_columns(); i++) {
        if (!_parsed_columns[i]) {
            auto* column = chunk->get_column_raw_ptr_by_index(i);
            column->append_nulls(1);
        }
    }

    return Status::OK();
}

Status HdfsJsonReader::_construct_column(simdjson::ondemand::value& value, Column* column,
                                         const TypeDescriptor& type_desc, const std::string& col_name) {
    return add_nullable_column(column, type_desc, col_name, &value, true);
}

HdfsJsonScanner::HdfsJsonScanner(const std::map<std::string, std::string>& serde_properties)
        : _serde_properties(serde_properties) {}

Status HdfsJsonScanner::do_init(RuntimeState* runtime_state, const HdfsScannerContext& scanner_ctx) {
    const auto& text_file_desc = _scanner_ctx->scan_range->text_file_desc;
    return _setup_compression_type(text_file_desc);
}

Status HdfsJsonScanner::do_open(RuntimeState* runtime_state) {
    if (_no_data) {
        return Status::OK();
    }
    RETURN_IF_ERROR(open_random_access_file());

    SCOPED_RAW_TIMER(&_app_stats.reader_init_ns);
    _reader = std::make_unique<HdfsJsonReader>(_file.get(), _scanner_ctx->slot_descs, _serde_properties);
    RETURN_IF_ERROR(_reader->init());

    return Status::OK();
}

void HdfsJsonScanner::do_close(RuntimeState* runtime_state) noexcept {
    if (_no_data) {
        return;
    }
    _reader.reset();
}

Status HdfsJsonScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    if (_no_data) {
        return Status::EndOfFile("");
    }

    Status st = _reader->next_record(chunk->get(), runtime_state->chunk_size());
    if (!st.ok()) {
        if (st.is_end_of_file()) {
            _no_data = true;
            st = Status::OK();
        } else {
            return st;
        }
    }

    if ((*chunk)->num_rows() > 0) {
        size_t rows_read = (*chunk)->num_rows();
        RETURN_IF_ERROR(_scanner_ctx->format_scan_context.append_side_columns_to_chunk(chunk, rows_read));
        RETURN_IF_ERROR(_scanner_ctx->format_scan_context.evaluate_all_predicates(chunk));
    }

    return st;
}

Status HdfsJsonScanner::_setup_compression_type(const TTextFileDesc& text_file_desc) {
    // by default, it's unknown compression. we will synthesize information from FE and BE(file extension)
    // parse a compression type from FE first.
    CompressionTypePB compression_type;
    if (text_file_desc.__isset.compression_type) {
        compression_type = CompressionUtils::to_compression_pb(text_file_desc.compression_type);
    } else {
        // if FE does not specify a compress type, we choose it by looking at the filename.
        compression_type = get_compression_type_from_path(_scanner_ctx->file_path);
    }
    if (compression_type != UNKNOWN_COMPRESSION) {
        _compression_type = compression_type;
    } else {
        _compression_type = NO_COMPRESSION;
    }
    return Status::OK();
}
} // namespace starrocks
