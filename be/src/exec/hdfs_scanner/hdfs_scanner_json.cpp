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

#include "exec/hdfs_scanner/hdfs_scanner_json.h"

#include "formats/avro/nullable_column.h"
#include "formats/json/json_utils.h"
#include "formats/json/nullable_column.h"
#include "util/compression/compression_utils.h"
#include "util/simdjson_util.h"

namespace starrocks {

HdfsJsonReader::HdfsJsonReader(RandomAccessFile* file, const std::vector<SlotDescriptor*>& slot_descs) {
    _file = file;

    for (const auto* slot_desc : slot_descs) {
        if (slot_desc == nullptr) {
            continue;
        }
        _desc_dict.emplace(slot_desc->col_name(),
                           std::make_pair(slot_desc, JsonUtils::construct_json_type(slot_desc->type())));
    }
}

Status HdfsJsonReader::init() {
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
                size_t truncated_bytes = _parser->truncated_bytes();
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
    return _parser->parse(_buf->ptr, _buf->limit, _buf->capacity);
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
            int column_index;
            std::string_view key = field_unescaped_key_safe(field, &buf);

            if (_prev_parsed_position.size() > key_index && _prev_parsed_position[key_index].key == key) {
                column_index = _prev_parsed_position[key_index].column_index;
                if (column_index < 0) {
                    key_index++;
                    continue;
                }
            } else {
                auto iter = _desc_dict.find(key);
                if (iter == _desc_dict.end()) {
                    _prev_parsed_position.emplace_back(key);
                    key_index++;
                    continue;
                }

                const auto* slot_desc = iter->second.first;
                const auto& type_desc = iter->second.second;
                column_index = chunk->get_index_by_slot_id(slot_desc->id());

                if (_prev_parsed_position.size() <= key_index) {
                    _prev_parsed_position.emplace_back(key, column_index, type_desc);
                } else {
                    _prev_parsed_position[key_index].key = key;
                    _prev_parsed_position[key_index].column_index = column_index;
                    _prev_parsed_position[key_index].type = type_desc;
                }
            }

            if (_parsed_columns[column_index]) {
                key_index++;
                continue;
            }
            _parsed_columns[column_index] = true;

            auto& column = chunk->get_column_by_index(column_index);

            auto value = field.value().value();

            RETURN_IF_ERROR(_construct_column(value, column.get(), _prev_parsed_position[key_index].type,
                                              _prev_parsed_position[key_index].key));
            key_index++;
        }

    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("construct row in object order failed, error: $0",
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }

    for (int i = 0; i < chunk->num_columns(); i++) {
        if (!_parsed_columns[i]) {
            auto& column = chunk->get_column_by_index(i);
            column->append_nulls(1);
        }
    }

    return Status::OK();
}

Status HdfsJsonReader::_construct_column(simdjson::ondemand::value& value, Column* column,
                                         const TypeDescriptor& type_desc, const std::string& col_name) {
    return add_nullable_column(column, type_desc, col_name, &value, true);
}

Status HdfsJsonScanner::do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    const auto& text_file_desc = _scanner_params.scan_range->text_file_desc;
    return _setup_compression_type(text_file_desc);
}

Status HdfsJsonScanner::do_open(RuntimeState* runtime_state) {
    if (_no_data) {
        return Status::OK();
    }
    RETURN_IF_ERROR(open_random_access_file());

    SCOPED_RAW_TIMER(&_app_stats.reader_init_ns);
    _reader = std::make_unique<HdfsJsonReader>(_file.get(), _scanner_ctx.slot_descs);
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
        RETURN_IF_ERROR(_scanner_ctx.append_or_update_not_existed_columns_to_chunk(chunk, rows_read));
        _scanner_ctx.append_or_update_partition_column_to_chunk(chunk, rows_read);

        for (auto& [_, ctxs] : _scanner_ctx.conjunct_ctxs_by_slot) {
            SCOPED_RAW_TIMER(&_app_stats.expr_filter_ns);
            RETURN_IF_ERROR(ExecNode::eval_conjuncts(ctxs, chunk->get()));
            if ((*chunk)->num_rows() == 0) {
                break;
            }
        }
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
        compression_type = get_compression_type_from_path(_scanner_params.path);
    }
    if (compression_type != UNKNOWN_COMPRESSION) {
        _compression_type = compression_type;
    } else {
        _compression_type = NO_COMPRESSION;
    }
    return Status::OK();
}
} // namespace starrocks