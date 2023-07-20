// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/segment_writer.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/rowset/segment_writer.h"

#include <memory>
#include <utility>

#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "column/nullable_column.h"
#include "common/logging.h" // LOG
#include "fs/fs.h"          // FileSystem
#include "gen_cpp/segment.pb.h"
#include "runtime/primitive_type.h"
#include "storage/field.h"
#include "storage/rowset/column_writer.h" // ColumnWriter
#include "storage/rowset/page_io.h"
#include "storage/seek_tuple.h"
#include "storage/short_key_index.h"
#include "util/crc32c.h"
#include "util/faststring.h"
#include "util/json.h"

namespace starrocks {

const char* const k_segment_magic = "D0R1";
const uint32_t k_segment_magic_length = 4;

SegmentWriter::SegmentWriter(std::unique_ptr<WritableFile> wfile, uint32_t segment_id,
                             const TabletSchema* tablet_schema, SegmentWriterOptions opts)
        : _segment_id(segment_id), _tablet_schema(tablet_schema), _opts(std::move(opts)), _wfile(std::move(wfile)) {
    CHECK_NOTNULL(_wfile.get());
}

SegmentWriter::~SegmentWriter() = default;

std::string SegmentWriter::segment_path() const {
    return _wfile->filename();
}

void SegmentWriter::_init_column_meta(ColumnMetaPB* meta, uint32_t column_id, const TabletColumn& column) {
    meta->set_column_id(column_id);
    meta->set_unique_id(column.unique_id());
    meta->set_type(column.type());
    meta->set_length(column.length());
    meta->set_encoding(DEFAULT_ENCODING);
    // For column_writer, data_page_body includes two slices: `encoded values` + `nullmap`.
    // However, LZ4 doesn't support compressing multiple slices. In order to use LZ4, one solution is to
    // copy the contents of the slice `nullmap` into the slice `encoded values`, but the cost of copying is still not small.
    // Here we set the compression from _tablet_schema which given from CREATE TABLE statement.
    meta->set_compression(_tablet_schema->compression_type());
    meta->set_is_nullable(column.is_nullable());

    // TODO(mofei) set the format_version from column
    if (column.type() == OLAP_FIELD_TYPE_JSON) {
        JsonMetaPB* json_meta = meta->mutable_json_meta();
        json_meta->set_format_version(kJsonMetaDefaultFormatVersion);
    }

    for (uint32_t i = 0; i < column.subcolumn_count(); ++i) {
        _init_column_meta(meta->add_children_columns(), column_id, column.subcolumn(i));
    }
}

Status SegmentWriter::init() {
    std::vector<uint32_t> all_column_indexes;
    for (uint32_t i = 0; i < _tablet_schema->num_columns(); ++i) {
        all_column_indexes.emplace_back(i);
    }
    return init(all_column_indexes, true);
}

inline bool is_zone_map_key_type(FieldType type) {
    return type != FieldType::OLAP_FIELD_TYPE_CHAR && type != FieldType::OLAP_FIELD_TYPE_VARCHAR &&
           type != FieldType::OLAP_FIELD_TYPE_JSON && type != FieldType::OLAP_FIELD_TYPE_OBJECT &&
           type != FieldType::OLAP_FIELD_TYPE_HLL && type != FieldType::OLAP_FIELD_TYPE_PERCENTILE;
}

Status SegmentWriter::init(const std::vector<uint32_t>& column_indexes, bool has_key, SegmentFooterPB* footer) {
    DCHECK(_column_writers.empty());
    DCHECK(_column_indexes.empty());

    // merge partial segment footer
    // in partial update, key columns and some value columns have been written in partial segment
    // rewrite partial segment into full segment only need to write other value columns into full segment
    // merge partial segment footer to avoid loss of metadata
    if (footer != nullptr) {
        for (uint32_t ordinal = 0; ordinal < footer->columns().size(); ++ordinal) {
            *_footer.add_columns() = footer->columns(ordinal);
        }
        if (footer->has_short_key_index_page()) {
            *_footer.mutable_short_key_index_page() = footer->short_key_index_page();
        }
        // in partial update, key columns have been written in partial segment
        // set _num_rows as _num_rows in partial segment
        _num_rows = footer->num_rows();
    }

    _column_indexes.insert(_column_indexes.end(), column_indexes.begin(), column_indexes.end());
    _column_writers.reserve(_column_indexes.size());
    size_t num_columns = _tablet_schema->num_columns();
    std::map<uint32_t, uint32_t> sort_column_idx_by_column_index;
    for (uint32_t i = 0; i < _column_indexes.size(); i++) {
        uint32_t column_index = _column_indexes[i];
        if (column_index >= num_columns) {
            return Status::InternalError(
                    strings::Substitute("column index $0 out of range $1", column_index, num_columns));
        }

        const auto& column = _tablet_schema->column(column_index);
        ColumnWriterOptions opts;
        opts.page_format = 2;
        opts.meta = _footer.add_columns();

        if (!_opts.referenced_column_ids.empty()) {
            DCHECK(_opts.referenced_column_ids.size() == num_columns);
            _init_column_meta(opts.meta, _opts.referenced_column_ids[column_index], column);
        } else {
            _init_column_meta(opts.meta, column_index, column);
        }

        // now we create zone map for key columns
        // and not support zone map for array type.
        // TODO(mofei) refactor it to type specification
        const bool enable_pk_zone_map = config::enable_pk_value_column_zonemap &&
                                        _tablet_schema->keys_type() == KeysType::PRIMARY_KEYS &&
                                        is_zone_map_key_type(column.type());
        const bool enable_dup_zone_map =
                _tablet_schema->keys_type() == KeysType::DUP_KEYS && is_zone_map_key_type(column.type());
        opts.need_zone_map = column.is_key() || enable_pk_zone_map || enable_dup_zone_map || column.is_sort_key();
        if (column.type() == FieldType::OLAP_FIELD_TYPE_ARRAY) {
            opts.need_zone_map = false;
        }
        opts.need_bloom_filter = column.is_bf_column();
        opts.need_bitmap_index = column.has_bitmap_index();
        if (column.type() == FieldType::OLAP_FIELD_TYPE_ARRAY) {
            if (opts.need_bloom_filter) {
                return Status::NotSupported("Do not support bloom filter for array type");
            }
            if (opts.need_bitmap_index) {
                return Status::NotSupported("Do not support bitmap index for array type");
            }
        }

        if (column.type() == FieldType::OLAP_FIELD_TYPE_VARCHAR && _opts.global_dicts != nullptr) {
            auto iter = _opts.global_dicts->find(column.name().data());
            if (iter != _opts.global_dicts->end()) {
                opts.global_dict = &iter->second.dict;
                _global_dict_columns_valid_info[iter->first] = true;
            }
        }

        ASSIGN_OR_RETURN(auto writer, ColumnWriter::create(opts, &column, _wfile.get()));
        RETURN_IF_ERROR(writer->init());
        _column_writers.push_back(std::move(writer));
        if (column.is_sort_key()) {
            sort_column_idx_by_column_index[column_index] = i;
        }
    }
    if (!sort_column_idx_by_column_index.empty()) {
        for (auto& column_idx : _tablet_schema->sort_key_idxes()) {
            auto iter = sort_column_idx_by_column_index.find(column_idx);
            if (iter != sort_column_idx_by_column_index.end()) {
                _sort_column_indexes.emplace_back(iter->second);
            } else {
                // Currently we have the following two scenarios：
                //  1. data load or horizontal compaction, we will write the whole row data once a time
                //  2. vertical compaction, we will first write all sort key columns and write value columns by group
                // So the all sort key columns should be found in `_column_indexes` so far.
                std::string err_msg =
                        strings::Substitute("column[$0]: $1 is sort key but not find while init segment writer",
                                            column_idx, _tablet_schema->column(column_idx).name().data());
                return Status::InternalError(err_msg);
            }
        }
    }

    _has_key = has_key;
    if (_has_key) {
        _index_builder = std::make_unique<ShortKeyIndexBuilder>(_segment_id, _opts.num_rows_per_block);
    }
    return Status::OK();
}

// TODO(lingbin): Currently this function does not include the size of various indexes,
// We should make this more precise.
// NOTE: This function will be called when any row of data is added, so we need to
// make this function efficient.
uint64_t SegmentWriter::estimate_segment_size() {
    // footer_size(4) + checksum(4) + segment_magic(4)
    uint64_t size = 12;
    for (auto& column_writer : _column_writers) {
        size += column_writer->estimate_buffer_size();
    }
    size += _index_builder->size();
    return size;
}

Status SegmentWriter::finalize(uint64_t* segment_file_size, uint64_t* index_size, uint64_t* footer_position) {
    RETURN_IF_ERROR(finalize_columns(index_size));
    *footer_position = _wfile->size();
    return finalize_footer(segment_file_size);
}

Status SegmentWriter::finalize_columns(uint64_t* index_size) {
    if (_has_key) {
        _num_rows = _num_rows_written;
    } else if (_num_rows != _num_rows_written) {
        return Status::InternalError(strings::Substitute("num rows written $0 is not equal to segment num rows $1",
                                                         _num_rows_written, _num_rows));
    }
    _num_rows_written = 0;

    size_t num_columns = _tablet_schema->num_columns();
    for (size_t i = 0; i < _column_indexes.size(); ++i) {
        uint32_t column_index = _column_indexes[i];
        if (column_index >= num_columns) {
            return Status::InternalError(
                    strings::Substitute("column index $0 out of range $1", column_index, num_columns));
        }

        auto& column_writer = _column_writers[i];
        RETURN_IF_ERROR(column_writer->finish());
        // write data
        RETURN_IF_ERROR(column_writer->write_data());
        // write index
        uint64_t index_offset = _wfile->size();
        RETURN_IF_ERROR(column_writer->write_ordinal_index());
        RETURN_IF_ERROR(column_writer->write_zone_map());
        RETURN_IF_ERROR(column_writer->write_bitmap_index());
        RETURN_IF_ERROR(column_writer->write_bloom_filter_index());
        *index_size += _wfile->size() - index_offset;

        // check global dict valid
        const auto& column = _tablet_schema->column(column_index);
        if (!column_writer->is_global_dict_valid() && is_string_type(column.type())) {
            std::string col_name(column.name());
            _global_dict_columns_valid_info[col_name] = false;
        }

        // reset to release memory
        column_writer.reset();
    }
    _column_writers.clear();
    _column_indexes.clear();

    if (_has_key) {
        uint64_t index_offset = _wfile->size();
        RETURN_IF_ERROR(_write_short_key_index());
        *index_size += _wfile->size() - index_offset;
        _index_builder.reset();
    }
    return Status::OK();
}

Status SegmentWriter::finalize_footer(uint64_t* segment_file_size, uint64_t* footer_position) {
    if (footer_position != nullptr) {
        *footer_position = _wfile->size();
    }
    RETURN_IF_ERROR(_write_footer());
    *segment_file_size = _wfile->size();
    return _wfile->close();
}

Status SegmentWriter::_write_short_key_index() {
    std::vector<Slice> body;
    PageFooterPB footer;
    RETURN_IF_ERROR(_index_builder->finalize(_num_rows, &body, &footer));
    PagePointer pp;
    // short key index page is not compressed right now
    RETURN_IF_ERROR(PageIO::write_page(_wfile.get(), body, footer, &pp));
    pp.to_proto(_footer.mutable_short_key_index_page());
    return Status::OK();
}

Status SegmentWriter::_write_footer() {
    _footer.set_version(2);
    _footer.set_num_rows(_num_rows);

    // Footer := SegmentFooterPB, FooterPBSize(4), FooterPBChecksum(4), MagicNumber(4)
    std::string footer_buf;
    if (!_footer.SerializeToString(&footer_buf)) {
        return Status::InternalError("failed to serialize segment footer");
    }

    faststring fixed_buf;
    // footer's size
    put_fixed32_le(&fixed_buf, footer_buf.size());
    // footer's checksum
    uint32_t checksum = crc32c::Value(footer_buf.data(), footer_buf.size());
    put_fixed32_le(&fixed_buf, checksum);
    // Append magic number. we don't write magic number in the header because
    // that will need an extra seek when reading
    fixed_buf.append(k_segment_magic, k_segment_magic_length);

    std::vector<Slice> slices{footer_buf, fixed_buf};
    return _write_raw_data(slices);
}

Status SegmentWriter::_write_raw_data(const std::vector<Slice>& slices) {
    RETURN_IF_ERROR(_wfile->appendv(&slices[0], slices.size()));
    return Status::OK();
}

Status SegmentWriter::append_chunk(const vectorized::Chunk& chunk) {
    DCHECK_EQ(_column_writers.size(), chunk.num_columns());
    for (size_t i = 0; i < _column_writers.size(); ++i) {
        const vectorized::Column* col = chunk.get_column_by_index(i).get();
        RETURN_IF_ERROR(_column_writers[i]->append(*col));
    }

    size_t chunk_num_rows = chunk.num_rows();
    if (_has_key) {
        for (size_t i = 0; i < chunk_num_rows; i++) {
            // At the begin of one block, so add a short key index entry
            if ((_num_rows_written % _opts.num_rows_per_block) == 0) {
                size_t keys = _tablet_schema->num_short_key_columns();
                vectorized::SeekTuple tuple(*chunk.schema(), chunk.get(i).datums());
                std::string encoded_key;
                encoded_key = tuple.short_key_encode(keys, _sort_column_indexes, 0);
                RETURN_IF_ERROR(_index_builder->add_item(encoded_key));
            }
            ++_num_rows_written;
        }
    } else {
        _num_rows_written += chunk_num_rows;
    }
    return Status::OK();
}

} // namespace starrocks
