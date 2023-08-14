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

#include "formats/parquet/column_reader.h"

#include <boost/algorithm/string.hpp>

#include "column/array_column.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "exec/exec_node.h"
#include "exec/hdfs_scanner.h"
#include "exprs/expr.h"
#include "formats/parquet/column_converter.h"
#include "formats/parquet/stored_column_reader.h"
#include "formats/parquet/stored_column_reader_with_index.h"
#include "formats/parquet/utils.h"
#include "gutil/strings/substitute.h"
#include "io/shared_buffered_input_stream.h"
#include "simd/batch_run_counter.h"
#include "storage/column_or_predicate.h"
#include "util/runtime_profile.h"
#include "util/thrift_util.h"
#include "utils.h"

namespace starrocks {
class RandomAccessFile;
}

namespace starrocks::parquet {

template <typename TOffset, typename TIsNull>
static void def_rep_to_offset(const LevelInfo& level_info, const level_t* def_levels, const level_t* rep_levels,
                              size_t num_levels, TOffset* offsets, TIsNull* is_nulls, size_t* num_offsets,
                              bool* has_null) {
    size_t offset_pos = 0;
    for (int i = 0; i < num_levels; ++i) {
        // when def_level is less than immediate_repeated_ancestor_def_level, it means that level
        // will affect its ancestor.
        // when rep_level is greater than max_rep_level, this means that level affects its
        // descendants.
        // So we can skip this levels
        if (def_levels[i] < level_info.immediate_repeated_ancestor_def_level ||
            rep_levels[i] > level_info.max_rep_level) {
            continue;
        }
        if (rep_levels[i] == level_info.max_rep_level) {
            offsets[offset_pos]++;
            continue;
        }

        // Start for a new row
        offset_pos++;
        offsets[offset_pos] = offsets[offset_pos - 1];
        if (def_levels[i] >= level_info.max_def_level) {
            offsets[offset_pos]++;
        }

        // when def_level equals with max_def_level, this is a non null element or a required element
        // when def_level equals with (max_def_level - 1), this indicates an empty array
        // when def_level less than (max_def_level - 1) it means this array is null
        if (def_levels[i] >= level_info.max_def_level - 1) {
            is_nulls[offset_pos - 1] = 0;
        } else {
            is_nulls[offset_pos - 1] = 1;
            *has_null = true;
        }
    }
    *num_offsets = offset_pos;
}

class ScalarColumnReader : public ColumnReader {
public:
    explicit ScalarColumnReader(const ColumnReaderOptions& opts) : _opts(opts) {}
    ~ScalarColumnReader() override = default;

    Status init(const ParquetField* field, const TypeDescriptor& col_type,
                const tparquet::ColumnChunk* chunk_metadata) {
        _field = field;
        _col_type = &col_type;
        _chunk_metadata = chunk_metadata;
        RETURN_IF_ERROR(ColumnConverterFactory::create_converter(*field, col_type, _opts.timezone, &converter));
        return StoredColumnReader::create(_opts, field, chunk_metadata, &_reader);
    }

    Status prepare_batch(size_t* num_records, Column* dst) override {
        DCHECK(_field->is_nullable ? dst->is_nullable() : true);
        ColumnContentType content_type =
                _dict_filter_ctx == nullptr ? ColumnContentType::VALUE : ColumnContentType::DICT_CODE;
        if (!converter->need_convert) {
            return _reader->read_records(num_records, content_type, dst);
        } else {
            auto column = converter->create_src_column();

            Status status = _reader->read_records(num_records, content_type, column.get());
            if (!status.ok() && !status.is_end_of_file()) {
                return status;
            }

            {
                SCOPED_RAW_TIMER(&_opts.stats->column_convert_ns);
                RETURN_IF_ERROR(converter->convert(column, dst));
            }

            return Status::OK();
        }
    }

    Status read_range(const Range<uint64_t>& range, const Filter* filter, Column* dst) override {
        DCHECK(_field->is_nullable ? dst->is_nullable() : true);
        ColumnContentType content_type =
                _dict_filter_ctx == nullptr ? ColumnContentType::VALUE : ColumnContentType::DICT_CODE;
        if (!converter->need_convert) {
            return _reader->read_range(range, filter, content_type, dst);
        } else {
            SCOPED_RAW_TIMER(&_opts.stats->column_convert_ns);
            auto column = converter->create_src_column();
            RETURN_IF_ERROR(_reader->read_range(range, filter, content_type, column.get()));
            return converter->convert(column, dst);
        }
    }

    Status finish_batch() override { return Status::OK(); }

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        _reader->get_levels(def_levels, rep_levels, num_levels);
    }

    Status get_dict_values(const std::vector<int32_t>& dict_codes, const NullableColumn& nulls,
                           Column* column) override {
        return _reader->get_dict_values(dict_codes, nulls, column);
    }

    void set_need_parse_levels(bool need_parse_levels) override { _reader->set_need_parse_levels(need_parse_levels); }

    bool try_to_use_dict_filter(ExprContext* ctx, bool is_decode_needed, const SlotId slotId,
                                const std::vector<std::string>& sub_field_path, const size_t& layer) override {
        if (sub_field_path.size() != layer) {
            return false;
        }

        if (!_col_type->is_string_type()) {
            return false;
        }

        if (_column_all_pages_dict_encoded()) {
            if (_dict_filter_ctx == nullptr) {
                _dict_filter_ctx = std::make_unique<ColumnDictFilterContext>();
                _dict_filter_ctx->is_decode_needed = is_decode_needed;
                _dict_filter_ctx->sub_field_path = sub_field_path;
                _dict_filter_ctx->slot_id = slotId;
            }
            _dict_filter_ctx->conjunct_ctxs.push_back(ctx);
            return true;
        } else {
            return false;
        }
    }

    Status rewrite_conjunct_ctxs_to_predicate(bool* is_group_filtered, const std::vector<std::string>& sub_field_path,
                                              const size_t& layer) override {
        DCHECK_EQ(sub_field_path.size(), layer);
        return _dict_filter_ctx->rewrite_conjunct_ctxs_to_predicate(_reader.get(), is_group_filtered);
    }

    void init_dict_column(ColumnPtr& column, const std::vector<std::string>& sub_field_path,
                          const size_t& layer) override {
        DCHECK_EQ(sub_field_path.size(), layer);
        auto dict_code_column = ColumnHelper::create_column(
                TypeDescriptor::from_logical_type(ColumnDictFilterContext::kDictCodePrimitiveType), true);
        dict_code_column->reserve(column->size());
        column = dict_code_column;
    }

    Status filter_dict_column(const ColumnPtr& column, Filter* filter, const std::vector<std::string>& sub_field_path,
                              const size_t& layer) override {
        DCHECK_EQ(sub_field_path.size(), layer);
        return _dict_filter_ctx->predicate->evaluate_and(column.get(), filter->data());
    }

    Status fill_dst_column(ColumnPtr& dst, const ColumnPtr& src) override {
        if (_dict_filter_ctx == nullptr) {
            dst->swap_column(*src);
        } else {
            if (_dict_filter_ctx->is_decode_needed) {
                ColumnPtr& dict_values = dst;
                dict_values->resize(0);

                // decode dict code to dict values.
                // note that in dict code, there could be null value.
                const ColumnPtr& dict_codes = src;
                auto* codes_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(dict_codes);
                auto* codes_column =
                        ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(codes_nullable_column->data_column());
                RETURN_IF_ERROR(get_dict_values(codes_column->get_data(), *codes_nullable_column, dict_values.get()));
                DCHECK_EQ(dict_codes->size(), dict_values->size());
                if (dict_values->is_nullable()) {
                    auto* nullable_codes = down_cast<NullableColumn*>(dict_codes.get());
                    auto* nullable_values = down_cast<NullableColumn*>(dict_values.get());
                    nullable_values->null_column_data().swap(nullable_codes->null_column_data());
                    nullable_values->set_has_null(nullable_codes->has_null());
                }
            } else {
                dst->append_default(src->size());
            }

            src->reset_column();
        }
        return Status::OK();
    }

    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                 ColumnIOType type, bool active) override;

    const tparquet::ColumnChunk* get_chunk_metadata() override { return _chunk_metadata; }

    const ParquetField* get_column_parquet_field() override { return _field; }

    StatusOr<tparquet::OffsetIndex*> get_offset_index(const uint64_t rg_first_row) override {
        if (_offset_index_ctx == nullptr) {
            _offset_index_ctx = std::make_unique<ColumnOffsetIndexCtx>();
            _offset_index_ctx->rg_first_row = rg_first_row;
            int64_t offset_index_offset = _chunk_metadata->offset_index_offset;
            uint32_t offset_index_length = _chunk_metadata->offset_index_length;
            std::vector<uint8_t> offset_index_data;
            offset_index_data.reserve(offset_index_length);
            RETURN_IF_ERROR(
                    _opts.file->read_at_fully(offset_index_offset, offset_index_data.data(), offset_index_length));

            RETURN_IF_ERROR(deserialize_thrift_msg(offset_index_data.data(), &offset_index_length,
                                                   TProtocolType::COMPACT, &_offset_index_ctx->offset_index));
        }
        return &_offset_index_ctx->offset_index;
    }

    void select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) override;

private:
    // Returns true if all of the data pages in the column chunk are dict encoded
    bool _column_all_pages_dict_encoded();

    const ColumnReaderOptions& _opts;

    std::unique_ptr<StoredColumnReader> _reader;
    const ParquetField* _field = nullptr;

    std::unique_ptr<ColumnDictFilterContext> _dict_filter_ctx;
    const TypeDescriptor* _col_type = nullptr;
    const tparquet::ColumnChunk* _chunk_metadata = nullptr;
    std::unique_ptr<ColumnOffsetIndexCtx> _offset_index_ctx;
};

bool ScalarColumnReader::_column_all_pages_dict_encoded() {
    // The Parquet spec allows for column chunks to have mixed encodings
    // where some data pages are dictionary-encoded and others are plain
    // encoded. For example, a Parquet file writer might start writing
    // a column chunk as dictionary encoded, but it will switch to plain
    // encoding if the dictionary grows too large.
    //
    // In order for dictionary filters to skip the entire row group,
    // the conjuncts must be evaluated on column chunks that are entirely
    // encoded with the dictionary encoding. There are two checks
    // available to verify this:
    // 1. The encoding_stats field on the column chunk metadata provides
    //    information about the number of data pages written in each
    //    format. This allows for a specific check of whether all the
    //    data pages are dictionary encoded.
    // 2. The encodings field on the column chunk metadata lists the
    //    encodings used. If this list contains the dictionary encoding
    //    and does not include unexpected encodings (i.e. encodings not
    //    associated with definition/repetition levels), then it is entirely
    //    dictionary encoded.
    const tparquet::ColumnMetaData& column_metadata =
            _opts.row_group_meta->columns[_field->physical_column_index].meta_data;
    if (column_metadata.__isset.encoding_stats) {
        // Condition #1 above
        for (const tparquet::PageEncodingStats& enc_stat : column_metadata.encoding_stats) {
            if (enc_stat.page_type == tparquet::PageType::DATA_PAGE &&
                (enc_stat.encoding != tparquet::Encoding::PLAIN_DICTIONARY &&
                 enc_stat.encoding != tparquet::Encoding::RLE_DICTIONARY) &&
                enc_stat.count > 0) {
                return false;
            }
        }
    } else {
        // Condition #2 above
        bool has_dict_encoding = false;
        bool has_nondict_encoding = false;
        for (const tparquet::Encoding::type& encoding : column_metadata.encodings) {
            if (encoding == tparquet::Encoding::PLAIN_DICTIONARY || encoding == tparquet::Encoding::RLE_DICTIONARY) {
                has_dict_encoding = true;
            }

            // RLE and BIT_PACKED are used for repetition/definition levels
            if (encoding != tparquet::Encoding::PLAIN_DICTIONARY && encoding != tparquet::Encoding::RLE_DICTIONARY &&
                encoding != tparquet::Encoding::RLE && encoding != tparquet::Encoding::BIT_PACKED) {
                has_nondict_encoding = true;
                break;
            }
        }
        // Not entirely dictionary encoded if:
        // 1. No dictionary encoding listed
        // OR
        // 2. Some non-dictionary encoding is listed
        if (!has_dict_encoding || has_nondict_encoding) {
            return false;
        }
    }

    return true;
}

void ScalarColumnReader::collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges,
                                                 int64_t* end_offset, ColumnIOType type, bool active) {
    const auto& column = _opts.row_group_meta->columns[_field->physical_column_index];
    if (type == ColumnIOType::PAGES) {
        const tparquet::ColumnMetaData& column_metadata = column.meta_data;
        if (_offset_index_ctx != nullptr) {
            // add dict page
            if (column_metadata.__isset.dictionary_page_offset) {
                auto r = io::SharedBufferedInputStream::IORange(
                        column_metadata.dictionary_page_offset,
                        column_metadata.data_page_offset - column_metadata.dictionary_page_offset, active);
                ranges->emplace_back(r);
                *end_offset = std::max(*end_offset, r.offset + r.size);
            }
            _offset_index_ctx->collect_io_range(ranges, end_offset, active);
        } else {
            int64_t offset = 0;
            if (column_metadata.__isset.dictionary_page_offset) {
                offset = column_metadata.dictionary_page_offset;
            } else {
                offset = column_metadata.data_page_offset;
            }
            int64_t size = column_metadata.total_compressed_size;
            auto r = io::SharedBufferedInputStream::IORange(offset, size, active);
            ranges->emplace_back(r);
            *end_offset = std::max(*end_offset, offset + size);
        }
    } else if (type == ColumnIOType::PAGE_INDEX) {
        // only active column need column index
        if (column.__isset.column_index_offset && active) {
            auto r = io::SharedBufferedInputStream::IORange(column.column_index_offset, column.column_index_length);
            ranges->emplace_back(r);
        }
        // all column need offset index
        if (column.__isset.offset_index_offset) {
            auto r = io::SharedBufferedInputStream::IORange(column.offset_index_offset, column.offset_index_length);
            ranges->emplace_back(r);
        }
    }
}

void ScalarColumnReader::select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) {
    if (_offset_index_ctx == nullptr) {
        if (!_chunk_metadata->__isset.offset_index_offset) {
            return;
        }
        auto st = get_offset_index(rg_first_row);
        if (!st.ok()) {
            return;
        }
    }
    size_t page_num = _offset_index_ctx->offset_index.page_locations.size();
    size_t range_size = range.size();

    size_t range_idx = 0;
    Range<uint64_t> r = range[range_idx++];

    for (size_t i = 0; i < page_num; i++) {
        int64_t first_row = _offset_index_ctx->offset_index.page_locations[i].first_row_index + rg_first_row;
        int64_t end_row = first_row;
        if (i != page_num - 1) {
            end_row = _offset_index_ctx->offset_index.page_locations[i + 1].first_row_index + rg_first_row;
        } else {
            // a little trick, we don't care about the real rows of the last page.
            if (range.end() < first_row) {
                _offset_index_ctx->page_selected.emplace_back(false);
                continue;
            } else {
                end_row = range.end();
            }
        }
        if (end_row <= r.begin()) {
            _offset_index_ctx->page_selected.emplace_back(false);
            continue;
        }
        while (first_row >= r.end() && range_idx < range_size) {
            r = range[range_idx++];
        }
        _offset_index_ctx->page_selected.emplace_back(first_row < r.end() && end_row > r.begin());
    }
    _reader = std::make_unique<StoredColumnReaderWithIndex>(std::move(_reader), _offset_index_ctx.get());
}

Status ColumnDictFilterContext::rewrite_conjunct_ctxs_to_predicate(StoredColumnReader* reader,
                                                                   bool* is_group_filtered) {
    // create dict value chunk for evaluation.
    ColumnPtr dict_value_column = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true);
    RETURN_IF_ERROR(reader->get_dict_values(dict_value_column.get()));
    // append a null value to check if null is ok or not.
    dict_value_column->append_default();

    ColumnPtr result_column = dict_value_column;
    for (int32_t i = sub_field_path.size() - 1; i >= 0; i--) {
        if (!result_column->is_nullable()) {
            result_column =
                    NullableColumn::create(std::move(result_column), NullColumn::create(result_column->size(), 0));
        }
        Columns columns;
        columns.emplace_back(result_column);
        std::vector<std::string> field_names;
        field_names.emplace_back(sub_field_path[i]);
        result_column = StructColumn::create(std::move(columns), std::move(field_names));
    }

    ChunkPtr dict_value_chunk = std::make_shared<Chunk>();
    dict_value_chunk->append_column(result_column, slot_id);
    Filter filter(dict_value_column->size(), 1);
    int dict_values_after_filter = 0;
    ASSIGN_OR_RETURN(dict_values_after_filter,
                     ExecNode::eval_conjuncts_into_filter(conjunct_ctxs, dict_value_chunk.get(), &filter));

    // dict column is empty after conjunct eval, file group can be skipped
    if (dict_values_after_filter == 0) {
        *is_group_filtered = true;
        return Status::OK();
    }

    // ---------
    // get dict codes according to dict values pos.
    std::vector<int32_t> dict_codes;
    BatchRunCounter<32> batch_run(filter.data(), 0, filter.size() - 1);
    BatchCount batch = batch_run.next_batch();
    int index = 0;
    while (batch.length > 0) {
        if (batch.AllSet()) {
            for (int32_t i = 0; i < batch.length; i++) {
                dict_codes.emplace_back(index + i);
            }
        } else if (batch.NoneSet()) {
            // do nothing
        } else {
            for (int32_t i = 0; i < batch.length; i++) {
                if (filter[index + i]) {
                    dict_codes.emplace_back(index + i);
                }
            }
        }
        index += batch.length;
        batch = batch_run.next_batch();
    }

    bool null_is_ok = filter[filter.size() - 1] == 1;

    // eq predicate is faster than in predicate
    // TODO: improve not eq and not in
    if (dict_codes.size() == 0) {
        predicate = nullptr;
    } else if (dict_codes.size() == 1) {
        predicate = obj_pool.add(
                new_column_eq_predicate(get_type_info(kDictCodeFieldType), slot_id, std::to_string(dict_codes[0])));
    } else {
        std::vector<std::string> str_codes;
        str_codes.reserve(dict_codes.size());
        for (int code : dict_codes) {
            str_codes.emplace_back(std::to_string(code));
        }
        predicate = obj_pool.add(new_column_in_predicate(get_type_info(kDictCodeFieldType), slot_id, str_codes));
    }

    // deal with if NULL works or not.
    if (null_is_ok) {
        ColumnPredicate* result = nullptr;
        ColumnPredicate* is_null_pred =
                obj_pool.add(new_column_null_predicate(get_type_info(kDictCodeFieldType), slot_id, true));

        if (predicate != nullptr) {
            ColumnOrPredicate* or_pred =
                    obj_pool.add(new ColumnOrPredicate(get_type_info(kDictCodeFieldType), slot_id));
            or_pred->add_child(predicate);
            or_pred->add_child(is_null_pred);
            result = or_pred;
        } else {
            result = is_null_pred;
        }
        predicate = result;
    }

    return Status::OK();
}

class ListColumnReader : public ColumnReader {
public:
    explicit ListColumnReader(const ColumnReaderOptions& opts) {}
    ~ListColumnReader() override = default;

    Status init(const ParquetField* field, std::unique_ptr<ColumnReader> element_reader) {
        _field = field;
        _element_reader = std::move(element_reader);
        return Status::OK();
    }

    Status prepare_batch(size_t* num_records, Column* dst) override {
        NullableColumn* nullable_column = nullptr;
        ArrayColumn* array_column = nullptr;
        if (dst->is_nullable()) {
            nullable_column = down_cast<NullableColumn*>(dst);
            DCHECK(nullable_column->mutable_data_column()->is_array());
            array_column = down_cast<ArrayColumn*>(nullable_column->mutable_data_column());
        } else {
            DCHECK(dst->is_array());
            DCHECK(!_field->is_nullable);
            array_column = down_cast<ArrayColumn*>(dst);
        }
        auto* child_column = array_column->elements_column().get();
        auto st = _element_reader->prepare_batch(num_records, child_column);

        level_t* def_levels = nullptr;
        level_t* rep_levels = nullptr;
        size_t num_levels = 0;
        _element_reader->get_levels(&def_levels, &rep_levels, &num_levels);

        auto& offsets = array_column->offsets_column()->get_data();
        offsets.resize(num_levels + 1);
        NullColumn null_column(num_levels);
        auto& is_nulls = null_column.get_data();
        size_t num_offsets = 0;
        bool has_null = false;
        def_rep_to_offset(_field->level_info, def_levels, rep_levels, num_levels, &offsets[0], &is_nulls[0],
                          &num_offsets, &has_null);
        offsets.resize(num_offsets + 1);
        is_nulls.resize(num_offsets);

        if (dst->is_nullable()) {
            DCHECK(nullable_column != nullptr);
            nullable_column->mutable_null_column()->swap_column(null_column);
            nullable_column->set_has_null(has_null);
        }

        return st;
    }

    Status read_range(const Range<uint64_t>& range, const Filter* filter, Column* dst) override {
        NullableColumn* nullable_column = nullptr;
        ArrayColumn* array_column = nullptr;
        if (dst->is_nullable()) {
            nullable_column = down_cast<NullableColumn*>(dst);
            DCHECK(nullable_column->mutable_data_column()->is_array());
            array_column = down_cast<ArrayColumn*>(nullable_column->mutable_data_column());
        } else {
            DCHECK(dst->is_array());
            DCHECK(!_field->is_nullable);
            array_column = down_cast<ArrayColumn*>(dst);
        }
        auto* child_column = array_column->elements_column().get();
        RETURN_IF_ERROR(_element_reader->read_range(range, filter, child_column));

        level_t* def_levels = nullptr;
        level_t* rep_levels = nullptr;
        size_t num_levels = 0;
        _element_reader->get_levels(&def_levels, &rep_levels, &num_levels);

        auto& offsets = array_column->offsets_column()->get_data();
        offsets.resize(num_levels + 1);
        NullColumn null_column(num_levels);
        auto& is_nulls = null_column.get_data();
        size_t num_offsets = 0;
        bool has_null = false;
        def_rep_to_offset(_field->level_info, def_levels, rep_levels, num_levels, &offsets[0], &is_nulls[0],
                          &num_offsets, &has_null);
        offsets.resize(num_offsets + 1);
        is_nulls.resize(num_offsets);

        if (dst->is_nullable()) {
            DCHECK(nullable_column != nullptr);
            nullable_column->mutable_null_column()->swap_column(null_column);
            nullable_column->set_has_null(has_null);
        }

        return Status::OK();
    }

    Status finish_batch() override { return Status::OK(); }

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        _element_reader->get_levels(def_levels, rep_levels, num_levels);
    }

    void set_need_parse_levels(bool need_parse_levels) override {
        _element_reader->set_need_parse_levels(need_parse_levels);
    }

    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                 ColumnIOType type, bool active) override {
        _element_reader->collect_column_io_range(ranges, end_offset, type, active);
    }

    void select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) override {
        _element_reader->select_offset_index(range, rg_first_row);
    }

private:
    const ParquetField* _field = nullptr;
    std::unique_ptr<ColumnReader> _element_reader;
};

class MapColumnReader : public ColumnReader {
public:
    explicit MapColumnReader(const ColumnReaderOptions& opts) : _opts(opts) {}
    ~MapColumnReader() override = default;

    Status init(const ParquetField* field, std::unique_ptr<ColumnReader> key_reader,
                std::unique_ptr<ColumnReader> value_reader) {
        _field = field;
        _key_reader = std::move(key_reader);
        _value_reader = std::move(value_reader);

        // Check must has one valid column reader
        if (_key_reader == nullptr && _value_reader == nullptr) {
            return Status::InternalError("No avaliable parquet subfield column reader in MapColumn");
        }

        return Status::OK();
    }

    Status prepare_batch(size_t* num_records, Column* dst) override {
        NullableColumn* nullable_column = nullptr;
        MapColumn* map_column = nullptr;
        if (dst->is_nullable()) {
            nullable_column = down_cast<NullableColumn*>(dst);
            DCHECK(nullable_column->mutable_data_column()->is_map());
            map_column = down_cast<MapColumn*>(nullable_column->mutable_data_column());
        } else {
            DCHECK(dst->is_map());
            DCHECK(!_field->is_nullable);
            map_column = down_cast<MapColumn*>(dst);
        }
        auto* key_column = map_column->keys_column().get();
        auto* value_column = map_column->values_column().get();
        Status st;

        // TODO(SmithCruise) Ugly code, it's a temporary solution,
        //  to reset late materialization's rows_to_skip before read each subfield column
        size_t origin_next_row = _opts.context->next_row;
        size_t origin_rows_to_skip = _opts.context->rows_to_skip;

        if (_key_reader != nullptr) {
            st = _key_reader->prepare_batch(num_records, key_column);
            if (!st.ok() && !st.is_end_of_file()) {
                return st;
            }
        }

        if (_value_reader != nullptr) {
            // do reset
            _opts.context->next_row = origin_next_row;
            _opts.context->rows_to_skip = origin_rows_to_skip;

            st = _value_reader->prepare_batch(num_records, value_column);
            if (!st.ok() && !st.is_end_of_file()) {
                return st;
            }
        }

        // if neither key_reader not value_reader is nullptr , check the value_column size is the same with key_column
        DCHECK((_key_reader == nullptr) || (_value_reader == nullptr) || (value_column->size() == key_column->size()));

        level_t* def_levels = nullptr;
        level_t* rep_levels = nullptr;
        size_t num_levels = 0;

        if (_key_reader != nullptr) {
            _key_reader->get_levels(&def_levels, &rep_levels, &num_levels);
        } else if (_value_reader != nullptr) {
            _value_reader->get_levels(&def_levels, &rep_levels, &num_levels);
        } else {
            DCHECK(false) << "Unreachable!";
        }

        auto& offsets = map_column->offsets_column()->get_data();
        offsets.resize(num_levels + 1);
        NullColumn null_column(num_levels);
        auto& is_nulls = null_column.get_data();
        size_t num_offsets = 0;
        bool has_null = false;

        // ParquetFiled Map -> Map<Struct<key,value>>
        def_rep_to_offset(_field->level_info, def_levels, rep_levels, num_levels, &offsets[0], &is_nulls[0],
                          &num_offsets, &has_null);
        offsets.resize(num_offsets + 1);
        is_nulls.resize(num_offsets);

        // fill with default
        if (_key_reader == nullptr) {
            key_column->append_default(offsets.back());
        }
        if (_value_reader == nullptr) {
            value_column->append_default(offsets.back());
        }

        if (dst->is_nullable()) {
            DCHECK(nullable_column != nullptr);
            nullable_column->mutable_null_column()->swap_column(null_column);
            nullable_column->set_has_null(has_null);
        }

        return st;
    }

    Status read_range(const Range<uint64_t>& range, const Filter* filter, Column* dst) override {
        NullableColumn* nullable_column = nullptr;
        MapColumn* map_column = nullptr;
        if (dst->is_nullable()) {
            nullable_column = down_cast<NullableColumn*>(dst);
            DCHECK(nullable_column->mutable_data_column()->is_map());
            map_column = down_cast<MapColumn*>(nullable_column->mutable_data_column());
        } else {
            DCHECK(dst->is_map());
            DCHECK(!_field->is_nullable);
            map_column = down_cast<MapColumn*>(dst);
        }
        auto* key_column = map_column->keys_column().get();
        auto* value_column = map_column->values_column().get();
        if (_key_reader != nullptr) {
            RETURN_IF_ERROR(_key_reader->read_range(range, filter, key_column));
        }

        if (_value_reader != nullptr) {
            RETURN_IF_ERROR(_value_reader->read_range(range, filter, value_column));
        }

        // if neither key_reader not value_reader is nullptr , check the value_column size is the same with key_column
        DCHECK((_key_reader == nullptr) || (_value_reader == nullptr) || (value_column->size() == key_column->size()));

        level_t* def_levels = nullptr;
        level_t* rep_levels = nullptr;
        size_t num_levels = 0;

        if (_key_reader != nullptr) {
            _key_reader->get_levels(&def_levels, &rep_levels, &num_levels);
        } else if (_value_reader != nullptr) {
            _value_reader->get_levels(&def_levels, &rep_levels, &num_levels);
        } else {
            DCHECK(false) << "Unreachable!";
        }

        auto& offsets = map_column->offsets_column()->get_data();
        offsets.resize(num_levels + 1);
        NullColumn null_column(num_levels);
        auto& is_nulls = null_column.get_data();
        size_t num_offsets = 0;
        bool has_null = false;

        // ParquetFiled Map -> Map<Struct<key,value>>
        def_rep_to_offset(_field->level_info, def_levels, rep_levels, num_levels, &offsets[0], &is_nulls[0],
                          &num_offsets, &has_null);
        offsets.resize(num_offsets + 1);
        is_nulls.resize(num_offsets);

        // fill with default
        if (_key_reader == nullptr) {
            key_column->append_default(offsets.back());
        }
        if (_value_reader == nullptr) {
            value_column->append_default(offsets.back());
        }

        if (dst->is_nullable()) {
            DCHECK(nullable_column != nullptr);
            nullable_column->mutable_null_column()->swap_column(null_column);
            nullable_column->set_has_null(has_null);
        }

        return Status::OK();
    }

    Status finish_batch() override { return Status::OK(); }

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        // check _value_reader
        if (_key_reader != nullptr) {
            _key_reader->get_levels(def_levels, rep_levels, num_levels);
        } else if (_value_reader != nullptr) {
            _value_reader->get_levels(def_levels, rep_levels, num_levels);
        } else {
            DCHECK(false) << "Unreachable!";
        }
    }

    void set_need_parse_levels(bool need_parse_levels) override {
        if (_key_reader != nullptr) {
            _key_reader->set_need_parse_levels(need_parse_levels);
        }

        if (_value_reader != nullptr) {
            _value_reader->set_need_parse_levels(need_parse_levels);
        }
    }

    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                 ColumnIOType type, bool active) override {
        if (_key_reader != nullptr) {
            _key_reader->collect_column_io_range(ranges, end_offset, type, active);
        }
        if (_value_reader != nullptr) {
            _value_reader->collect_column_io_range(ranges, end_offset, type, active);
        }
    }

    void select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) override {
        if (_key_reader != nullptr) {
            _key_reader->select_offset_index(range, rg_first_row);
        }
        if (_value_reader != nullptr) {
            _value_reader->select_offset_index(range, rg_first_row);
        }
    }

private:
    const ParquetField* _field = nullptr;
    std::unique_ptr<ColumnReader> _key_reader;
    std::unique_ptr<ColumnReader> _value_reader;
    const ColumnReaderOptions& _opts;
};

class StructColumnReader : public ColumnReader {
public:
    explicit StructColumnReader(const ColumnReaderOptions& opts) : _opts(opts) {}
    ~StructColumnReader() override = default;

    Status init(const ParquetField* field, std::map<std::string, std::unique_ptr<ColumnReader>>&& child_readers) {
        _field = field;
        _child_readers = std::move(child_readers);

        if (_child_readers.empty()) {
            return Status::InternalError("No avaliable parquet subfield column reader in StructColumn");
        }

        for (const auto& pair : _child_readers) {
            if (pair.second != nullptr) {
                _def_rep_level_child_reader = &(pair.second);
                return Status::OK();
            }
        }

        return Status::InternalError("No existed parquet subfield column reader in StructColumn");
    }

    Status prepare_batch(size_t* num_records, Column* dst) override {
        NullableColumn* nullable_column = nullptr;
        StructColumn* struct_column = nullptr;
        if (dst->is_nullable()) {
            nullable_column = down_cast<NullableColumn*>(dst);
            DCHECK(nullable_column->mutable_data_column()->is_struct());
            struct_column = down_cast<StructColumn*>(nullable_column->mutable_data_column());
        } else {
            DCHECK(dst->is_struct());
            DCHECK(!_field->is_nullable);
            struct_column = down_cast<StructColumn*>(dst);
        }

        const auto& field_names = struct_column->field_names();

        DCHECK_EQ(field_names.size(), _child_readers.size());

        // TODO(SmithCruise) Ugly code, it's a temporary solution,
        //  to reset late materialization's rows_to_skip before read each subfield column
        size_t origin_next_row = _opts.context->next_row;
        size_t origin_rows_to_skip = _opts.context->rows_to_skip;

        // Fill data for subfield column reader
        bool first_read = true;
        size_t real_read = 0;
        for (size_t i = 0; i < field_names.size(); i++) {
            const auto& field_name = field_names[i];
            if (LIKELY(_child_readers.find(field_name) != _child_readers.end())) {
                if (_child_readers[field_name] != nullptr) {
                    Column* child_column = struct_column->field_column(field_name).get();
                    _opts.context->next_row = origin_next_row;
                    _opts.context->rows_to_skip = origin_rows_to_skip;
                    RETURN_IF_ERROR(_child_readers[field_name]->prepare_batch(num_records, child_column));
                    size_t current_real_read = child_column->size();
                    real_read = first_read ? current_real_read : real_read;
                    first_read = false;
                    if (UNLIKELY(real_read != current_real_read)) {
                        return Status::InternalError(strings::Substitute("Unmatched row count, $0", field_name));
                    }
                }
            } else {
                return Status::InternalError(
                        strings::Substitute("there is no match subfield reader for $1", field_name));
            }
        }

        if (UNLIKELY(first_read)) {
            return Status::InternalError(
                    strings::Substitute("All used subfield of struct type $1 is not exist", _field->name));
        }

        for (size_t i = 0; i < field_names.size(); i++) {
            const auto& field_name = field_names[i];
            if (_child_readers[field_name] == nullptr) {
                Column* child_column = struct_column->field_column(field_name).get();
                child_column->append_default(real_read);
            }
        }

        if (dst->is_nullable()) {
            DCHECK(nullable_column != nullptr);
            size_t row_nums = struct_column->fields_column()[0]->size();
            NullColumn null_column(row_nums, 0);
            auto& is_nulls = null_column.get_data();
            bool has_null = false;
            _handle_null_rows(is_nulls.data(), &has_null, row_nums);

            nullable_column->mutable_null_column()->swap_column(null_column);
            nullable_column->set_has_null(has_null);
        }
        return Status::OK();
    }

    Status read_range(const Range<uint64_t>& range, const Filter* filter, Column* dst) override {
        NullableColumn* nullable_column = nullptr;
        StructColumn* struct_column = nullptr;
        if (dst->is_nullable()) {
            nullable_column = down_cast<NullableColumn*>(dst);
            DCHECK(nullable_column->mutable_data_column()->is_struct());
            struct_column = down_cast<StructColumn*>(nullable_column->mutable_data_column());
        } else {
            DCHECK(dst->is_struct());
            DCHECK(!_field->is_nullable);
            struct_column = down_cast<StructColumn*>(dst);
        }

        const auto& field_names = struct_column->field_names();

        DCHECK_EQ(field_names.size(), _child_readers.size());

        // Fill data for subfield column reader
        size_t real_read = 0;
        bool first_read = true;
        for (size_t i = 0; i < field_names.size(); i++) {
            const auto& field_name = field_names[i];
            if (LIKELY(_child_readers.find(field_name) != _child_readers.end())) {
                if (_child_readers[field_name] != nullptr) {
                    Column* child_column = struct_column->field_column(field_name).get();
                    RETURN_IF_ERROR(_child_readers[field_name]->read_range(range, filter, child_column));
                    real_read = child_column->size();
                    first_read = false;
                }
            } else {
                return Status::InternalError(
                        strings::Substitute("there is no match subfield reader for $1", field_name));
            }
        }

        if (UNLIKELY(first_read)) {
            return Status::InternalError(
                    strings::Substitute("All used subfield of struct type $1 is not exist", _field->name));
        }

        for (size_t i = 0; i < field_names.size(); i++) {
            const auto& field_name = field_names[i];
            if (_child_readers[field_name] == nullptr) {
                Column* child_column = struct_column->field_column(field_name).get();
                child_column->append_default(real_read);
            }
        }

        if (dst->is_nullable()) {
            DCHECK(nullable_column != nullptr);
            size_t row_nums = struct_column->fields_column()[0]->size();
            NullColumn null_column(row_nums, 0);
            auto& is_nulls = null_column.get_data();
            bool has_null = false;
            _handle_null_rows(is_nulls.data(), &has_null, row_nums);

            nullable_column->mutable_null_column()->swap_column(null_column);
            nullable_column->set_has_null(has_null);
        }
        return Status::OK();
    }

    Status finish_batch() override { return Status::OK(); }

    // get_levels functions only called by complex type
    // If parent is a struct type, only def_levels has value.
    // If parent is list or map type, def_levels & rep_levels both have value.
    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        for (const auto& pair : _child_readers) {
            // Considering not existed subfield, we will not create its ColumnReader
            // So we should pick up the first existed subfield column reader
            if (pair.second != nullptr) {
                pair.second->get_levels(def_levels, rep_levels, num_levels);
                return;
            }
        }
    }

    void set_need_parse_levels(bool need_parse_levels) override {
        for (const auto& pair : _child_readers) {
            if (pair.second != nullptr) {
                pair.second->set_need_parse_levels(need_parse_levels);
            }
        }
    }

    bool try_to_use_dict_filter(ExprContext* ctx, bool is_decode_needed, const SlotId slotId,
                                const std::vector<std::string>& sub_field_path, const size_t& layer) override {
        if (sub_field_path.size() <= layer) {
            return false;
        }
        const std::string& sub_field = sub_field_path[layer];
        if (_child_readers.find(sub_field) == _child_readers.end()) {
            return false;
        }

        if (_child_readers[sub_field] == nullptr) {
            return false;
        }
        return _child_readers[sub_field]->try_to_use_dict_filter(ctx, is_decode_needed, slotId, sub_field_path,
                                                                 layer + 1);
    }

    Status rewrite_conjunct_ctxs_to_predicate(bool* is_group_filtered, const std::vector<std::string>& sub_field_path,
                                              const size_t& layer) override {
        const std::string& sub_field = sub_field_path[layer];
        return _child_readers[sub_field]->rewrite_conjunct_ctxs_to_predicate(is_group_filtered, sub_field_path,
                                                                             layer + 1);
    }

    void init_dict_column(ColumnPtr& column, const std::vector<std::string>& sub_field_path,
                          const size_t& layer) override {
        const std::string& sub_field = sub_field_path[layer];
        StructColumn* struct_column = nullptr;
        if (column->is_nullable()) {
            NullableColumn* nullable_column = down_cast<NullableColumn*>(column.get());
            DCHECK(nullable_column->mutable_data_column()->is_struct());
            struct_column = down_cast<StructColumn*>(nullable_column->mutable_data_column());
        } else {
            DCHECK(column->is_struct());
            DCHECK(!_field->is_nullable);
            struct_column = down_cast<StructColumn*>(column.get());
        }
        return _child_readers[sub_field]->init_dict_column(struct_column->field_column(sub_field), sub_field_path,
                                                           layer + 1);
    }

    Status filter_dict_column(const ColumnPtr& column, Filter* filter, const std::vector<std::string>& sub_field_path,
                              const size_t& layer) override {
        const std::string& sub_field = sub_field_path[layer];
        StructColumn* struct_column = nullptr;
        if (column->is_nullable()) {
            NullableColumn* nullable_column = down_cast<NullableColumn*>(column.get());
            DCHECK(nullable_column->mutable_data_column()->is_struct());
            struct_column = down_cast<StructColumn*>(nullable_column->mutable_data_column());
        } else {
            DCHECK(column->is_struct());
            DCHECK(!_field->is_nullable);
            struct_column = down_cast<StructColumn*>(column.get());
        }
        return _child_readers[sub_field]->filter_dict_column(struct_column->field_column(sub_field), filter,
                                                             sub_field_path, layer + 1);
    }

    Status fill_dst_column(ColumnPtr& dst, const ColumnPtr& src) override {
        StructColumn* struct_column_src = nullptr;
        StructColumn* struct_column_dst = nullptr;
        if (src->is_nullable()) {
            NullableColumn* nullable_column_src = down_cast<NullableColumn*>(src.get());
            DCHECK(nullable_column_src->mutable_data_column()->is_struct());
            struct_column_src = down_cast<StructColumn*>(nullable_column_src->mutable_data_column());
            NullColumn* null_column_src = nullable_column_src->mutable_null_column();
            NullableColumn* nullable_column_dst = down_cast<NullableColumn*>(dst.get());
            DCHECK(nullable_column_dst->mutable_data_column()->is_struct());
            struct_column_dst = down_cast<StructColumn*>(nullable_column_dst->mutable_data_column());
            NullColumn* null_column_dst = nullable_column_dst->mutable_null_column();
            null_column_dst->swap_column(*null_column_src);
            nullable_column_src->update_has_null();
            nullable_column_dst->update_has_null();
        } else {
            DCHECK(src->is_struct());
            DCHECK(dst->is_struct());
            DCHECK(!_field->is_nullable);
            struct_column_src = down_cast<StructColumn*>(src.get());
            struct_column_dst = down_cast<StructColumn*>(dst.get());
        }
        const auto& field_names = struct_column_dst->field_names();
        for (size_t i = 0; i < field_names.size(); i++) {
            const auto& field_name = field_names[i];
            if (LIKELY(_child_readers.find(field_name) != _child_readers.end())) {
                if (_child_readers[field_name] == nullptr) {
                    struct_column_dst->field_column(field_name)
                            ->swap_column(*(struct_column_src->field_column(field_name)));
                } else {
                    RETURN_IF_ERROR(_child_readers[field_name]->fill_dst_column(
                            struct_column_dst->field_column(field_name), struct_column_src->field_column(field_name)));
                }
            } else {
                return Status::InternalError(
                        strings::Substitute("there is no match subfield reader for $1", field_name));
            }
        }
        return Status::OK();
    }

    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                 ColumnIOType type, bool active) override {
        for (const auto& pair : _child_readers) {
            if (pair.second != nullptr) {
                pair.second->collect_column_io_range(ranges, end_offset, type, active);
            }
        }
    }

    void select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) override {
        for (const auto& pair : _child_readers) {
            if (pair.second != nullptr) {
                pair.second->select_offset_index(range, rg_first_row);
            }
        }
    }

private:
    void _handle_null_rows(uint8_t* is_nulls, bool* has_null, size_t num_rows) {
        level_t* def_levels = nullptr;
        level_t* rep_levels = nullptr;
        size_t num_levels = 0;
        (*_def_rep_level_child_reader)->get_levels(&def_levels, &rep_levels, &num_levels);

        if (def_levels == nullptr) {
            // If subfields are required, def_levels is nullptr
            *has_null = false;
            return;
        }

        LevelInfo level_info = _field->level_info;

        if (rep_levels != nullptr) {
            // It's a RepeatedStoredColumnReader
            size_t rows = 0;
            for (size_t i = 0; i < num_levels; i++) {
                if (def_levels[i] < level_info.immediate_repeated_ancestor_def_level ||
                    rep_levels[i] > level_info.max_rep_level) {
                    continue;
                }

                // Start for a new row
                if (def_levels[i] >= level_info.max_def_level) {
                    is_nulls[rows] = 0;
                } else {
                    is_nulls[rows] = 1;
                    *has_null = true;
                }
                rows++;
            }
            DCHECK_EQ(num_rows, rows);
        } else {
            // For OptionalStoredColumnReader, num_levels is equal to num_rows
            DCHECK(num_rows == num_levels);
            for (size_t i = 0; i < num_levels; i++) {
                if (def_levels[i] >= level_info.max_def_level) {
                    is_nulls[i] = 0;
                } else {
                    is_nulls[i] = 1;
                    *has_null = true;
                }
            }
        }
    }

    // _field is generated by parquet format, so it's child order may different from _child_readers.
    const ParquetField* _field = nullptr;
    // _children_readers order is the same as TypeDescriptor children order.
    std::map<std::string, std::unique_ptr<ColumnReader>> _child_readers;
    // First non-nullptr child ColumnReader, used to get def & rep levels
    const std::unique_ptr<ColumnReader>* _def_rep_level_child_reader = nullptr;
    const ColumnReaderOptions& _opts;
};

void ColumnReader::get_subfield_pos_with_pruned_type(const ParquetField& field, const TypeDescriptor& col_type,
                                                     bool case_sensitive, std::vector<int32_t>& pos) {
    DCHECK(field.type.type == LogicalType::TYPE_STRUCT);

    // build tmp mapping for ParquetField
    std::unordered_map<std::string, size_t> field_name_2_pos;
    for (size_t i = 0; i < field.children.size(); i++) {
        const std::string format_field_name =
                case_sensitive ? field.children[i].name : boost::algorithm::to_lower_copy(field.children[i].name);
        field_name_2_pos.emplace(format_field_name, i);
    }

    for (size_t i = 0; i < col_type.children.size(); i++) {
        const std::string formatted_subfield_name =
                case_sensitive ? col_type.field_names[i] : boost::algorithm::to_lower_copy(col_type.field_names[i]);

        auto it = field_name_2_pos.find(formatted_subfield_name);
        if (it == field_name_2_pos.end()) {
            LOG(WARNING) << "Struct subfield name: " + formatted_subfield_name + " not found.";
            pos[i] = -1;
            continue;
        }
        pos[i] = it->second;
    }
}

void ColumnReader::get_subfield_pos_with_pruned_type(const ParquetField& field, const TypeDescriptor& col_type,
                                                     bool case_sensitive,
                                                     const TIcebergSchemaField* iceberg_schema_field,
                                                     std::vector<int32_t>& pos,
                                                     std::vector<const TIcebergSchemaField*>& iceberg_schema_subfield) {
    // For Struct type with schema change, we need consider subfield not existed suitition.
    // When Iceberg add a new struct subfield, the original parquet file do not contains newly added subfield,
    std::unordered_map<std::string, const TIcebergSchemaField*> subfield_name_2_field_schema{};
    for (const auto& each : iceberg_schema_field->children) {
        std::string format_subfield_name = case_sensitive ? each.name : boost::algorithm::to_lower_copy(each.name);
        subfield_name_2_field_schema.emplace(format_subfield_name, &each);
    }

    std::unordered_map<int32_t, size_t> field_id_2_pos{};
    for (size_t i = 0; i < field.children.size(); i++) {
        field_id_2_pos.emplace(field.children[i].field_id, i);
    }
    for (size_t i = 0; i < col_type.children.size(); i++) {
        const auto& format_subfield_name =
                case_sensitive ? col_type.field_names[i] : boost::algorithm::to_lower_copy(col_type.field_names[i]);

        auto iceberg_it = subfield_name_2_field_schema.find(format_subfield_name);
        if (iceberg_it == subfield_name_2_field_schema.end()) {
            // This suitition should not be happened, means table's struct subfield not existed in iceberg schema
            // Below code is defensive
            DCHECK(false) << "Struct subfield name: " + format_subfield_name + " not found in iceberg schema.";
            pos[i] = -1;
            iceberg_schema_subfield[i] = nullptr;
            continue;
        }

        int32_t field_id = iceberg_it->second->field_id;

        auto parquet_field_it = field_id_2_pos.find(field_id);
        if (parquet_field_it == field_id_2_pos.end()) {
            // Means newly added struct subfield not existed in original parquet file, we put nullptr
            // column reader in children_reader, we will append default value for this subfield later.
            LOG(INFO) << "Struct subfield name: " + format_subfield_name + " not found in ParquetField.";
            pos[i] = -1;
            iceberg_schema_subfield[i] = nullptr;
            continue;
        }

        pos[i] = parquet_field_it->second;
        iceberg_schema_subfield[i] = iceberg_it->second;
    }
}

Status ColumnReader::create(const ColumnReaderOptions& opts, const ParquetField* field, const TypeDescriptor& col_type,
                            std::unique_ptr<ColumnReader>* output) {
    // We will only set a complex type in ParquetField
    if ((field->type.is_complex_type() || col_type.is_complex_type()) && (field->type.type != col_type.type)) {
        return Status::InternalError(strings::Substitute("ParquetField's type $0 is different from table's type $1",
                                                         field->type.type, col_type.type));
    }
    if (field->type.type == LogicalType::TYPE_ARRAY) {
        std::unique_ptr<ColumnReader> child_reader;
        RETURN_IF_ERROR(ColumnReader::create(opts, &field->children[0], col_type.children[0], &child_reader));
        std::unique_ptr<ListColumnReader> reader(new ListColumnReader(opts));
        RETURN_IF_ERROR(reader->init(field, std::move(child_reader)));
        *output = std::move(reader);
    } else if (field->type.type == LogicalType::TYPE_MAP) {
        std::unique_ptr<ColumnReader> key_reader = nullptr;
        std::unique_ptr<ColumnReader> value_reader = nullptr;

        if (!col_type.children[0].is_unknown_type()) {
            RETURN_IF_ERROR(ColumnReader::create(opts, &(field->children[0]), col_type.children[0], &key_reader));
        }
        if (!col_type.children[1].is_unknown_type()) {
            RETURN_IF_ERROR(ColumnReader::create(opts, &(field->children[1]), col_type.children[1], &value_reader));
        }

        std::unique_ptr<MapColumnReader> reader(new MapColumnReader(opts));
        RETURN_IF_ERROR(reader->init(field, std::move(key_reader), std::move(value_reader)));
        *output = std::move(reader);
    } else if (field->type.type == LogicalType::TYPE_STRUCT) {
        std::vector<int32_t> subfield_pos(col_type.children.size());
        get_subfield_pos_with_pruned_type(*field, col_type, opts.case_sensitive, subfield_pos);

        std::map<std::string, std::unique_ptr<ColumnReader>> children_readers;
        for (size_t i = 0; i < col_type.children.size(); i++) {
            if (subfield_pos[i] == -1) {
                // -1 means subfield not existed, we need to emplace nullptr
                children_readers.emplace(col_type.field_names[i], nullptr);
                continue;
            }
            std::unique_ptr<ColumnReader> child_reader;
            RETURN_IF_ERROR(
                    ColumnReader::create(opts, &field->children[subfield_pos[i]], col_type.children[i], &child_reader));
            children_readers.emplace(col_type.field_names[i], std::move(child_reader));
        }

        std::unique_ptr<StructColumnReader> reader(new StructColumnReader(opts));
        RETURN_IF_ERROR(reader->init(field, std::move(children_readers)));
        *output = std::move(reader);
        return Status::OK();
    } else {
        std::unique_ptr<ScalarColumnReader> reader(new ScalarColumnReader(opts));
        RETURN_IF_ERROR(reader->init(field, col_type, &opts.row_group_meta->columns[field->physical_column_index]));
        *output = std::move(reader);
    }
    return Status::OK();
}

Status ColumnReader::create(const ColumnReaderOptions& opts, const ParquetField* field, const TypeDescriptor& col_type,
                            const TIcebergSchemaField* iceberg_schema_field, std::unique_ptr<ColumnReader>* output) {
    // We will only set a complex type in ParquetField
    if ((field->type.is_complex_type() || col_type.is_complex_type()) && (field->type.type != col_type.type)) {
        return Status::InternalError(strings::Substitute("ParquetField's type $0 is different from table's type $1",
                                                         field->type.type, col_type.type));
    }
    DCHECK(iceberg_schema_field != nullptr);
    if (field->type.type == LogicalType::TYPE_ARRAY) {
        std::unique_ptr<ColumnReader> child_reader;
        const TIcebergSchemaField* element_schema = &iceberg_schema_field->children[0];
        RETURN_IF_ERROR(
                ColumnReader::create(opts, &field->children[0], col_type.children[0], element_schema, &child_reader));
        std::unique_ptr<ListColumnReader> reader(new ListColumnReader(opts));
        RETURN_IF_ERROR(reader->init(field, std::move(child_reader)));
        *output = std::move(reader);
    } else if (field->type.type == LogicalType::TYPE_MAP) {
        std::unique_ptr<ColumnReader> key_reader = nullptr;
        std::unique_ptr<ColumnReader> value_reader = nullptr;

        const TIcebergSchemaField* key_iceberg_schema = &iceberg_schema_field->children[0];
        const TIcebergSchemaField* value_iceberg_schema = &iceberg_schema_field->children[1];

        if (!col_type.children[0].is_unknown_type()) {
            RETURN_IF_ERROR(ColumnReader::create(opts, &(field->children[0]), col_type.children[0], key_iceberg_schema,
                                                 &key_reader));
        }
        if (!col_type.children[1].is_unknown_type()) {
            RETURN_IF_ERROR(ColumnReader::create(opts, &(field->children[1]), col_type.children[1],
                                                 value_iceberg_schema, &value_reader));
        }

        std::unique_ptr<MapColumnReader> reader(new MapColumnReader(opts));
        RETURN_IF_ERROR(reader->init(field, std::move(key_reader), std::move(value_reader)));
        *output = std::move(reader);
    } else if (field->type.type == LogicalType::TYPE_STRUCT) {
        std::vector<int32_t> subfield_pos(col_type.children.size());
        std::vector<const TIcebergSchemaField*> iceberg_schema_subfield(col_type.children.size());
        get_subfield_pos_with_pruned_type(*field, col_type, opts.case_sensitive, iceberg_schema_field, subfield_pos,
                                          iceberg_schema_subfield);

        std::map<std::string, std::unique_ptr<ColumnReader>> children_readers;
        for (size_t i = 0; i < col_type.children.size(); i++) {
            if (subfield_pos[i] == -1) {
                // -1 means subfield not existed, we need to emplace nullptr
                children_readers.emplace(col_type.field_names[i], nullptr);
                continue;
            }

            std::unique_ptr<ColumnReader> child_reader;
            RETURN_IF_ERROR(ColumnReader::create(opts, &field->children[subfield_pos[i]], col_type.children[i],
                                                 iceberg_schema_subfield[i], &child_reader));
            children_readers.emplace(col_type.field_names[i], std::move(child_reader));
        }

        std::unique_ptr<StructColumnReader> reader(new StructColumnReader(opts));
        RETURN_IF_ERROR(reader->init(field, std::move(children_readers)));
        *output = std::move(reader);
        return Status::OK();
    } else {
        std::unique_ptr<ScalarColumnReader> reader(new ScalarColumnReader(opts));
        RETURN_IF_ERROR(reader->init(field, col_type, &opts.row_group_meta->columns[field->physical_column_index]));
        *output = std::move(reader);
    }
    return Status::OK();
}

} // namespace starrocks::parquet
