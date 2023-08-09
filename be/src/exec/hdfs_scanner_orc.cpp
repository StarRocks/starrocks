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

#include "exec/hdfs_scanner_orc.h"

#include <utility>

#include "exec/exec_node.h"
#include "exec/iceberg/iceberg_delete_builder.h"
#include "formats/orc/orc_chunk_reader.h"
#include "formats/orc/orc_input_stream.h"
#include "formats/orc/orc_memory_pool.h"
#include "formats/orc/orc_min_max_decoder.h"
#include "formats/orc/utils.h"
#include "gen_cpp/orc_proto.pb.h"
#include "simd/simd.h"
#include "storage/chunk_helper.h"
#include "util/runtime_profile.h"
#include "util/timezone_utils.h"

namespace starrocks {

class OrcRowReaderFilter : public orc::RowReaderFilter {
public:
    OrcRowReaderFilter(const HdfsScannerContext& scanner_ctx, OrcChunkReader* reader);
    bool filterOnOpeningStripe(uint64_t stripeIndex, const orc::proto::StripeInformation* stripeInformation) override;
    bool filterOnPickRowGroup(size_t rowGroupIdx, const std::unordered_map<uint64_t, orc::proto::RowIndex>& rowIndexes,
                              const std::map<uint32_t, orc::BloomFilterIndex>& bloomFilters) override;
    bool filterMinMax(size_t rowGroupIdx, const std::unordered_map<uint64_t, orc::proto::RowIndex>& rowIndexes,
                      const std::map<uint32_t, orc::BloomFilterIndex>& bloomFilter);
    bool filterOnPickStringDictionary(const std::unordered_map<uint64_t, orc::StringDictionary*>& sdicts) override;

    bool is_slot_evaluated(SlotId id) { return _dict_filter_eval_cache.find(id) != _dict_filter_eval_cache.end(); }
    void onStartingPickRowGroups() override;
    void onEndingPickRowGroups() override;
    void setWriterTimezone(const std::string& tz) override;

private:
    const HdfsScannerContext& _scanner_ctx;
    uint64_t _current_stripe_index{0};
    bool _init_use_dict_filter_slots{false};
    std::vector<pair<SlotDescriptor*, uint64_t>> _use_dict_filter_slots;
    friend class HdfsOrcScanner;
    std::unordered_map<SlotId, FilterPtr> _dict_filter_eval_cache;
    bool _can_do_filter_on_orc_cvb{true}; // cvb: column vector batch.
    // key: end of range.
    // value: start of range.
    // ranges are not overlapped.
    // check `offset` in a range or not:
    // 1. use `upper_bound` to find the first range.end > `offset`
    // 2. and check if range.start <= `offset`
    std::map<uint64_t, uint64_t> _scan_ranges;
    OrcChunkReader* _reader;
    int64_t _writer_tzoffset_in_seconds;
};

void OrcRowReaderFilter::onStartingPickRowGroups() {}

void OrcRowReaderFilter::onEndingPickRowGroups() {}

void OrcRowReaderFilter::setWriterTimezone(const std::string& tz) {
    cctz::time_zone writer_tzinfo;
    if (tz == "" || !TimezoneUtils::find_cctz_time_zone(tz, writer_tzinfo)) {
        _writer_tzoffset_in_seconds = _reader->tzoffset_in_seconds();
    } else {
        _writer_tzoffset_in_seconds = TimezoneUtils::to_utc_offset(writer_tzinfo);
    }
}

OrcRowReaderFilter::OrcRowReaderFilter(const HdfsScannerContext& scanner_ctx, OrcChunkReader* reader)
        : _scanner_ctx(scanner_ctx), _reader(reader), _writer_tzoffset_in_seconds(reader->tzoffset_in_seconds()) {
    if (_scanner_ctx.min_max_tuple_desc != nullptr) {
        VLOG_FILE << "OrcRowReaderFilter: min_max_tuple_desc = " << _scanner_ctx.min_max_tuple_desc->debug_string();
        for (ExprContext* ctx : _scanner_ctx.min_max_conjunct_ctxs) {
            VLOG_FILE << "OrcRowReaderFilter: min_max_ctx = " << ctx->root()->debug_string();
        }
    }
    for (const auto& r : _scanner_ctx.scan_ranges) {
        _scan_ranges.insert(std::make_pair(r->offset + r->length, r->offset));
    }
}

bool OrcRowReaderFilter::filterOnOpeningStripe(uint64_t stripeIndex,
                                               const orc::proto::StripeInformation* stripeInformation) {
    _current_stripe_index = stripeIndex;
    uint64_t offset = stripeInformation->offset();
    // range end must > offset
    auto it = _scan_ranges.upper_bound(offset);
    if ((it != _scan_ranges.end()) && (offset >= it->second) && (offset < it->first)) {
        return false;
    }
    return true;
}

bool OrcRowReaderFilter::filterMinMax(size_t rowGroupIdx,
                                      const std::unordered_map<uint64_t, orc::proto::RowIndex>& rowIndexes,
                                      const std::map<uint32_t, orc::BloomFilterIndex>& bloomFilter) {
    const TupleDescriptor* min_max_tuple_desc = _scanner_ctx.min_max_tuple_desc;
    ChunkPtr min_chunk = ChunkHelper::new_chunk(*min_max_tuple_desc, 0);
    ChunkPtr max_chunk = ChunkHelper::new_chunk(*min_max_tuple_desc, 0);
    for (size_t i = 0; i < min_max_tuple_desc->slots().size(); i++) {
        SlotDescriptor* slot = min_max_tuple_desc->slots()[i];
        int32_t column_index = _reader->get_column_id_by_slot_name(slot->col_name());
        if (column_index >= 0) {
            auto row_idx_iter = rowIndexes.find(column_index);
            // there is no column stats, skip filter process.
            if (row_idx_iter == rowIndexes.end()) {
                return false;
            }
            const orc::proto::ColumnStatistics& stats = row_idx_iter->second.entry(rowGroupIdx).statistics();
            ColumnPtr min_col = min_chunk->columns()[i];
            ColumnPtr max_col = max_chunk->columns()[i];
            DCHECK(!min_col->is_constant() && !max_col->is_constant());
            int64_t tz_offset_in_seconds = _reader->tzoffset_in_seconds() - _writer_tzoffset_in_seconds;
            Status st = OrcMinMaxDecoder::decode(slot, stats, min_col, max_col, tz_offset_in_seconds);
            if (!st.ok()) {
                return false;
            }
        } else {
            // search partition columns.
            int part_idx = 0;
            const int part_size = _scanner_ctx.partition_columns.size();
            for (part_idx = 0; part_idx < part_size; part_idx++) {
                if (_scanner_ctx.partition_columns[part_idx].col_name == slot->col_name()) {
                    break;
                }
            }
            // not found in partition columns.
            if (part_idx == part_size) {
                min_chunk->columns()[i]->append_nulls(1);
                max_chunk->columns()[i]->append_nulls(1);
            } else {
                auto* const_column = ColumnHelper::as_raw_column<ConstColumn>(_scanner_ctx.partition_values[part_idx]);
                min_chunk->columns()[i]->append(*const_column->data_column(), 0, 1);
                max_chunk->columns()[i]->append(*const_column->data_column(), 0, 1);
            }
        }
    }

    VLOG_FILE << "stripe = " << _current_stripe_index << ", row_group = " << rowGroupIdx
              << ", min_chunk = " << min_chunk->debug_row(0) << ", max_chunk = " << max_chunk->debug_row(0);
    for (auto& min_max_conjunct_ctx : _scanner_ctx.min_max_conjunct_ctxs) {
        // TODO: add a warning log here
        auto min_col = EVALUATE_NULL_IF_ERROR(min_max_conjunct_ctx, min_max_conjunct_ctx->root(), min_chunk.get());
        auto max_col = EVALUATE_NULL_IF_ERROR(min_max_conjunct_ctx, min_max_conjunct_ctx->root(), max_chunk.get());
        auto min = min_col->get(0).get_int8();
        auto max = max_col->get(0).get_int8();
        if (min == 0 && max == 0) {
            // Means this row group dont stastisfy min-max predicates, we can filter this row group.
            return true;
        }
    }
    return false;
}
bool OrcRowReaderFilter::filterOnPickRowGroup(size_t rowGroupIdx,
                                              const std::unordered_map<uint64_t, orc::proto::RowIndex>& rowIndexes,
                                              const std::map<uint32_t, orc::BloomFilterIndex>& bloomFilters) {
    if (_scanner_ctx.min_max_tuple_desc != nullptr) {
        if (filterMinMax(rowGroupIdx, rowIndexes, bloomFilters)) {
            VLOG_FILE << "OrcRowReaderFilter: skip row group " << rowGroupIdx << ", stripe " << _current_stripe_index;
            return true;
        }
    }
    return false;
}

bool OrcRowReaderFilter::filterOnPickStringDictionary(
        const std::unordered_map<uint64_t, orc::StringDictionary*>& sdicts) {
    _dict_filter_eval_cache.clear();

    if (sdicts.empty()) return false;

    if (!_init_use_dict_filter_slots) {
        for (auto& col : _scanner_ctx.materialized_columns) {
            SlotDescriptor* slot = col.slot_desc;
            if (!_scanner_ctx.can_use_dict_filter_on_slot(slot)) {
                continue;
            }
            int32_t column_index = _reader->get_column_id_by_slot_name(col.col_name);
            if (column_index < 0) {
                continue;
            }
            _use_dict_filter_slots.emplace_back(std::make_pair(slot, column_index));
        }
        _init_use_dict_filter_slots = true;
    }

    for (auto& p : _use_dict_filter_slots) {
        SlotDescriptor* slot_desc = p.first;
        SlotId slot_id = slot_desc->id();
        uint64_t column_index = p.second;
        const auto& it = sdicts.find(column_index);
        if (it == sdicts.end()) {
            continue;
        }
        // create chunk
        orc::StringDictionary* dict = it->second;
        if (dict->dictionaryOffset.size() > _reader->runtime_state()->chunk_size()) {
            continue;
        }
        ChunkPtr dict_value_chunk = std::make_shared<Chunk>();
        // always assume there is a possibility of null value in ORC column.
        // and we evaluate with null always.
        ColumnPtr column_ptr = ColumnHelper::create_column(slot_desc->type(), true);
        dict_value_chunk->append_column(column_ptr, slot_id);

        auto* nullable_column = down_cast<NullableColumn*>(column_ptr.get());
        auto* dict_value_column = down_cast<BinaryColumn*>(nullable_column->data_column().get());

        // copy dict and offset to column.
        Bytes& bytes = dict_value_column->get_bytes();
        Offsets& offsets = dict_value_column->get_offset();

        const char* content_data = dict->dictionaryBlob.data();
        size_t content_size = dict->dictionaryBlob.size();
        bytes.reserve(content_size);
        const auto* start = reinterpret_cast<const uint8_t*>(content_data);
        const auto* end = reinterpret_cast<const uint8_t*>(content_data + content_size);

        size_t offset_size = dict->dictionaryOffset.size();
        size_t dict_size = offset_size - 1;
        const int64_t* offset_data = dict->dictionaryOffset.data();
        offsets.resize(offset_size);

        if (slot_desc->type().type == TYPE_CHAR) {
            // for char type, dict strings are also padded with spaces.
            // we also have to strip space off. For example
            // | hello      |  world      | yes     |, we have to compact to
            // | hello | world | yes |
            size_t total_size = 0;
            const char* p_start = reinterpret_cast<const char*>(start);
            for (size_t i = 0; i < dict_size; i++) {
                const char* s = p_start + offset_data[i];
                size_t old_size = offset_data[i + 1] - offset_data[i];
                size_t new_size = remove_trailing_spaces(s, old_size);
                bytes.insert(bytes.end(), s, s + new_size);
                offsets[i] = total_size;
                total_size += new_size;
            }
            offsets[dict_size] = total_size;
        } else {
            bytes.insert(bytes.end(), start, end);
            // type mismatch, have to use loop to assign.
            for (size_t i = 0; i < offset_size; i++) {
                offsets[i] = offset_data[i];
            }
        }

        // first (dict_size) th items are all not-null
        nullable_column->null_column()->append_default(dict_size);
        // and last one is null.
        nullable_column->append_default();
        DCHECK(nullable_column->size() == (dict_size + 1));

        VLOG_FILE << "OrcRowReaderFilter: stripe = " << _current_stripe_index
                  << ", slot = " << slot_desc->debug_string()
                  << ", dict values = " << dict_value_column->debug_string();

        FilterPtr* filter_ptr = nullptr;
        if (_can_do_filter_on_orc_cvb) {
            // not sure it's best practice.
            FilterPtr tmp;
            _dict_filter_eval_cache[slot_id] = tmp;
            filter_ptr = &_dict_filter_eval_cache[slot_id];
        }

        // do evaluation with dictionary.
        ExecNode::eval_conjuncts(_scanner_ctx.conjunct_ctxs_by_slot.at(slot_id), dict_value_chunk.get(), filter_ptr);
        if (dict_value_chunk->num_rows() == 0) {
            // release memory early.
            _dict_filter_eval_cache.clear();
            VLOG_FILE << "OrcRowReaderFilter: skip stripe by dict filter, stripe " << _current_stripe_index
                      << ", on slot = " << slot_desc->debug_string();
            return true;
        }
        DCHECK(filter_ptr->get() != nullptr);
    }

    return false;
}

Status HdfsOrcScanner::do_open(RuntimeState* runtime_state) {
    RETURN_IF_ERROR(open_random_access_file());
    auto input_stream = std::make_unique<ORCHdfsFileStream>(_file.get(), _file->get_size().value(),
                                                            _shared_buffered_input_stream.get());
    SCOPED_RAW_TIMER(&_stats.reader_init_ns);
    std::unique_ptr<orc::Reader> reader;
    try {
        orc::ReaderOptions options;
        options.setMemoryPool(*getOrcMemoryPool());
        reader = orc::createReader(std::move(input_stream), options);
    } catch (std::exception& e) {
        auto s = strings::Substitute("HdfsOrcScanner::do_open failed. reason = $0", e.what());
        LOG(WARNING) << s;
        return Status::InternalError(s);
    }

    std::unordered_set<std::string> known_column_names;
    OrcChunkReader::build_column_name_set(&known_column_names, _scanner_ctx.hive_column_names, reader->getType(),
                                          _scanner_ctx.case_sensitive);
    _scanner_ctx.update_materialized_columns(known_column_names);
    ASSIGN_OR_RETURN(auto skip, _scanner_ctx.should_skip_by_evaluating_not_existed_slots());
    if (skip) {
        LOG(INFO) << "HdfsOrcScanner: do_open. skip file for non existed slot conjuncts.";
        _should_skip_file = true;
        // no need to intialize following context.
        return Status::OK();
    }

    // create orc reader for further reading.
    int src_slot_index = 0;
    // we don't need to eval conjunct ctxs at outside any more
    // we evaluate conjunct ctxs in `do_get_next`.
    _scanner_params.eval_conjunct_ctxs = false;
    for (const auto& column : _scanner_ctx.materialized_columns) {
        auto col_name = OrcChunkReader::format_column_name(column.col_name, _scanner_ctx.case_sensitive);
        if (known_column_names.find(col_name) == known_column_names.end()) continue;
        bool is_lazy_slot = _scanner_params.is_lazy_materialization_slot(column.slot_id);
        if (is_lazy_slot) {
            _lazy_load_ctx.lazy_load_slots.emplace_back(column.slot_desc);
            _lazy_load_ctx.lazy_load_indices.emplace_back(src_slot_index);
            // reserve room for later set in `OrcChunkReader`
            _lazy_load_ctx.lazy_load_orc_positions.emplace_back(0);
        } else {
            _lazy_load_ctx.active_load_slots.emplace_back(column.slot_desc);
            _lazy_load_ctx.active_load_indices.emplace_back(src_slot_index);
            // reserve room for later set in `OrcChunkReader`
            _lazy_load_ctx.active_load_orc_positions.emplace_back(0);
        }
        _src_slot_descriptors.emplace_back(column.slot_desc);
        src_slot_index++;
    }

    _orc_reader = std::make_unique<OrcChunkReader>(runtime_state->chunk_size(), _src_slot_descriptors);
    _orc_row_reader_filter = std::make_shared<OrcRowReaderFilter>(_scanner_ctx, _orc_reader.get());
    _orc_reader->disable_broker_load_mode();
    _orc_reader->set_row_reader_filter(_orc_row_reader_filter);
    _orc_reader->set_read_chunk_size(runtime_state->chunk_size());
    _orc_reader->set_runtime_state(runtime_state);
    _orc_reader->set_current_file_name(_file->filename());
    RETURN_IF_ERROR(_orc_reader->set_timezone(_scanner_ctx.timezone));
    if (_use_orc_sargs) {
        std::vector<Expr*> conjuncts;
        for (const auto& it : _scanner_ctx.conjunct_ctxs_by_slot) {
            for (const auto& it2 : it.second) {
                conjuncts.push_back(it2->root());
            }
        }
        RETURN_IF_ERROR(
                _orc_reader->set_conjuncts_and_runtime_filters(conjuncts, _scanner_ctx.runtime_filter_collector));
    }
    _orc_reader->set_hive_column_names(_scanner_ctx.hive_column_names);
    _orc_reader->set_case_sensitive(_scanner_ctx.case_sensitive);
    if (config::enable_orc_late_materialization && _lazy_load_ctx.lazy_load_slots.size() != 0 &&
        _lazy_load_ctx.active_load_slots.size() != 0) {
        _orc_reader->set_lazy_load_context(&_lazy_load_ctx);
    }
    RETURN_IF_ERROR(_orc_reader->init(std::move(reader)));
    return Status::OK();
}

void HdfsOrcScanner::do_close(RuntimeState* runtime_state) noexcept {
    _orc_reader.reset(nullptr);
}

Status HdfsOrcScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    CHECK(chunk != nullptr);
    if (_should_skip_file) {
        return Status::EndOfFile("");
    }

    ChunkPtr& ck = *chunk;
    // this infinite for loop is for retry.
    for (;;) {
        orc::RowReader::ReadPosition position;
        size_t read_num_values = 0;
        bool has_used_dict_filter = false;
        ColumnPtr row_delete_filter = BooleanColumn::create();
        {
            SCOPED_RAW_TIMER(&_stats.column_read_ns);
            RETURN_IF_ERROR(_orc_reader->read_next(&position));
            row_delete_filter = _orc_reader->get_row_delete_filter(_need_skip_rowids);
            // read num values is how many rows actually read before doing dict filtering.
            read_num_values = position.num_values;
            RETURN_IF_ERROR(_orc_reader->apply_dict_filter_eval_cache(_orc_row_reader_filter->_dict_filter_eval_cache,
                                                                      &_dict_filter));
            if (_orc_reader->get_cvb_size() != read_num_values) {
                has_used_dict_filter = true;
                row_delete_filter->filter(_dict_filter);
            }
        }

        size_t chunk_size = 0;
        size_t chunk_size_ori = 0;
        if (_orc_reader->get_cvb_size() != 0) {
            chunk_size = _orc_reader->get_cvb_size();
            chunk_size_ori = chunk_size;
            {
                StatusOr<ChunkPtr> ret;
                SCOPED_RAW_TIMER(&_stats.column_convert_ns);
                if (!_orc_reader->has_lazy_load_context()) {
                    ret = _orc_reader->get_chunk();
                } else {
                    ret = _orc_reader->get_active_chunk();
                }
                RETURN_IF_ERROR(ret);
                *chunk = std::move(ret.value());
            }

            // important to add columns before evaluation
            // because ctxs_by_slot maybe refers to some non-existed slot or partition slot.
            _scanner_ctx.append_not_existed_columns_to_chunk(chunk, chunk_size);
            _scanner_ctx.append_partition_column_to_chunk(chunk, chunk_size);
            // do stats before we filter rows which does not match.
            _stats.raw_rows_read += chunk_size;
            _chunk_filter.assign(chunk_size, 1);
            {
                SCOPED_RAW_TIMER(&_stats.expr_filter_ns);
                for (auto& it : _scanner_ctx.conjunct_ctxs_by_slot) {
                    // do evaluation.
                    if (_orc_row_reader_filter->is_slot_evaluated(it.first)) {
                        continue;
                    }
                    ASSIGN_OR_RETURN(chunk_size,
                                     ExecNode::eval_conjuncts_into_filter(it.second, ck.get(), &_chunk_filter));
                    if (chunk_size == 0) {
                        break;
                    }
                }
                if (chunk_size != 0) {
                    ASSIGN_OR_RETURN(chunk_size, ExecNode::eval_conjuncts_into_filter(_scanner_params.conjunct_ctxs,
                                                                                      ck.get(), &_chunk_filter));
                }
            }

            if (chunk_size != 0) {
                ColumnHelper::merge_two_filters(row_delete_filter, &_chunk_filter, nullptr);
                chunk_size = SIMD::count_nonzero(_chunk_filter);
            }

            if (chunk_size != 0 && chunk_size != ck->num_rows()) {
                ck->filter(_chunk_filter);
            }
        }
        ck->set_num_rows(chunk_size);

        if (!_orc_reader->has_lazy_load_context()) {
            return Status::OK();
        }

        // if has lazy load fields, skip it if chunk_size == 0
        if (chunk_size == 0) {
            _stats.skip_read_rows += chunk_size_ori;
            continue;
        }
        {
            SCOPED_RAW_TIMER(&_stats.column_read_ns);
            RETURN_IF_ERROR(_orc_reader->lazy_seek_to(position.row_in_stripe));
            RETURN_IF_ERROR(_orc_reader->lazy_read_next(read_num_values));
        }
        {
            SCOPED_RAW_TIMER(&_stats.column_convert_ns);
            if (has_used_dict_filter) {
                _orc_reader->lazy_filter_on_cvb(&_dict_filter);
            }
            _orc_reader->lazy_filter_on_cvb(&_chunk_filter);
            StatusOr<ChunkPtr> ret = _orc_reader->get_lazy_chunk();
            RETURN_IF_ERROR(ret);
            Chunk& ret_ck = *(ret.value());
            ck->merge(std::move(ret_ck));
        }
        return Status::OK();
    }
    __builtin_unreachable();
    return Status::OK();
}

Status HdfsOrcScanner::do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    _should_skip_file = false;
    _use_orc_sargs = true;
    // todo: build predicate hook and ranges hook.
    if (!scanner_params.deletes.empty()) {
        SCOPED_RAW_TIMER(&_stats.delete_build_ns);
        IcebergDeleteBuilder iceberg_delete_builder(scanner_params.fs, scanner_params.path,
                                                    scanner_params.conjunct_ctxs, scanner_params.materialize_slots,
                                                    &_need_skip_rowids);
        for (const auto& tdelete_file : scanner_params.deletes) {
            RETURN_IF_ERROR(iceberg_delete_builder.build_orc(runtime_state->timezone(), *tdelete_file));
        }
        _stats.delete_file_per_scan += scanner_params.deletes.size();
    }

    return Status::OK();
}

static const std::string kORCProfileSectionPrefix = "ORC";

void HdfsOrcScanner::do_update_counter(HdfsScanProfile* profile) {
    RuntimeProfile::Counter* delete_build_timer = nullptr;
    RuntimeProfile::Counter* delete_file_per_scan_counter = nullptr;
    RuntimeProfile* root = profile->runtime_profile;

    ADD_COUNTER(root, kORCProfileSectionPrefix, TUnit::UNIT);

    delete_build_timer = ADD_CHILD_TIMER(root, "DeleteBuildTimer", kORCProfileSectionPrefix);
    delete_file_per_scan_counter = ADD_CHILD_COUNTER(root, "DeleteFilesPerScan", TUnit::UNIT, kORCProfileSectionPrefix);

    COUNTER_UPDATE(delete_build_timer, _stats.delete_build_ns);
    COUNTER_UPDATE(delete_file_per_scan_counter, _stats.delete_file_per_scan);
}

} // namespace starrocks
