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
#include "formats/orc/fill_function.h"
#include "formats/orc/orc_chunk_reader.h"
#include "formats/orc/orc_chunk_reader_factory.h"
#include "formats/orc/orc_input_stream.h"
#include "simd/simd.h"
#include "storage/chunk_helper.h"
#include "util/runtime_profile.h"
#include "util/timezone_utils.h"

namespace starrocks {

Status HdfsOrcScanner::do_open(RuntimeState* runtime_state) {
    RETURN_IF_ERROR(open_random_access_file());
    auto input_stream = std::make_unique<ORCHdfsFileStream>(_file.get(), _scanner_params.scan_ranges[0]->file_length);
    SCOPED_RAW_TIMER(&_stats.reader_init_ns);
    std::unique_ptr<orc::Reader> reader;
    try {
        orc::ReaderOptions options;
        reader = orc::createReader(std::move(input_stream), options);
    } catch (std::exception& e) {
        auto s = strings::Substitute("HdfsOrcScanner::do_open failed. reason = $0", e.what());
        LOG(WARNING) << s;
        return Status::InternalError(s);
    }

    std::unordered_set<std::string> known_column_names;
    OrcChunkReader::build_column_name_set(&known_column_names, _scanner_params.hive_column_names, reader->getType(),
                                          _scanner_params.case_sensitive);
    _scanner_ctx.set_columns_from_file(known_column_names);
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
    for (const auto& it : _scanner_params.materialize_slots) {
        auto col_name = OrcChunkReader::format_column_name(it->col_name(), _scanner_params.case_sensitive);
        if (known_column_names.find(col_name) == known_column_names.end()) continue;
        bool is_lazy_slot = _scanner_params.is_lazy_materialization_slot(it->id());
        if (is_lazy_slot) {
            _lazy_load_ctx.lazy_load_slots.emplace_back(it);
            _lazy_load_ctx.lazy_load_indices.emplace_back(src_slot_index);
            // reserve room for later set in `OrcChunkReader`
            _lazy_load_ctx.lazy_load_orc_positions.emplace_back(0);
        } else {
            _lazy_load_ctx.active_load_slots.emplace_back(it);
            _lazy_load_ctx.active_load_indices.emplace_back(src_slot_index);
            // reserve room for later set in `OrcChunkReader`
            _lazy_load_ctx.active_load_orc_positions.emplace_back(0);
        }
        _src_slot_descriptors.emplace_back(it);
        src_slot_index++;
    }

    size_t read_chunk_size = runtime_state->chunk_size() ? runtime_state->chunk_size() : 4096;
    HdfsOrcReaderFactory orc_reader_factory{_src_slot_descriptors,
                                            _runtime_state,
                                            std::move(reader),
                                            false,
                                            _scanner_params.hive_column_names,
                                            _scanner_params.case_sensitive,
                                            _scanner_ctx.timezone,
                                            _scanner_params.scan_ranges[0]->relative_path,
                                            read_chunk_size,
                                            &_lazy_load_ctx};
    if (_use_orc_sargs) {
        std::vector<Expr*> conjuncts;
        for (const auto& it : _scanner_params.conjunct_ctxs_by_slot) {
            for (const auto& it2 : it.second) {
                conjuncts.push_back(it2->root());
            }
        }
        orc_reader_factory.set_use_orc_search_arguments(true, conjuncts, _scanner_params.runtime_filter_collector);
    } else {
        orc_reader_factory.set_use_orc_search_arguments(false);
    }
    orc_reader_factory.set_use_orc_row_reader_filter(true, &_scanner_params, &_scanner_ctx);
    ASSIGN_OR_RETURN(_orc_reader, orc_reader_factory.create());
    _orc_row_reader_filter = orc_reader_factory.orc_row_reader_filter();
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
        if (_orc_reader->get_cvb_size() != 0) {
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
            _scanner_ctx.append_not_existed_columns_to_chunk(chunk, ck->num_rows());
            _scanner_ctx.append_partition_column_to_chunk(chunk, ck->num_rows());
            chunk_size = ck->num_rows();
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
            continue;
        }
        {
            SCOPED_RAW_TIMER(&_stats.column_read_ns);
            _orc_reader->lazy_seek_to(position.row_in_stripe);
            _orc_reader->lazy_read_next(read_num_values);
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
} // namespace starrocks
