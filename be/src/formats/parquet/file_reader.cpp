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

#include "formats/parquet/file_reader.h"

#include <glog/logging.h>

#include <cstring>
#include <iterator>
#include <map>
#include <sstream>
#include <unordered_set>
#include <utility>
#include <vector>

#include "cache/block_cache/kv_cache.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/vectorized_fwd.h"
#include "common/compiler_util.h"
#include "common/config.h"
#include "common/global_types.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/hdfs_scanner.h"
#include "exprs/expr_context.h"
#include "exprs/runtime_filter.h"
#include "exprs/runtime_filter_bank.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/scalar_column_reader.h"
#include "formats/parquet/schema.h"
#include "formats/parquet/statistics_helper.h"
#include "formats/parquet/utils.h"
#include "formats/parquet/zone_map_filter_evaluator.h"
#include "fs/fs.h"
#include "gen_cpp/parquet_types.h"
#include "gutil/casts.h"
#include "gutil/strings/substitute.h"
#include "io/shared_buffered_input_stream.h"
#include "runtime/descriptors.h"
#include "storage/chunk_helper.h"

namespace starrocks::parquet {

FileReader::FileReader(int chunk_size, RandomAccessFile* file, size_t file_size,
                       const DataCacheOptions& datacache_options, io::SharedBufferedInputStream* sb_stream,
                       SkipRowsContextPtr skip_rows_context)
        : _chunk_size(chunk_size),
          _file(file),
          _file_size(file_size),
          _datacache_options(datacache_options),
          _sb_stream(sb_stream),
          _skip_rows_ctx(std::move(skip_rows_context)) {}

FileReader::~FileReader() = default;

Status FileReader::init(HdfsScannerContext* ctx) {
    _scanner_ctx = ctx;
#ifdef WITH_STARCACHE
    // Only support file metacache in starcache engine
    if (ctx->use_file_metacache && config::datacache_enable) {
        _cache = BlockCache::instance();
    }
#endif
    // parse FileMetadata
    FileMetaDataParser file_metadata_parser{_file, ctx, _cache, &_datacache_options, _file_size};
    ASSIGN_OR_RETURN(_file_metadata, file_metadata_parser.get_file_metadata());

    // set existed SlotDescriptor in this parquet file
    std::unordered_set<std::string> existed_column_names;
    _meta_helper = _build_meta_helper();
    _prepare_read_columns(existed_column_names);
    RETURN_IF_ERROR(_scanner_ctx->update_materialized_columns(existed_column_names));

    ASSIGN_OR_RETURN(_is_file_filtered, _scanner_ctx->should_skip_by_evaluating_not_existed_slots());
    if (_is_file_filtered) {
        return Status::OK();
    }

    RETURN_IF_ERROR(_build_split_tasks());
    if (_scanner_ctx->split_tasks.size() > 0) {
        _scanner_ctx->has_split_tasks = true;
        _is_file_filtered = true;
        return Status::OK();
    }

    RETURN_IF_ERROR(_init_group_readers());
    return Status::OK();
}

std::shared_ptr<MetaHelper> FileReader::_build_meta_helper() {
    if (_scanner_ctx->iceberg_schema != nullptr && _file_metadata->schema().exist_filed_id()) {
        // If we want read this parquet file with iceberg schema,
        // we also need to make sure it contains parquet field id.
        return std::make_shared<IcebergMetaHelper>(_file_metadata.get(), _scanner_ctx->case_sensitive,
                                                   _scanner_ctx->iceberg_schema);
    } else {
        return std::make_shared<ParquetMetaHelper>(_file_metadata.get(), _scanner_ctx->case_sensitive);
    }
}

FileMetaData* FileReader::get_file_metadata() {
    return _file_metadata.get();
}

Status FileReader::collect_scan_io_ranges(std::vector<io::SharedBufferedInputStream::IORange>* io_ranges) {
    int64_t dummy_offset = 0;
    for (auto& r : _row_group_readers) {
        r->collect_io_ranges(io_ranges, &dummy_offset, ColumnIOType::PAGE_INDEX);
        r->collect_io_ranges(io_ranges, &dummy_offset, ColumnIOType::PAGES);
    }
    return Status::OK();
}

Status FileReader::_build_split_tasks() {
    // dont do split in following cases:
    // 1. this feature is not enabled
    // 2. we have already do split before (that's why `split_context` is nullptr)
    if (!_scanner_ctx->enable_split_tasks || _scanner_ctx->split_context != nullptr) {
        return Status::OK();
    }

    size_t row_group_size = _file_metadata->t_metadata().row_groups.size();
    for (size_t i = 0; i < row_group_size; i++) {
        const tparquet::RowGroup& row_group = _file_metadata->t_metadata().row_groups[i];
        if (!_select_row_group(row_group)) continue;
        int64_t start_offset = ParquetUtils::get_row_group_start_offset(row_group);
        int64_t end_offset = ParquetUtils::get_row_group_end_offset(row_group);
        if (start_offset >= end_offset) {
            LOG(INFO) << "row group " << i << " is empty. start = " << start_offset << ", end = " << end_offset;
            continue;
        }
#ifndef NDEBUG
        DCHECK(start_offset < end_offset) << "start = " << start_offset << ", end = " << end_offset;
        // there could be holes between row groups.
        // but this does not affect our scan range filter logic.
        // because in `_select_row_group`, we check if `start offset of row group` is in this range
        // so as long as `end_offset > start_offset && end_offset <= start_offset(next_group)`, it's ok
        if ((i + 1) < row_group_size) {
            const tparquet::RowGroup& next_row_group = _file_metadata->t_metadata().row_groups[i + 1];
            DCHECK(end_offset <= ParquetUtils::get_row_group_start_offset(next_row_group));
        }
#endif
        auto split_ctx = std::make_unique<SplitContext>();
        split_ctx->split_start = start_offset;
        split_ctx->split_end = end_offset;
        split_ctx->file_metadata = _file_metadata;
        split_ctx->skip_rows_ctx = _skip_rows_ctx;
        _scanner_ctx->split_tasks.emplace_back(std::move(split_ctx));
    }
    _scanner_ctx->merge_split_tasks();
    // if only one split task, clear it, no need to do split work.
    if (_scanner_ctx->split_tasks.size() <= 1) {
        _scanner_ctx->split_tasks.clear();
    }

    if (VLOG_OPERATOR_IS_ON) {
        std::stringstream ss;
        for (const HdfsSplitContextPtr& ctx : _scanner_ctx->split_tasks) {
            ss << "[" << ctx->split_start << "," << ctx->split_end << "]";
        }
        VLOG_OPERATOR << "FileReader: do_open. split task for " << _file->filename()
                      << ", split_tasks.size = " << _scanner_ctx->split_tasks.size() << ", range = " << ss.str();
    }
    return Status::OK();
}

// when doing row group filter, there maybe some error, but we'd better just ignore it instead of returning the error
// status and lead to the query failed.
bool FileReader::_filter_group(const GroupReaderPtr& group_reader) {
    if (_scanner_ctx->rf_scan_range_pruner != nullptr) {
        _rf_scan_range_pruner = std::make_shared<RuntimeScanRangePruner>(*_scanner_ctx->rf_scan_range_pruner);
    }
    auto res = _scanner_ctx->predicate_tree.visit(
            ZoneMapEvaluator<FilterLevel::ROW_GROUP>{_scanner_ctx->predicate_tree, group_reader.get()});
    if (!res.ok()) {
        LOG(WARNING) << "filter row group failed: " << res.status().message();
        return false;
    }
    if (res.value().has_value() && res.value()->span_size() == 0) {
        // no rows selected, the whole row group can be filtered
        return true;
    }
    return false;
}

StatusOr<bool> FileReader::_update_rf_and_filter_group(const GroupReaderPtr& group_reader) {
    bool filter = false;
    if (_rf_scan_range_pruner != nullptr) {
        RETURN_IF_ERROR(_rf_scan_range_pruner->update_range_if_arrived(
                _scanner_ctx->global_dictmaps,
                [&filter, &group_reader](auto cid, const PredicateList& predicates) {
                    PredicateCompoundNode<CompoundNodeType::AND> pred_tree;
                    for (const auto& pred : predicates) {
                        pred_tree.add_child(PredicateColumnNode{pred});
                    }
                    auto real_tree = PredicateTree::create(std::move(pred_tree));

                    auto res = real_tree.visit(ZoneMapEvaluator<FilterLevel::ROW_GROUP>{real_tree, group_reader.get()});
                    if (res.ok() && res->has_value() && res->value().span_size() == 0) {
                        filter = true;
                    }
                    return Status::OK();
                },
                true, 0));
    }
    return filter;
}

int32_t FileReader::_get_partition_column_idx(const std::string& col_name) const {
    for (int32_t i = 0; i < _scanner_ctx->partition_columns.size(); i++) {
        if (_scanner_ctx->partition_columns[i].name() == col_name) {
            return i;
        }
    }
    return -1;
}

void FileReader::_prepare_read_columns(std::unordered_set<std::string>& existed_column_names) {
    _meta_helper->prepare_read_columns(_scanner_ctx->materialized_columns, _group_reader_param.read_cols,
                                       existed_column_names);
    _no_materialized_column_scan = (_group_reader_param.read_cols.size() == 0);
}

bool FileReader::_select_row_group(const tparquet::RowGroup& row_group) {
    size_t row_group_start = ParquetUtils::get_row_group_start_offset(row_group);
    const auto* scan_range = _scanner_ctx->scan_range;
    size_t scan_start = scan_range->offset;
    size_t scan_end = scan_range->length + scan_start;
    if (row_group_start >= scan_start && row_group_start < scan_end) {
        return true;
    }
    return false;
}

Status FileReader::_init_group_readers() {
    const HdfsScannerContext& fd_scanner_ctx = *_scanner_ctx;

    // _group_reader_param is used by all group readers
    _group_reader_param.conjunct_ctxs_by_slot = fd_scanner_ctx.conjunct_ctxs_by_slot;
    _group_reader_param.timezone = fd_scanner_ctx.timezone;
    _group_reader_param.stats = fd_scanner_ctx.stats;
    _group_reader_param.sb_stream = _sb_stream;
    _group_reader_param.chunk_size = _chunk_size;
    _group_reader_param.file = _file;
    _group_reader_param.file_metadata = _file_metadata.get();
    _group_reader_param.case_sensitive = fd_scanner_ctx.case_sensitive;
    _group_reader_param.lazy_column_coalesce_counter = fd_scanner_ctx.lazy_column_coalesce_counter;
    _group_reader_param.partition_columns = &fd_scanner_ctx.partition_columns;
    _group_reader_param.partition_values = &fd_scanner_ctx.partition_values;
    _group_reader_param.not_existed_slots = &fd_scanner_ctx.not_existed_slots;
    // for pageIndex
    _group_reader_param.min_max_conjunct_ctxs = fd_scanner_ctx.min_max_conjunct_ctxs;
    _group_reader_param.predicate_tree = &fd_scanner_ctx.predicate_tree;
    _group_reader_param.global_dictmaps = fd_scanner_ctx.global_dictmaps;

    int64_t row_group_first_row = 0;
    // select and create row group readers.
    for (size_t i = 0; i < _file_metadata->t_metadata().row_groups.size(); i++) {
        if (i > 0) {
            row_group_first_row += _file_metadata->t_metadata().row_groups[i - 1].num_rows;
        }

        if (!_select_row_group(_file_metadata->t_metadata().row_groups[i])) {
            continue;
        }

        auto row_group_reader =
                std::make_shared<GroupReader>(_group_reader_param, i, _skip_rows_ctx, row_group_first_row);
        RETURN_IF_ERROR(row_group_reader->init());

        _group_reader_param.stats->parquet_total_row_groups += 1;

        // You should call row_group_reader->init() before _filter_group()
        if (_filter_group(row_group_reader)) {
            DLOG(INFO) << "row group " << i << " of file has been filtered by min/max conjunct";
            _group_reader_param.stats->parquet_filtered_row_groups += 1;
            continue;
        }

        _row_group_readers.emplace_back(row_group_reader);
        int64_t num_rows = _file_metadata->t_metadata().row_groups[i].num_rows;
        // for skip rows which already deleted
        if (_skip_rows_ctx != nullptr && _skip_rows_ctx->has_skip_rows()) {
            uint64_t deletion_rows = _skip_rows_ctx->deletion_bitmap->get_range_cardinality(
                    row_group_first_row, row_group_first_row + num_rows);
            num_rows -= deletion_rows;
        }
        _total_row_count += num_rows;
    }
    _row_group_size = _row_group_readers.size();

    // collect pageIndex io ranges.
    if (config::parquet_coalesce_read_enable && _sb_stream != nullptr && config::parquet_page_index_enable) {
        std::vector<io::SharedBufferedInputStream::IORange> ranges;
        int64_t end_offset = 0;
        for (auto& r : _row_group_readers) {
            r->collect_io_ranges(&ranges, &end_offset, ColumnIOType::PAGE_INDEX);
        }
        RETURN_IF_ERROR(_sb_stream->set_io_ranges(ranges));
    }

    if (!_row_group_readers.empty()) {
        // prepare first row group
        RETURN_IF_ERROR(_row_group_readers[_cur_row_group_idx]->prepare());
    }

    return Status::OK();
}

Status FileReader::get_next(ChunkPtr* chunk) {
    if (_is_file_filtered) {
        return Status::EndOfFile("");
    }
    if (_no_materialized_column_scan) {
        RETURN_IF_ERROR(_exec_no_materialized_column_scan(chunk));
        return Status::OK();
    }

    if (_cur_row_group_idx < _row_group_size) {
        size_t row_count = _chunk_size;
        Status status = _row_group_readers[_cur_row_group_idx]->get_next(chunk, &row_count);
        if (status.ok() || status.is_end_of_file()) {
            if (row_count > 0) {
                RETURN_IF_ERROR(_scanner_ctx->append_or_update_not_existed_columns_to_chunk(chunk, row_count));
                _scanner_ctx->append_or_update_partition_column_to_chunk(chunk, row_count);
                _scanner_ctx->append_or_update_extended_column_to_chunk(chunk, row_count);
                _scan_row_count += (*chunk)->num_rows();
            }
            if (status.is_end_of_file()) {
                // release previous RowGroupReader
                do {
                    _row_group_readers[_cur_row_group_idx] = nullptr;
                    _cur_row_group_idx++;
                    if (_cur_row_group_idx < _row_group_size) {
                        const auto& cur_row_group = _row_group_readers[_cur_row_group_idx];
                        auto ret = _update_rf_and_filter_group(cur_row_group);
                        if (ret.ok() && ret.value()) {
                            // row group is filtered by runtime filter
                            _group_reader_param.stats->parquet_filtered_row_groups += 1;
                            continue;
                        } else if (ret.status().is_end_of_file()) {
                            // If rf is always false, will return eof
                            _group_reader_param.stats->parquet_filtered_row_groups +=
                                    (_row_group_size - _cur_row_group_idx);
                            _row_group_readers.assign(_row_group_readers.size(), nullptr);
                            _cur_row_group_idx = _row_group_size;
                            break;
                        } else {
                            // do nothing, ignore the error code
                        }

                        RETURN_IF_ERROR(cur_row_group->prepare());
                    }
                    break;
                } while (true);

                return Status::OK();
            }
        } else {
            auto s = strings::Substitute("FileReader::get_next failed. reason = $0, file = $1", status.to_string(),
                                         _file->filename());
            LOG(WARNING) << s;
            return status;
        }

        return status;
    }

    return Status::EndOfFile("");
}

Status FileReader::_exec_no_materialized_column_scan(ChunkPtr* chunk) {
    if (_scan_row_count < _total_row_count) {
        size_t read_size = 0;
        if (_scanner_ctx->return_count_column) {
            read_size = _total_row_count - _scan_row_count;
            _scanner_ctx->append_or_update_count_column_to_chunk(chunk, read_size);
            _scanner_ctx->append_or_update_partition_column_to_chunk(chunk, 1);
            _scanner_ctx->append_or_update_extended_column_to_chunk(chunk, 1);
        } else {
            read_size = std::min(static_cast<size_t>(_chunk_size), _total_row_count - _scan_row_count);
            RETURN_IF_ERROR(_scanner_ctx->append_or_update_not_existed_columns_to_chunk(chunk, read_size));
            _scanner_ctx->append_or_update_partition_column_to_chunk(chunk, read_size);
            _scanner_ctx->append_or_update_extended_column_to_chunk(chunk, read_size);
        }
        _scan_row_count += read_size;
        return Status::OK();
    }

    return Status::EndOfFile("");
}

} // namespace starrocks::parquet
