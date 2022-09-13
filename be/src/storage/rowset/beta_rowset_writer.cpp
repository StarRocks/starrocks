// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/beta_rowset_writer.cpp

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

#include "storage/rowset/beta_rowset_writer.h"

#include <fmt/format.h>

#include <ctime>
#include <memory>

#include "column/chunk.h"
#include "common/config.h"
#include "common/logging.h"
#include "env/env.h"
#include "gutil/strings/substitute.h"
#include "runtime/exec_env.h"
#include "serde/column_array_serde.h"
#include "storage/fs/fs_util.h"
#include "storage/olap_define.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/vectorized/segment_options.h"
#include "storage/storage_engine.h"
#include "storage/vectorized/aggregate_iterator.h"
#include "storage/vectorized/chunk_helper.h"
#include "storage/vectorized/merge_iterator.h"
#include "storage/vectorized/type_utils.h"
#include "util/pretty_printer.h"

namespace starrocks {

BetaRowsetWriter::BetaRowsetWriter(const RowsetWriterContext& context)
        : _context(context),
          _rowset_meta(nullptr),
          _num_segment(0),
          _num_rows_written(0),
          _total_row_size(0),
          _total_data_size(0),
          _total_index_size(0) {}

Status BetaRowsetWriter::init() {
    DCHECK(!(_context.tablet_schema->contains_format_v1_column() &&
             _context.tablet_schema->contains_format_v2_column()));
    auto real_data_format = _context.storage_format_version;
    auto tablet_format = real_data_format;
    if (_context.tablet_schema->contains_format_v1_column()) {
        tablet_format = kDataFormatV1;
    } else if (_context.tablet_schema->contains_format_v2_column()) {
        tablet_format = kDataFormatV2;
    }

    // StarRocks is built on earlier work on Apache Doris and is compatible with its data format but
    // has newly designed storage formats for DATA/DATETIME/DECIMAL for better performance.
    // When loading data into a tablet created by Apache Doris, the real data format will
    // be different from the tablet schema, so here we create a new schema matched with the
    // real data format to init `SegmentWriter`.
    if (real_data_format != tablet_format) {
        _rowset_schema = _context.tablet_schema->convert_to_format(real_data_format);
    }

    _rowset_meta = std::make_shared<RowsetMeta>();
    _rowset_meta->set_rowset_id(_context.rowset_id);
    _rowset_meta->set_partition_id(_context.partition_id);
    _rowset_meta->set_tablet_id(_context.tablet_id);
    _rowset_meta->set_tablet_schema_hash(_context.tablet_schema_hash);
    _rowset_meta->set_rowset_type(_context.rowset_type);
    _rowset_meta->set_rowset_state(_context.rowset_state);
    _rowset_meta->set_segments_overlap(_context.segments_overlap);
    if (_context.rowset_state == PREPARED || _context.rowset_state == COMMITTED) {
        _is_pending = true;
        _rowset_meta->set_txn_id(_context.txn_id);
        _rowset_meta->set_load_id(_context.load_id);
    } else {
        _rowset_meta->set_version(_context.version);
    }
    _rowset_meta->set_tablet_uid(_context.tablet_uid);

    _writer_options.storage_format_version = _context.storage_format_version;
    _writer_options.global_dicts = _context.global_dicts != nullptr ? _context.global_dicts : nullptr;
    _writer_options.referenced_column_ids = _context.referenced_column_ids;

    if (_context.tablet_schema->keys_type() == KeysType::PRIMARY_KEYS && _context.partial_update_tablet_schema) {
        _rowset_txn_meta_pb = std::make_unique<RowsetTxnMetaPB>();
    }

    return Status::OK();
}

StatusOr<RowsetSharedPtr> BetaRowsetWriter::build() {
    if (_num_rows_written > 0) {
        RETURN_IF_ERROR(_context.env->sync_dir(_context.rowset_path_prefix));
    }
    _rowset_meta->set_num_rows(_num_rows_written);
    _rowset_meta->set_total_row_size(_total_row_size);
    _rowset_meta->set_total_disk_size(_total_data_size);
    _rowset_meta->set_data_disk_size(_total_data_size);
    _rowset_meta->set_index_disk_size(_total_index_size);
    // TODO write zonemap to meta
    _rowset_meta->set_empty(_num_rows_written == 0);
    _rowset_meta->set_creation_time(time(nullptr));
    _rowset_meta->set_num_segments(_num_segment);
    // newly created rowset do not have rowset_id yet, use 0 instead
    _rowset_meta->set_rowset_seg_id(0);
    // updatable tablet require extra processing
    if (_context.tablet_schema->keys_type() == KeysType::PRIMARY_KEYS) {
        _rowset_meta->set_num_delete_files(!_segment_has_deletes.empty() && _segment_has_deletes[0]);
        _rowset_meta->set_segments_overlap(NONOVERLAPPING);
        if (_context.partial_update_tablet_schema) {
            DCHECK(_context.referenced_column_ids.size() == _context.partial_update_tablet_schema->columns().size());
            for (auto i = 0; i < _context.partial_update_tablet_schema->columns().size(); ++i) {
                const auto& tablet_column = _context.partial_update_tablet_schema->column(i);
                _rowset_txn_meta_pb->add_partial_update_column_ids(_context.referenced_column_ids[i]);
                _rowset_txn_meta_pb->add_partial_update_column_unique_ids(tablet_column.unique_id());
            }
            _rowset_meta->set_txn_meta(*_rowset_txn_meta_pb);
        }
    } else {
        if (_num_segment <= 1) {
            _rowset_meta->set_segments_overlap(NONOVERLAPPING);
        }
    }
    if (_is_pending) {
        _rowset_meta->set_rowset_state(COMMITTED);
    } else {
        _rowset_meta->set_rowset_state(VISIBLE);
    }
    RowsetSharedPtr rowset;
    RETURN_IF_ERROR(
            RowsetFactory::create_rowset(_context.tablet_schema, _context.rowset_path_prefix, _rowset_meta, &rowset));
    _already_built = true;
    return rowset;
}

Status BetaRowsetWriter::flush_src_rssids(uint32_t segment_id) {
    auto path = BetaRowset::segment_srcrssid_file_path(_context.rowset_path_prefix, _context.rowset_id,
                                                       static_cast<int>(segment_id));
    std::unique_ptr<fs::WritableBlock> wblock;
    fs::CreateBlockOptions opts({path});
    RETURN_IF_ERROR(_context.block_mgr->create_block(opts, &wblock));
    RETURN_IF_ERROR(wblock->append(Slice((const char*)(_src_rssids->data()), _src_rssids->size() * sizeof(uint32_t))));
    RETURN_IF_ERROR(wblock->finalize());
    RETURN_IF_ERROR(wblock->close());
    _src_rssids->clear();
    _src_rssids.reset();
    return Status::OK();
}

HorizontalBetaRowsetWriter::HorizontalBetaRowsetWriter(const RowsetWriterContext& context)
        : BetaRowsetWriter(context), _segment_writer(nullptr) {}

HorizontalBetaRowsetWriter::~HorizontalBetaRowsetWriter() {
    // TODO(lingbin): Should wrapper exception logic, no need to know file ops directly.
    if (!_already_built) {       // abnormal exit, remove all files generated
        _segment_writer.reset(); // ensure all files are closed
        if (_context.tablet_schema->keys_type() == KeysType::PRIMARY_KEYS) {
            for (const auto& tmp_segment_file : _tmp_segment_files) {
                // Even if an error is encountered, these files that have not been cleaned up
                // will be cleaned up by the GC background. So here we only print the error
                // message when we encounter an error.
                auto st = _context.env->delete_file(tmp_segment_file);
                LOG_IF(WARNING, !st.ok()) << "Fail to delete file=" << tmp_segment_file << ", " << st.to_string();
            }
            _tmp_segment_files.clear();
            for (auto i = 0; i < _num_segment; ++i) {
                auto path = BetaRowset::segment_file_path(_context.rowset_path_prefix, _context.rowset_id, i);
                // Even if an error is encountered, these files that have not been cleaned up
                // will be cleaned up by the GC background. So here we only print the error
                // message when we encounter an error.
                auto st = _context.env->delete_file(path);
                LOG_IF(WARNING, !st.ok()) << "Fail to delete file=" << path << ", " << st.to_string();
            }
            for (auto i = 0; i < _segment_has_deletes.size(); ++i) {
                if (!_segment_has_deletes[i]) {
                    auto path = BetaRowset::segment_del_file_path(_context.rowset_path_prefix, _context.rowset_id, i);
                    auto st = _context.env->delete_file(path);
                    LOG_IF(WARNING, !st.ok()) << "Fail to delete file=" << path << ", " << st.to_string();
                }
            }
            //// only deleting segment files may not delete all delete files or temporary files
            //// during final merge, should iterate directory and delete all files with rowset_id prefix
            //std::vector<std::string> files;
            //Status st = _context.env->get_children(_context.rowset_path_prefix, &files);
            //if (!st.ok()) {
            //	LOG(WARNING) << "list dir failed: " << _context.rowset_path_prefix << " "
            //				 << st.to_string();
            //}
            //string prefix = _context.rowset_id.to_string();
            //for (auto& f : files) {
            //	if (strncmp(f.c_str(), prefix.c_str(), prefix.size()) == 0) {
            //		string path = _context.rowset_path_prefix + "/" + f;
            //		// Even if an error is encountered, these files that have not been cleaned up
            //		// will be cleaned up by the GC background. So here we only print the error
            //		// message when we encounter an error.
            //		Status st = _context.env->delete_file(path);
            //		LOG_IF(WARNING, !st.ok())
            //				<< "Fail to delete file=" << path << ", " << st.to_string();
            //	}
            //}
        } else {
            for (int i = 0; i < _num_segment; ++i) {
                auto path = BetaRowset::segment_file_path(_context.rowset_path_prefix, _context.rowset_id, i);
                // Even if an error is encountered, these files that have not been cleaned up
                // will be cleaned up by the GC background. So here we only print the error
                // message when we encounter an error.
                auto st = _context.env->delete_file(path);
                LOG_IF(WARNING, !st.ok()) << "Fail to delete file=" << path << ", " << st.to_string();
            }
        }
        // if _already_built is false, we need to release rowset_id to avoid rowset_id leak
        StorageEngine::instance()->release_rowset_id(_context.rowset_id);
    }
}

StatusOr<std::unique_ptr<SegmentWriter>> HorizontalBetaRowsetWriter::_create_segment_writer() {
    std::lock_guard<std::mutex> l(_lock);
    std::string path;
    if ((_context.tablet_schema->keys_type() == KeysType::PRIMARY_KEYS &&
         _context.segments_overlap != NONOVERLAPPING) ||
        _context.write_tmp) {
        path = BetaRowset::segment_temp_file_path(_context.rowset_path_prefix, _context.rowset_id, _num_segment);
        _tmp_segment_files.emplace_back(path);
    } else {
        // for update final merge scenario, we marked segments_overlap to NONOVERLAPPING in
        // function _final_merge, so we create segment data file here, rather than
        // temporary segment files.
        path = BetaRowset::segment_file_path(_context.rowset_path_prefix, _context.rowset_id, _num_segment);
    }
    std::unique_ptr<fs::WritableBlock> wblock;
    fs::CreateBlockOptions opts({path});
    RETURN_IF_ERROR(_context.block_mgr->create_block(opts, &wblock));
    DCHECK(wblock != nullptr);
    const auto* schema = _rowset_schema != nullptr ? _rowset_schema.get() : _context.tablet_schema;
    auto segment_writer = std::make_unique<SegmentWriter>(std::move(wblock), _num_segment, schema, _writer_options);
    RETURN_IF_ERROR(segment_writer->init());
    ++_num_segment;
    return std::move(segment_writer);
}

Status HorizontalBetaRowsetWriter::add_chunk(const vectorized::Chunk& chunk) {
    if (_segment_writer == nullptr) {
        auto segment_writer = _create_segment_writer();
        if (!segment_writer.ok()) return segment_writer.status();
        _segment_writer = std::move(segment_writer).value();
    } else if (_segment_writer->estimate_segment_size() >= config::max_segment_file_size ||
               _segment_writer->num_rows_written() + chunk.num_rows() >= _context.max_rows_per_segment) {
        RETURN_IF_ERROR(_flush_segment_writer(&_segment_writer));
        auto segment_writer = _create_segment_writer();
        if (!segment_writer.ok()) return segment_writer.status();
        _segment_writer = std::move(segment_writer).value();
    }

    RETURN_IF_ERROR(_segment_writer->append_chunk(chunk));
    _num_rows_written += static_cast<int64_t>(chunk.num_rows());
    _total_row_size += static_cast<int64_t>(chunk.bytes_usage());
    return Status::OK();
}

Status HorizontalBetaRowsetWriter::add_chunk_with_rssid(const vectorized::Chunk& chunk, const vector<uint32_t>& rssid) {
    if (_segment_writer == nullptr) {
        auto segment_writer = _create_segment_writer();
        if (!segment_writer.ok()) return segment_writer.status();
        _segment_writer = std::move(segment_writer).value();
    } else if (_segment_writer->estimate_segment_size() >= config::max_segment_file_size ||
               _segment_writer->num_rows_written() + chunk.num_rows() >= _context.max_rows_per_segment) {
        RETURN_IF_ERROR(_flush_segment_writer(&_segment_writer));
        auto segment_writer = _create_segment_writer();
        if (!segment_writer.ok()) return segment_writer.status();
        _segment_writer = std::move(segment_writer).value();
    }

    RETURN_IF_ERROR(_segment_writer->append_chunk(chunk));
    if (!_src_rssids) {
        _src_rssids = std::make_unique<vector<uint32_t>>();
    }
    _src_rssids->insert(_src_rssids->end(), rssid.begin(), rssid.end());
    _num_rows_written += static_cast<int64_t>(chunk.num_rows());
    _total_row_size += static_cast<int64_t>(chunk.bytes_usage());
    return Status::OK();
}

Status HorizontalBetaRowsetWriter::flush_chunk(const vectorized::Chunk& chunk) {
    auto segment_writer = _create_segment_writer();
    if (!segment_writer.ok()) return segment_writer.status();
    RETURN_IF_ERROR((*segment_writer)->append_chunk(chunk));
    {
        std::lock_guard<std::mutex> l(_lock);
        _num_rows_written += static_cast<int64_t>(chunk.num_rows());
        _total_row_size += static_cast<int64_t>(chunk.bytes_usage());
    }
    return _flush_segment_writer(&segment_writer.value());
}

Status HorizontalBetaRowsetWriter::flush_chunk_with_deletes(const vectorized::Chunk& upserts,
                                                            const vectorized::Column& deletes) {
    if (!deletes.empty()) {
        auto path = BetaRowset::segment_del_file_path(_context.rowset_path_prefix, _context.rowset_id, _num_segment);
        std::unique_ptr<fs::WritableBlock> wblock;
        fs::CreateBlockOptions opts({path});
        RETURN_IF_ERROR(_context.block_mgr->create_block(opts, &wblock));
        size_t sz = serde::ColumnArraySerde::max_serialized_size(deletes);
        // TODO(cbl): temp buffer doubles the memory usage, need to optimize
        std::vector<uint8_t> content(sz);
        if (serde::ColumnArraySerde::serialize(deletes, content.data()) == nullptr) {
            return Status::InternalError("deletes column serialize failed");
        }
        RETURN_IF_ERROR(wblock->append(Slice(content.data(), content.size())));
        RETURN_IF_ERROR(wblock->finalize());
        RETURN_IF_ERROR(wblock->close());
        _segment_has_deletes.resize(_num_segment + 1, false);
        _segment_has_deletes[_num_segment] = true;
    }
    return flush_chunk(upserts);
}

Status HorizontalBetaRowsetWriter::add_rowset(RowsetSharedPtr rowset) {
    assert(rowset->rowset_meta()->rowset_type() == BETA_ROWSET);
    RETURN_IF_ERROR(rowset->link_files_to(_context.rowset_path_prefix, _context.rowset_id));
    _num_rows_written += rowset->num_rows();
    _total_row_size += static_cast<int64_t>(rowset->total_row_size());
    _total_data_size += static_cast<int64_t>(rowset->rowset_meta()->data_disk_size());
    _total_index_size += static_cast<int64_t>(rowset->rowset_meta()->index_disk_size());
    _num_segment += static_cast<int>(rowset->num_segments());
    // TODO update zonemap
    if (rowset->rowset_meta()->has_delete_predicate()) {
        _rowset_meta->set_delete_predicate(rowset->rowset_meta()->delete_predicate());
    }
    return Status::OK();
}

Status HorizontalBetaRowsetWriter::add_rowset_for_linked_schema_change(RowsetSharedPtr rowset,
                                                                       const SchemaMapping& schema_mapping) {
    // TODO use schema_mapping to transfer zonemap
    return add_rowset(rowset);
}

Status HorizontalBetaRowsetWriter::flush() {
    if (_segment_writer != nullptr) {
        return _flush_segment_writer(&_segment_writer);
    }
    return Status::OK();
}

StatusOr<RowsetSharedPtr> HorizontalBetaRowsetWriter::build() {
    if (!_tmp_segment_files.empty()) {
        RETURN_IF_ERROR(_final_merge());
    }
    // When building a rowset, we must ensure that the current _segment_writer has been
    // flushed, that is, the current _segment_wirter is nullptr
    DCHECK(_segment_writer == nullptr) << "segment must be null when build rowset";
    return BetaRowsetWriter::build();
}

// why: when the data is large, multi segment files created, may be OVERLAPPINGed.
// what: do final merge for NONOVERLAPPING state among segment files
// when: segment files number larger than one, no delete files(for now, ignore it when just one segment)
// how: for final merge scenario, temporary files created at first, merge them, create final segment files.
Status HorizontalBetaRowsetWriter::_final_merge() {
    if (_num_segment == 1) {
        auto old_path = BetaRowset::segment_temp_file_path(_context.rowset_path_prefix, _context.rowset_id, 0);
        auto new_path = BetaRowset::segment_file_path(_context.rowset_path_prefix, _context.rowset_id, 0);
        auto st = _context.env->rename_file(old_path, new_path);
        RETURN_IF_ERROR_WITH_WARN(st, "Fail to rename file");
        return Status::OK();
    }

    if (!std::all_of(_segment_has_deletes.cbegin(), _segment_has_deletes.cend(), std::logical_not<bool>())) {
        return Status::NotSupported("multi-segments with delete not supported.");
    }

    MonotonicStopWatch timer;
    timer.start();

    auto schema = vectorized::ChunkHelper::convert_schema_to_format_v2(*_context.tablet_schema);

    std::vector<vectorized::ChunkIteratorPtr> seg_iterators;
    seg_iterators.reserve(_num_segment);

    vectorized::SegmentReadOptions seg_options;
    seg_options.block_mgr = _context.block_mgr;

    OlapReaderStatistics stats;
    seg_options.stats = &stats;

    for (int seg_id = 0; seg_id < _num_segment; ++seg_id) {
        std::string tmp_segment_file =
                BetaRowset::segment_temp_file_path(_context.rowset_path_prefix, _context.rowset_id, seg_id);
        auto segment_ptr =
                Segment::open(fs::fs_util::block_manager(), tmp_segment_file, seg_id, _context.tablet_schema);
        if (!segment_ptr.ok()) {
            LOG(WARNING) << "Fail to open " << tmp_segment_file << ": " << segment_ptr.status();
            return segment_ptr.status();
        }
        if ((*segment_ptr)->num_rows() == 0) {
            continue;
        }
        auto res = (*segment_ptr)->new_iterator(schema, seg_options);
        if (res.status().is_end_of_file()) {
            continue;
        } else if (!res.ok()) {
            return res.status();
        } else if (res.value() == nullptr) {
            continue;
        } else {
            seg_iterators.emplace_back(res.value());
        }
    }

    ChunkIteratorPtr itr = nullptr;
    // schema change vecotrized
    // schema change with sorting create temporary segment files first
    // merge them and create final segment files if _context.write_tmp is true
    if (_context.write_tmp) {
        if (_context.tablet_schema->keys_type() == KeysType::DUP_KEYS) {
            itr = new_heap_merge_iterator(seg_iterators);
        } else if (_context.tablet_schema->keys_type() == KeysType::UNIQUE_KEYS ||
                   _context.tablet_schema->keys_type() == KeysType::AGG_KEYS) {
            itr = new_aggregate_iterator(new_heap_merge_iterator(seg_iterators), 0);
        } else {
            return Status::NotSupported(fmt::format("HorizontalBetaRowsetWriter not support {} key type final merge",
                                                    _context.tablet_schema->keys_type()));
        }
        _context.write_tmp = false;
    } else {
        itr = new_aggregate_iterator(new_heap_merge_iterator(seg_iterators), 0);
    }
    itr->init_encoded_schema(vectorized::EMPTY_GLOBAL_DICTMAPS);

    auto chunk_shared_ptr = vectorized::ChunkHelper::new_chunk(schema, config::vector_chunk_size);
    auto chunk = chunk_shared_ptr.get();

    _num_segment = 0;
    _num_rows_written = 0;
    _total_data_size = 0;
    _total_index_size = 0;

    // since the segment already NONOVERLAPPING here, make the _create_segment_writer
    // method to create segment data files, rather than temporary segment files.
    _context.segments_overlap = NONOVERLAPPING;

    auto char_field_indexes = vectorized::ChunkHelper::get_char_field_indexes(schema);

    size_t total_rows = 0;
    size_t total_chunk = 0;
    while (true) {
        chunk->reset();
        auto st = itr->get_next(chunk);
        if (st.is_end_of_file()) {
            break;
        } else if (st.ok()) {
            vectorized::ChunkHelper::padding_char_columns(char_field_indexes, schema, *_context.tablet_schema, chunk);
            total_rows += chunk->num_rows();
            total_chunk++;
            add_chunk(*chunk);
        } else {
            return st;
        }
    }
    itr->close();
    flush();

    timer.stop();
    LOG(INFO) << "rowset writer final merge finished. tablet:" << _context.tablet_id
              << " #key:" << schema.num_key_fields() << " input("
              << "entry=" << seg_iterators.size() << " rows=" << stats.raw_rows_read
              << " bytes=" << PrettyPrinter::print(stats.bytes_read, TUnit::UNIT) << ") output(rows=" << total_rows
              << " chunk=" << total_chunk << " bytes=" << PrettyPrinter::print(total_data_size(), TUnit::UNIT)
              << ") duration: " << timer.elapsed_time() / 1000000 << "ms";

    for (const auto& tmp_segment_file : _tmp_segment_files) {
        // Even if an error is encountered, these files that have not been cleaned up
        // will be cleaned up by the GC background. So here we only print the error
        // message when we encounter an error.
        auto st = _context.env->delete_file(tmp_segment_file);
        RETURN_IF_ERROR_WITH_WARN(st, "Fail to delete segment temp file");
    }
    _tmp_segment_files.clear();

    return Status::OK();
}

Status HorizontalBetaRowsetWriter::_flush_segment_writer(std::unique_ptr<SegmentWriter>* segment_writer) {
    uint64_t segment_size = 0;
    uint64_t index_size = 0;
    uint64_t footer_position = 0;
    RETURN_IF_ERROR((*segment_writer)->finalize(&segment_size, &index_size, &footer_position));
    if (_context.tablet_schema->keys_type() == KeysType::PRIMARY_KEYS && _context.partial_update_tablet_schema) {
        uint64_t footer_size = segment_size - footer_position;
        auto* partial_rowset_footer = _rowset_txn_meta_pb->add_partial_rowset_footers();
        partial_rowset_footer->set_position(footer_position);
        partial_rowset_footer->set_size(footer_size);
    }
    {
        std::lock_guard<std::mutex> l(_lock);
        _total_data_size += static_cast<int64_t>(segment_size);
        _total_index_size += static_cast<int64_t>(index_size);
    }
    if (_src_rssids) {
        RETURN_IF_ERROR(flush_src_rssids(_segment_writer->segment_id()));
    }

    // check global_dict efficacy
    const auto& seg_global_dict_columns_valid_info = (*segment_writer)->global_dict_columns_valid_info();
    for (const auto& it : seg_global_dict_columns_valid_info) {
        if (!it.second) {
            _global_dict_columns_valid_info[it.first] = false;
        } else {
            if (const auto& iter = _global_dict_columns_valid_info.find(it.first);
                iter == _global_dict_columns_valid_info.end()) {
                _global_dict_columns_valid_info[it.first] = true;
            }
        }
    }

    (*segment_writer).reset();
    return Status::OK();
}

VerticalBetaRowsetWriter::VerticalBetaRowsetWriter(const RowsetWriterContext& context) : BetaRowsetWriter(context) {}

VerticalBetaRowsetWriter::~VerticalBetaRowsetWriter() {
    if (!_already_built) {
        for (auto& segment_writer : _segment_writers) {
            segment_writer.reset();
        }

        for (int i = 0; i < _num_segment; ++i) {
            auto path = BetaRowset::segment_file_path(_context.rowset_path_prefix, _context.rowset_id, i);
            // Even if an error is encountered, these files that have not been cleaned up
            // will be cleaned up by the GC background. So here we only print the error
            // message when we encounter an error.
            auto st = _context.env->delete_file(path);
            LOG_IF(WARNING, !st.ok()) << "Fail to delete file=" << path << ", " << st.to_string();
        }
        // if _already_built is false, we need to release rowset_id to avoid rowset_id leak
        StorageEngine::instance()->release_rowset_id(_context.rowset_id);
    }
}

Status VerticalBetaRowsetWriter::add_columns(const vectorized::Chunk& chunk,
                                             const std::vector<uint32_t>& column_indexes, bool is_key) {
    size_t chunk_num_rows = chunk.num_rows();
    if (_segment_writers.empty()) {
        DCHECK(is_key);
        auto segment_writer = _create_segment_writer(column_indexes, is_key);
        if (!segment_writer.ok()) return segment_writer.status();
        _segment_writers.emplace_back(std::move(segment_writer).value());
        _current_writer_index = 0;
        RETURN_IF_ERROR(_segment_writers[_current_writer_index]->append_chunk(chunk));
    } else if (is_key) {
        // key columns
        if (_segment_writers[_current_writer_index]->num_rows_written() + chunk_num_rows >=
            _context.max_rows_per_segment) {
            RETURN_IF_ERROR(_flush_columns(&_segment_writers[_current_writer_index]));
            auto segment_writer = _create_segment_writer(column_indexes, is_key);
            if (!segment_writer.ok()) return segment_writer.status();
            _segment_writers.emplace_back(std::move(segment_writer).value());
            ++_current_writer_index;
        }
        RETURN_IF_ERROR(_segment_writers[_current_writer_index]->append_chunk(chunk));
    } else {
        // non key columns
        uint32_t num_rows_written = _segment_writers[_current_writer_index]->num_rows_written();
        uint32_t segment_num_rows = _segment_writers[_current_writer_index]->num_rows();
        DCHECK_LE(num_rows_written, segment_num_rows);

        // init segment writer
        if (_current_writer_index == 0 && num_rows_written == 0) {
            RETURN_IF_ERROR(_segment_writers[_current_writer_index]->init(column_indexes, is_key));
        }

        if (num_rows_written + chunk_num_rows <= segment_num_rows) {
            RETURN_IF_ERROR(_segment_writers[_current_writer_index]->append_chunk(chunk));
        } else {
            // split into multi chunks and write into multi segments
            auto write_chunk = chunk.clone_empty();
            size_t num_left_rows = chunk_num_rows;
            size_t offset = 0;
            while (num_left_rows > 0) {
                if (segment_num_rows == num_rows_written) {
                    RETURN_IF_ERROR(_flush_columns(&_segment_writers[_current_writer_index]));
                    ++_current_writer_index;
                    RETURN_IF_ERROR(_segment_writers[_current_writer_index]->init(column_indexes, is_key));
                    num_rows_written = _segment_writers[_current_writer_index]->num_rows_written();
                    segment_num_rows = _segment_writers[_current_writer_index]->num_rows();
                }

                size_t write_size = segment_num_rows - num_rows_written;
                if (write_size > num_left_rows) {
                    write_size = num_left_rows;
                }
                write_chunk->append(chunk, offset, write_size);
                RETURN_IF_ERROR(_segment_writers[_current_writer_index]->append_chunk(*write_chunk));
                write_chunk->reset();
                num_left_rows -= write_size;
                offset += write_size;
                num_rows_written = _segment_writers[_current_writer_index]->num_rows_written();
            }
            DCHECK_EQ(0, num_left_rows);
            DCHECK_EQ(offset, chunk_num_rows);
        }
    }

    if (is_key) {
        _num_rows_written += static_cast<int64_t>(chunk_num_rows);
    }
    _total_row_size += static_cast<int64_t>(chunk.bytes_usage());
    return Status::OK();
}

Status VerticalBetaRowsetWriter::add_columns_with_rssid(const vectorized::Chunk& chunk,
                                                        const std::vector<uint32_t>& column_indexes,
                                                        const std::vector<uint32_t>& rssid) {
    RETURN_IF_ERROR(add_columns(chunk, column_indexes, true));
    if (!_src_rssids) {
        _src_rssids = std::make_unique<std::vector<uint32_t>>();
    }
    _src_rssids->insert(_src_rssids->end(), rssid.begin(), rssid.end());
    return Status::OK();
}

Status VerticalBetaRowsetWriter::flush_columns() {
    if (_segment_writers.empty()) {
        return Status::OK();
    }

    DCHECK(_segment_writers[_current_writer_index]);
    RETURN_IF_ERROR(_flush_columns(&_segment_writers[_current_writer_index]));
    _current_writer_index = 0;
    return Status::OK();
}

Status VerticalBetaRowsetWriter::final_flush() {
    if (_segment_writers.empty()) {
        return Status::OK();
    }
    for (auto& segment_writer : _segment_writers) {
        uint64_t segment_size = 0;
        if (auto st = segment_writer->finalize_footer(&segment_size); !st.ok()) {
            LOG(WARNING) << "Fail to finalize segment footer, " << st;
            return st;
        }
        {
            std::lock_guard<std::mutex> l(_lock);
            _total_data_size += static_cast<int64_t>(segment_size);
        }

        // check global_dict efficacy
        const auto& seg_global_dict_columns_valid_info = segment_writer->global_dict_columns_valid_info();
        for (const auto& it : seg_global_dict_columns_valid_info) {
            if (!it.second) {
                _global_dict_columns_valid_info[it.first] = false;
            } else {
                if (const auto& iter = _global_dict_columns_valid_info.find(it.first);
                    iter == _global_dict_columns_valid_info.end()) {
                    _global_dict_columns_valid_info[it.first] = true;
                }
            }
        }

        segment_writer.reset();
    }
    return Status::OK();
}

StatusOr<std::unique_ptr<SegmentWriter>> VerticalBetaRowsetWriter::_create_segment_writer(
        const std::vector<uint32_t>& column_indexes, bool is_key) {
    std::lock_guard<std::mutex> l(_lock);
    std::string path = BetaRowset::segment_file_path(_context.rowset_path_prefix, _context.rowset_id, _num_segment);
    std::unique_ptr<fs::WritableBlock> wblock;
    fs::CreateBlockOptions opts({path});
    RETURN_IF_ERROR(_context.block_mgr->create_block(opts, &wblock));

    DCHECK(wblock != nullptr);
    const auto* schema = _rowset_schema != nullptr ? _rowset_schema.get() : _context.tablet_schema;
    auto segment_writer = std::make_unique<SegmentWriter>(std::move(wblock), _num_segment, schema, _writer_options);
    RETURN_IF_ERROR(segment_writer->init(column_indexes, is_key));
    ++_num_segment;
    return std::move(segment_writer);
}

Status VerticalBetaRowsetWriter::_flush_columns(std::unique_ptr<SegmentWriter>* segment_writer) {
    uint64_t index_size = 0;
    RETURN_IF_ERROR((*segment_writer)->finalize_columns(&index_size));
    {
        std::lock_guard<std::mutex> l(_lock);
        _total_index_size += static_cast<int64_t>(index_size);
    }
    if (_src_rssids) {
        return flush_src_rssids((*segment_writer)->segment_id());
    }
    return Status::OK();
}

} // namespace starrocks
