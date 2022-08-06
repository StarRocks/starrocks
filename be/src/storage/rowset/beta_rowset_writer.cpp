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
#include "storage/fs/fs_util.h"
#include "storage/olap_define.h"
#include "storage/row.h"        // ContiguousRow
#include "storage/row_cursor.h" // RowCursor
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/segment_v2/segment_writer.h"
#include "storage/rowset/vectorized/segment_options.h"
#include "storage/storage_engine.h"
#include "storage/vectorized/aggregate_iterator.h"
#include "storage/vectorized/chunk_helper.h"
#include "storage/vectorized/merge_iterator.h"
#include "storage/vectorized/type_utils.h"
#include "storage/vectorized/union_iterator.h"
#include "util/defer_op.h"
#include "util/pretty_printer.h"

namespace starrocks {

// TODO(lingbin): Should be a conf that can be dynamically adjusted, or a member in the context
const uint32_t MAX_SEGMENT_SIZE =
        static_cast<uint32_t>(OLAP_MAX_COLUMN_SEGMENT_FILE_SIZE * OLAP_COLUMN_FILE_SEGMENT_SIZE_SCALE);

BetaRowsetWriter::BetaRowsetWriter(const RowsetWriterContext& context)
        : _context(context),
          _rowset_meta(nullptr),
          _num_segment(0),
          _segment_writer(nullptr),
          _num_rows_written(0),
          _total_row_size(0),
          _total_data_size(0),
          _total_index_size(0) {}

BetaRowsetWriter::~BetaRowsetWriter() {
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

OLAPStatus BetaRowsetWriter::init() {
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
        _rowset_meta->set_version_hash(_context.version_hash);
    }
    _rowset_meta->set_tablet_uid(_context.tablet_uid);
    return OLAP_SUCCESS;
}

template <typename RowType>
OLAPStatus BetaRowsetWriter::_add_row(const RowType& row) {
    if (PREDICT_FALSE(_segment_writer == nullptr)) {
        _segment_writer = _create_segment_writer();
        if (_segment_writer == nullptr) {
            return OLAP_ERR_INIT_FAILED;
        }
    }
    // TODO update rowset's zonemap
    auto s = _segment_writer->append_row(row);
    if (PREDICT_FALSE(!s.ok())) {
        LOG(WARNING) << "Fail to append row, " << s.to_string();
        return OLAP_ERR_WRITER_DATA_WRITE_ERROR;
    }
    if (PREDICT_FALSE(_segment_writer->estimate_segment_size() >= MAX_SEGMENT_SIZE ||
                      _segment_writer->num_rows_written() >= _context.max_rows_per_segment)) {
        RETURN_NOT_OK(_flush_segment_writer(&_segment_writer));
    }
    ++_num_rows_written;
    // schema_size may be inaccurate in non-vectorized loading
    _total_row_size += row.schema()->schema_size();
    return OLAP_SUCCESS;
}

template OLAPStatus BetaRowsetWriter::_add_row(const RowCursor& row);
template OLAPStatus BetaRowsetWriter::_add_row(const ContiguousRow& row);

OLAPStatus BetaRowsetWriter::add_rowset(RowsetSharedPtr rowset) {
    assert(rowset->rowset_meta()->rowset_type() == BETA_ROWSET);
    if (!rowset->link_files_to(_context.rowset_path_prefix, _context.rowset_id).ok()) {
        return OLAP_ERR_OTHER_ERROR;
    }
    _num_rows_written += rowset->num_rows();
    _total_row_size += rowset->total_row_size();
    _total_data_size += rowset->rowset_meta()->data_disk_size();
    _total_index_size += rowset->rowset_meta()->index_disk_size();
    _num_segment += rowset->num_segments();
    // TODO update zonemap
    if (rowset->rowset_meta()->has_delete_predicate()) {
        _rowset_meta->set_delete_predicate(rowset->rowset_meta()->delete_predicate());
    }
    return OLAP_SUCCESS;
}

OLAPStatus BetaRowsetWriter::add_rowset_for_linked_schema_change(RowsetSharedPtr rowset,
                                                                 const SchemaMapping& schema_mapping) {
    // TODO use schema_mapping to transfer zonemap
    return add_rowset(rowset);
}

OLAPStatus BetaRowsetWriter::flush() {
    if (_segment_writer != nullptr) {
        RETURN_NOT_OK(_flush_segment_writer(&_segment_writer));
    }
    return OLAP_SUCCESS;
}

// why: when the data is large, multi segment files created, may be OVERLAPPINGed.
// what: do final merge for NONOVERLAPPING state among segment files
// when: segment files number larger than one, no delete files(for now, ignore it when just one segment)
// how: for final merge scenario, temporary files created at first, merge them, create final segment files.
Status BetaRowsetWriter::_final_merge() {
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

        auto segment_ptr = segment_v2::Segment::open(ExecEnv::GetInstance()->tablet_meta_mem_tracker(),
                                                     fs::fs_util::block_manager(), tmp_segment_file, seg_id,
                                                     _context.tablet_schema);
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
            itr = new_merge_iterator(seg_iterators);
        } else if (_context.tablet_schema->keys_type() == KeysType::UNIQUE_KEYS ||
                   _context.tablet_schema->keys_type() == KeysType::AGG_KEYS) {
            itr = new_aggregate_iterator(new_merge_iterator(seg_iterators), 0);
        } else {
            return Status::NotSupported(fmt::format("HorizontalBetaRowsetWriter not support {} key type final merge",
                                                    _context.tablet_schema->keys_type()));
        }
        _context.write_tmp = false;
    } else {
        itr = new_aggregate_iterator(new_merge_iterator(seg_iterators), 0);
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

RowsetSharedPtr BetaRowsetWriter::build() {
    if (!_tmp_segment_files.empty()) {
        auto s = _final_merge();
        if (!s.ok()) {
            LOG(WARNING) << "final merge error: " << s.to_string();
            return nullptr;
        }
    }
    // When building a rowset, we must ensure that the current _segment_writer has been
    // flushed, that is, the current _segment_wirter is nullptr
    DCHECK(_segment_writer == nullptr) << "segment must be null when build rowset";
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
        if (_segment_has_deletes.size() >= 1 && _segment_has_deletes[0]) {
            _rowset_meta->set_num_delete_files(1);
        } else {
            _rowset_meta->set_num_delete_files(0);
        }
        _rowset_meta->set_segments_overlap(NONOVERLAPPING);
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
    auto status =
            RowsetFactory::create_rowset(_context.tablet_schema, _context.rowset_path_prefix, _rowset_meta, &rowset);
    if (!status.ok()) {
        LOG(WARNING) << "Fail to create rowset: " << status;
        return nullptr;
    }
    _already_built = true;
    return rowset;
}

std::unique_ptr<SegmentWriter> BetaRowsetWriter::_create_segment_writer() {
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
    Status st = _context.block_mgr->create_block(opts, &wblock);
    if (!st.ok()) {
        LOG(WARNING) << "Fail to create writable block=" << path << ", " << st.to_string();
        return nullptr;
    }

    DCHECK(wblock != nullptr);
    segment_v2::SegmentWriterOptions writer_options;
    writer_options.storage_format_version = _context.storage_format_version;
    const auto* schema = _rowset_schema != nullptr ? _rowset_schema.get() : _context.tablet_schema;
    writer_options.global_dicts = _context.global_dicts != nullptr ? _context.global_dicts : nullptr;
    std::unique_ptr<SegmentWriter> segment_writer =
            std::make_unique<segment_v2::SegmentWriter>(std::move(wblock), _num_segment, schema, writer_options);
    // TODO set write_mbytes_per_sec based on writer type (load/base compaction/cumulative compaction)
    auto s = segment_writer->init(config::push_write_mbytes_per_sec);
    if (!s.ok()) {
        LOG(WARNING) << "Fail to init segment writer, " << s.to_string();
        segment_writer.reset(nullptr);
        return nullptr;
    }
    ++_num_segment;
    return segment_writer;
}

OLAPStatus BetaRowsetWriter::_flush_segment_writer(std::unique_ptr<segment_v2::SegmentWriter>* segment_writer) {
    uint64_t segment_size = 0;
    uint64_t index_size = 0;
    Status s = (*segment_writer)->finalize(&segment_size, &index_size);
    if (!s.ok()) {
        LOG(WARNING) << "Fail to finalize segment, " << s.to_string();
        return OLAP_ERR_WRITER_DATA_WRITE_ERROR;
    }
    {
        std::lock_guard<std::mutex> l(_lock);
        _total_data_size += segment_size;
        _total_index_size += index_size;
    }
    if (_src_rssids) {
        Status st = _flush_src_rssids();
        if (!st.ok()) {
            LOG(WARNING) << "_flush_src_rssids error: " << st.to_string();
            return OLAP_ERR_IO_ERROR;
        }
    }

    // check global_dict efficacy
    const auto& seg_global_dict_columns_valid_info = (*segment_writer)->global_dict_columns_valid_info();
    for (const auto& it : seg_global_dict_columns_valid_info) {
        if (it.second == false) {
            _global_dict_columns_valid_info[it.first] = false;
        } else {
            if (const auto& iter = _global_dict_columns_valid_info.find(it.first);
                iter == _global_dict_columns_valid_info.end()) {
                _global_dict_columns_valid_info[it.first] = true;
            }
        }
    }

    (*segment_writer).reset();
    return OLAP_SUCCESS;
}

Status BetaRowsetWriter::_flush_src_rssids() {
    auto path = BetaRowset::segment_srcrssid_file_path(_context.rowset_path_prefix, _context.rowset_id,
                                                       _segment_writer->segment_id());
    std::unique_ptr<fs::WritableBlock> wblock;
    fs::CreateBlockOptions opts({path});
    Status st = _context.block_mgr->create_block(opts, &wblock);
    if (!st.ok()) {
        return st;
    }
    st = wblock->append(Slice((const char*)(_src_rssids->data()), _src_rssids->size() * sizeof(uint32_t)));
    if (!st.ok()) {
        return Status::IOError("_flush_src_rssids WritableBlock.append error");
    }
    st = wblock->finalize();
    if (!st.ok()) {
        return Status::IOError("_flush_src_rssids WritableBlock.finalize error");
    }
    st = wblock->close();
    if (!st.ok()) {
        return Status::IOError("_flush_src_rssids WritableBlock.close error");
    }
    _src_rssids->clear();
    return Status::OK();
}

OLAPStatus BetaRowsetWriter::add_chunk(const vectorized::Chunk& chunk) {
    if (_segment_writer == nullptr) {
        _segment_writer = _create_segment_writer();
    } else if (_segment_writer->estimate_segment_size() >= MAX_SEGMENT_SIZE ||
               _segment_writer->num_rows_written() + chunk.num_rows() >= _context.max_rows_per_segment) {
        RETURN_NOT_OK(_flush_segment_writer(&_segment_writer));
        _segment_writer = _create_segment_writer();
    }
    if (_segment_writer == nullptr) {
        return OLAP_ERR_INIT_FAILED;
    }
    auto s = _segment_writer->append_chunk(chunk);
    if (!s.ok()) {
        LOG(WARNING) << "Fail to append chunk, " << s.to_string();
        return OLAP_ERR_WRITER_DATA_WRITE_ERROR;
    }
    _num_rows_written += chunk.num_rows();
    _total_row_size += chunk.bytes_usage();
    return OLAP_SUCCESS;
}

OLAPStatus BetaRowsetWriter::add_chunk_with_rssid(const vectorized::Chunk& chunk, const vector<uint32_t>& rssid) {
    if (_segment_writer == nullptr) {
        _segment_writer = _create_segment_writer();
    } else if (_segment_writer->estimate_segment_size() >= MAX_SEGMENT_SIZE ||
               _segment_writer->num_rows_written() + chunk.num_rows() >= _context.max_rows_per_segment) {
        RETURN_NOT_OK(_flush_segment_writer(&_segment_writer));
        _segment_writer = _create_segment_writer();
    }
    if (_segment_writer == nullptr) {
        return OLAP_ERR_INIT_FAILED;
    }
    auto s = _segment_writer->append_chunk(chunk);
    if (!_src_rssids) {
        _src_rssids = std::make_unique<vector<uint32_t>>();
    }
    _src_rssids->insert(_src_rssids->end(), rssid.begin(), rssid.end());
    if (!s.ok()) {
        LOG(WARNING) << "Fail to append chunk, " << s.to_string();
        return OLAP_ERR_WRITER_DATA_WRITE_ERROR;
    }
    _num_rows_written += chunk.num_rows();
    return OLAP_SUCCESS;
}

OLAPStatus BetaRowsetWriter::flush_chunk(const vectorized::Chunk& chunk) {
    // create segment writer
    std::unique_ptr<segment_v2::SegmentWriter> segment_writer = _create_segment_writer();
    if (segment_writer == nullptr) {
        return OLAP_ERR_INIT_FAILED;
    }

    // append chunk
    auto s = segment_writer->append_chunk(chunk);
    if (!s.ok()) {
        LOG(WARNING) << "Fail to append chunk, " << s.to_string();
        return OLAP_ERR_WRITER_DATA_WRITE_ERROR;
    }
    {
        std::lock_guard<std::mutex> l(_lock);
        _num_rows_written += chunk.num_rows();
        _total_row_size += chunk.bytes_usage();
    }

    // flush
    RETURN_NOT_OK(_flush_segment_writer(&segment_writer));
    return OLAP_SUCCESS;
}

OLAPStatus BetaRowsetWriter::flush_chunk_with_deletes(const vectorized::Chunk& upserts,
                                                      const vectorized::Column& deletes) {
    if (!deletes.empty()) {
        auto path = BetaRowset::segment_del_file_path(_context.rowset_path_prefix, _context.rowset_id, _num_segment);
        std::unique_ptr<fs::WritableBlock> wblock;
        fs::CreateBlockOptions opts({path});
        Status st = _context.block_mgr->create_block(opts, &wblock);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to create writable block=" << path << ", " << st.to_string();
            return OLAP_ERR_INIT_FAILED;
        }
        size_t sz = deletes.serialize_size();
        // TODO(cbl): temp buffer doubles the memory usage, need to optimize
        string content;
        content.resize(sz);
        const_cast<vectorized::Column&>(deletes).serialize_column((uint8_t*)(content.data()));
        st = wblock->append(Slice(content.data(), content.size()));
        if (!st.ok()) {
            return OLAP_ERR_WRITER_DATA_WRITE_ERROR;
        }
        st = wblock->finalize();
        if (!st.ok()) {
            return OLAP_ERR_WRITER_DATA_WRITE_ERROR;
        }
        st = wblock->close();
        if (!st.ok()) {
            return OLAP_ERR_WRITER_DATA_WRITE_ERROR;
        }
        _segment_has_deletes.resize(_num_segment + 1, false);
        _segment_has_deletes[_num_segment] = true;
    }
    return flush_chunk(upserts);
}

} // namespace starrocks
