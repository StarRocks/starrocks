// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/beta_rowset.cpp

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

#include "storage/rowset/beta_rowset.h"

#include <unistd.h> // for link()
#include <util/file_utils.h>

#include <cstdio> // for remove()
#include <memory>
#include <set>

#include "gutil/strings/substitute.h"
#include "storage/rowset/vectorized/rowset_options.h"
#include "storage/rowset/vectorized/segment_options.h"
#include "storage/storage_engine.h"
#include "storage/update_manager.h"
#include "storage/utils.h"
#include "storage/vectorized/chunk_helper.h"
#include "storage/vectorized/chunk_iterator.h"
#include "storage/vectorized/delete_predicates.h"
#include "storage/vectorized/empty_iterator.h"
#include "storage/vectorized/merge_iterator.h"
#include "storage/vectorized/projection_iterator.h"
#include "storage/vectorized/union_iterator.h"
#include "util/file_utils.h"

namespace starrocks {

std::string BetaRowset::segment_file_path(const std::string& dir, const RowsetId& rowset_id, int segment_id) {
    return strings::Substitute("$0/$1_$2.dat", dir, rowset_id.to_string(), segment_id);
}

std::string BetaRowset::segment_temp_file_path(const std::string& dir, const RowsetId& rowset_id, int segment_id) {
    return strings::Substitute("$0/$1_$2.dat.tmp", dir, rowset_id.to_string(), segment_id);
}

std::string BetaRowset::segment_del_file_path(const std::string& dir, const RowsetId& rowset_id, int segment_id) {
    return strings::Substitute("$0/$1_$2.del", dir, rowset_id.to_string(), segment_id);
}

std::string BetaRowset::segment_srcrssid_file_path(const std::string& dir, const RowsetId& rowset_id, int segment_id) {
    return strings::Substitute("$0/$1_$2.rssid", dir, rowset_id.to_string(), segment_id);
}

BetaRowset::BetaRowset(const TabletSchema* schema, string rowset_path, RowsetMetaSharedPtr rowset_meta)
        : Rowset(schema, std::move(rowset_path), std::move(rowset_meta)) {}

Status BetaRowset::init() {
    return Status::OK();
}

Status BetaRowset::do_load() {
    fs::BlockManager* block_mgr = fs::fs_util::block_manager();

    _segments.clear();
    size_t footer_size_hint = 16 * 1024;
    for (int seg_id = 0; seg_id < num_segments(); ++seg_id) {
        std::string seg_path = segment_file_path(_rowset_path, rowset_id(), seg_id);
        auto res = Segment::open(block_mgr, seg_path, seg_id, _schema, &footer_size_hint);
        if (!res.ok()) {
            LOG(WARNING) << "Fail to open " << seg_path << ": " << res.status();
            _segments.clear();
            return res.status();
        }
        _segments.push_back(std::move(res).value());
    }
    return Status::OK();
}

Status BetaRowset::remove() {
    VLOG(1) << "Removing files in rowset id=" << unique_id() << " version=" << start_version() << "-" << end_version()
            << " tablet_id=" << _rowset_meta->tablet_id();
    bool success = true;
    for (int i = 0; i < num_segments(); ++i) {
        std::string path = segment_file_path(_rowset_path, rowset_id(), i);
        VLOG(1) << "Deleting " << path;
        // TODO(lingbin): use Env API
        if (::remove(path.c_str()) != 0) {
            PLOG(WARNING) << "Fail to delete " << path;
            success = false;
        }
    }
    for (int i = 0; i < num_delete_files(); ++i) {
        std::string del_path = segment_del_file_path(_rowset_path, rowset_id(), i);
        VLOG(1) << "Deleting " << del_path;
        if (::remove(del_path.c_str()) != 0) {
            PLOG(WARNING) << "Fail to delete " << del_path;
            success = false;
        }
    }
    for (int i = 0; i < num_segments(); ++i) {
        std::string path = segment_srcrssid_file_path(_rowset_path, rowset_id(), i);
        if (::access(path.c_str(), F_OK) == 0) {
            VLOG(1) << "Deleting " << path;
            if (::remove(path.c_str()) != 0) {
                PLOG(WARNING) << "Fail to delete " << path;
                success = false;
            }
        }
    }
    if (!success) {
        LOG(WARNING) << "Fail to remove files in rowset id=" << unique_id();
        return Status::IOError(fmt::format("Fail to remove files. rowset_id: {}", unique_id()));
    }
    return Status::OK();
}

void BetaRowset::do_close() {
    _segments.clear();
}

Status BetaRowset::link_files_to(const std::string& dir, RowsetId new_rowset_id) {
    for (int i = 0; i < num_segments(); ++i) {
        std::string dst_link_path = segment_file_path(dir, new_rowset_id, i);
        std::string src_file_path = segment_file_path(_rowset_path, rowset_id(), i);
        if (link(src_file_path.c_str(), dst_link_path.c_str()) != 0) {
            PLOG(WARNING) << "Fail to link " << src_file_path << " to " << dst_link_path;
            return Status::RuntimeError("Fail to link segment data file");
        }
    }
    for (int i = 0; i < num_delete_files(); ++i) {
        std::string src_file_path = segment_del_file_path(_rowset_path, rowset_id(), i);
        std::string dst_link_path = segment_del_file_path(dir, new_rowset_id, i);
        if (link(src_file_path.c_str(), dst_link_path.c_str()) != 0) {
            PLOG(WARNING) << "Fail to link " << src_file_path << " to " << dst_link_path;
            return Status::RuntimeError("Fail to link segment delete file");
        }
    }
    return Status::OK();
}

Status BetaRowset::copy_files_to(const std::string& dir) {
    for (int i = 0; i < num_segments(); ++i) {
        std::string dst_path = segment_file_path(dir, rowset_id(), i);
        if (FileUtils::check_exist(dst_path)) {
            LOG(WARNING) << "Path already exist: " << dst_path;
            return Status::AlreadyExist(fmt::format("Path already exist: {}", dst_path));
        }
        std::string src_path = segment_file_path(_rowset_path, rowset_id(), i);
        if (!FileUtils::copy_file(src_path, dst_path).ok()) {
            LOG(WARNING) << "Error to copy file. src:" << src_path << ", dst:" << dst_path << ", errno=" << Errno::no();
            return Status::IOError(fmt::format("Error to copy file. src: {}, dst: {}, error:{} ", src_path, dst_path,
                                               std::strerror(Errno::no())));
        }
    }
    for (int i = 0; i < num_delete_files(); ++i) {
        std::string src_path = segment_del_file_path(_rowset_path, rowset_id(), i);
        if (FileUtils::check_exist(src_path)) {
            std::string dst_path = segment_del_file_path(dir, rowset_id(), i);
            if (FileUtils::check_exist(dst_path)) {
                LOG(WARNING) << "Path already exist: " << dst_path;
                return Status::AlreadyExist(fmt::format("Path already exist: {}", dst_path));
            }
            if (!FileUtils::copy_file(src_path, dst_path).ok()) {
                LOG(WARNING) << "Error to copy file. src:" << src_path << ", dst:" << dst_path
                             << ", errno=" << Errno::no();
                return Status::IOError(fmt::format("Error to copy file. src: {}, dst: {}, error:{} ", src_path,
                                                   dst_path, std::strerror(Errno::no())));
            }
        }
    }
    return Status::OK();
}

bool BetaRowset::check_path(const std::string& path) {
    std::set<std::string> valid_paths;
    for (int i = 0; i < num_segments(); ++i) {
        valid_paths.insert(segment_file_path(_rowset_path, rowset_id(), i));
    }
    return valid_paths.find(path) != valid_paths.end();
}

class SegmentIteratorWrapper : public vectorized::ChunkIterator {
public:
    SegmentIteratorWrapper(std::shared_ptr<Rowset> rowset, vectorized::ChunkIteratorPtr iter)
            : ChunkIterator(iter->schema(), iter->chunk_size()), _guard(std::move(rowset)), _iter(std::move(iter)) {}

    void close() override {
        _iter->close();
        _iter.reset();
    }

    virtual Status init_encoded_schema(vectorized::ColumnIdToGlobalDictMap& dict_maps) override {
        RETURN_IF_ERROR(vectorized::ChunkIterator::init_encoded_schema(dict_maps));
        return _iter->init_encoded_schema(dict_maps);
    }

    virtual Status init_output_schema(const std::unordered_set<uint32_t>& unused_output_column_ids) override {
        ChunkIterator::init_output_schema(unused_output_column_ids);
        return _iter->init_output_schema(unused_output_column_ids);
    }

protected:
    Status do_get_next(vectorized::Chunk* chunk) override { return _iter->get_next(chunk); }
    Status do_get_next(vectorized::Chunk* chunk, vector<uint32_t>* rowid) override {
        return _iter->get_next(chunk, rowid);
    }

private:
    RowsetReleaseGuard _guard;
    vectorized::ChunkIteratorPtr _iter;
};

StatusOr<vectorized::ChunkIteratorPtr> BetaRowset::new_iterator(const vectorized::Schema& schema,
                                                                const vectorized::RowsetReadOptions& options) {
    std::vector<vectorized::ChunkIteratorPtr> seg_iters;
    RETURN_IF_ERROR(get_segment_iterators(schema, options, &seg_iters));
    if (seg_iters.empty()) {
        return vectorized::new_empty_iterator(schema, options.chunk_size);
    } else if (options.sorted) {
        return vectorized::new_heap_merge_iterator(seg_iters);
    } else {
        return vectorized::new_union_iterator(std::move(seg_iters));
    }
}

Status BetaRowset::get_segment_iterators(const vectorized::Schema& schema, const vectorized::RowsetReadOptions& options,
                                         std::vector<vectorized::ChunkIteratorPtr>* segment_iterators) {
    RowsetReleaseGuard guard(shared_from_this());

    RETURN_IF_ERROR(load());

    vectorized::SegmentReadOptions seg_options;
    seg_options.block_mgr = options.block_mgr;
    seg_options.stats = options.stats;
    seg_options.ranges = options.ranges;
    seg_options.predicates = options.predicates;
    seg_options.use_page_cache = options.use_page_cache;
    seg_options.profile = options.profile;
    seg_options.reader_type = options.reader_type;
    seg_options.chunk_size = options.chunk_size;
    seg_options.global_dictmaps = options.global_dictmaps;
    seg_options.unused_output_column_ids = options.unused_output_column_ids;
    if (options.delete_predicates != nullptr) {
        seg_options.delete_predicates = options.delete_predicates->get_predicates(end_version());
    }
    if (options.is_primary_keys) {
        seg_options.is_primary_keys = true;
        seg_options.tablet_id = rowset_meta()->tablet_id();
        seg_options.rowset_id = rowset_meta()->get_rowset_seg_id();
        seg_options.version = options.version;
        seg_options.meta = options.meta;
    }

    auto segment_schema = schema;
    // Append the columns with delete condition to segment schema.
    std::set<ColumnId> delete_columns;
    seg_options.delete_predicates.get_column_ids(&delete_columns);
    for (ColumnId cid : delete_columns) {
        const TabletColumn& col = options.tablet_schema->column(cid);
        if (segment_schema.get_field_by_name(std::string(col.name())) == nullptr) {
            auto f = vectorized::ChunkHelper::convert_field_to_format_v2(cid, col);
            segment_schema.append(std::make_shared<vectorized::Field>(std::move(f)));
        }
    }

    std::vector<vectorized::ChunkIteratorPtr> tmp_seg_iters;
    tmp_seg_iters.reserve(num_segments());
    if (options.stats) {
        options.stats->segments_read_count += num_segments();
    }
    for (auto& seg_ptr : segments()) {
        if (seg_ptr->num_rows() == 0) {
            continue;
        }
        auto res = seg_ptr->new_iterator(segment_schema, seg_options);
        if (res.status().is_end_of_file()) {
            continue;
        }
        if (!res.ok()) {
            return res.status();
        }
        if (segment_schema.num_fields() > schema.num_fields()) {
            tmp_seg_iters.emplace_back(vectorized::new_projection_iterator(schema, std::move(res).value()));
        } else {
            tmp_seg_iters.emplace_back(std::move(res).value());
        }
    }

    auto this_rowset = shared_from_this();
    if (tmp_seg_iters.empty()) {
        // nothing to do
    } else if (rowset_meta()->is_segments_overlapping()) {
        for (auto& iter : tmp_seg_iters) {
            auto wrapper = std::make_shared<SegmentIteratorWrapper>(this_rowset, std::move(iter));
            segment_iterators->emplace_back(std::move(wrapper));
        }
    } else {
        auto iter = vectorized::new_union_iterator(std::move(tmp_seg_iters));
        auto wrapper = std::make_shared<SegmentIteratorWrapper>(this_rowset, std::move(iter));
        segment_iterators->emplace_back(std::move(wrapper));
    }
    return Status::OK();
}

StatusOr<std::vector<vectorized::ChunkIteratorPtr>> BetaRowset::get_segment_iterators2(const vectorized::Schema& schema,
                                                                                       KVStore* meta, int64_t version,
                                                                                       OlapReaderStatistics* stats) {
    RETURN_IF_ERROR(load());

    vectorized::SegmentReadOptions seg_options;
    seg_options.block_mgr = fs::fs_util::block_manager();
    seg_options.stats = stats;
    seg_options.is_primary_keys = meta != nullptr;
    seg_options.tablet_id = rowset_meta()->tablet_id();
    seg_options.rowset_id = rowset_meta()->get_rowset_seg_id();
    seg_options.version = version;
    seg_options.meta = meta;

    std::vector<vectorized::ChunkIteratorPtr> seg_iterators(num_segments());
    TabletSegmentId tsid;
    tsid.tablet_id = rowset_meta()->tablet_id();
    for (int64_t i = 0; i < num_segments(); i++) {
        auto& seg_ptr = segments()[i];
        if (seg_ptr->num_rows() == 0) {
            continue;
        }
        auto res = seg_ptr->new_iterator(schema, seg_options);
        if (res.status().is_end_of_file()) {
            continue;
        }
        if (!res.ok()) {
            return res.status();
        }
        seg_iterators[i] = std::move(res).value();
    }
    return seg_iterators;
}

Status BetaRowset::reload() {
    fs::BlockManager* block_mgr = fs::fs_util::block_manager();

    _segments.clear();
    size_t footer_size_hint = 16 * 1024;
    for (int seg_id = 0; seg_id < num_segments(); ++seg_id) {
        std::string seg_path = segment_file_path(_rowset_path, rowset_id(), seg_id);
        block_mgr->erase_block_cache(seg_path);
        auto res = Segment::open(block_mgr, seg_path, seg_id, _schema, &footer_size_hint);
        if (!res.ok()) {
            LOG(WARNING) << "Fail to open " << seg_path << ": " << res.status();
            _segments.clear();
            return res.status();
        }
        _segments.push_back(std::move(res).value());
    }
    return Status::OK();
}

} // namespace starrocks
