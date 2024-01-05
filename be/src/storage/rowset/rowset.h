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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/rowset.h

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

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "common/statusor.h"
#include "gen_cpp/olap_file.pb.h"
#include "gutil/macros.h"
#include "gutil/strings/substitute.h"
#include "runtime/mem_tracker.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/segment.h"

namespace starrocks {

class DataDir;
class OlapTuple;
class PrimaryIndex;
class Rowset;
using RowsetSharedPtr = std::shared_ptr<Rowset>;
class RowsetFactory;
class RowsetReadOptions;
class TabletSchema;
class KVStore;

class Schema;
class ChunkIterator;
using ChunkIteratorPtr = std::shared_ptr<ChunkIterator>;

// the rowset state transfer graph:
//    ROWSET_UNLOADED    <--|
//          |               |
//          v               |
//    ROWSET_LOADED         |
//          |               |
//          v               |
//    ROWSET_UNLOADING   -->|
enum RowsetState {
    // state for new created rowset
    ROWSET_UNLOADED,
    // state after load() called
    ROWSET_LOADED,
    // state for closed() called but owned by some readers
    ROWSET_UNLOADING
};

class RowsetStateMachine {
public:
    RowsetStateMachine() = default;

    Status on_load() {
        switch (_rowset_state) {
        case ROWSET_UNLOADED:
            _rowset_state = ROWSET_LOADED;
            break;
        default:
            return Status::InternalError(strings::Substitute("rowset state on_load error, $0", _rowset_state));
        }
        return Status::OK();
    }

    Status on_close(uint64_t refs_by_reader) {
        switch (_rowset_state) {
        case ROWSET_LOADED:
            if (refs_by_reader == 0) {
                _rowset_state = ROWSET_UNLOADED;
            } else {
                _rowset_state = ROWSET_UNLOADING;
            }
            break;

        default:
            return Status::InternalError(strings::Substitute("rowset state on_close error, $0", _rowset_state));
        }
        return Status::OK();
    }

    Status on_release() {
        switch (_rowset_state) {
        case ROWSET_UNLOADING:
            _rowset_state = ROWSET_UNLOADED;
            break;
        default:
            return Status::InternalError(strings::Substitute("rowset state on_release error, $0", _rowset_state));
        }
        return Status::OK();
    }

    RowsetState rowset_state() { return _rowset_state; }

private:
    RowsetState _rowset_state{ROWSET_UNLOADED};
};

class Rowset : public std::enable_shared_from_this<Rowset> {
public:
    Rowset(const TabletSchemaCSPtr&, std::string rowset_path, RowsetMetaSharedPtr rowset_meta);
    Rowset(const Rowset&) = delete;
    const Rowset& operator=(const Rowset&) = delete;

    virtual ~Rowset();

    static std::shared_ptr<Rowset> create(const TabletSchemaCSPtr& schema, std::string rowset_path,
                                          RowsetMetaSharedPtr rowset_meta) {
        return std::make_shared<Rowset>(schema, std::move(rowset_path), std::move(rowset_meta));
    }

    // Open all segment files in this rowset and load necessary metadata.
    //
    // May be called multiple times, subsequent calls will no-op.
    // Derived class implements the load logic by overriding the `do_load_once()` method.
    Status load();

    // reload this rowset after the underlying segment file is changed
    Status reload();
    Status reload_segment(int32_t segment_id);
    Status reload_segment_with_schema(int32_t segment_id, TabletSchemaCSPtr& schema);
    int64_t total_segment_data_size();

    const TabletSchema& schema_ref() const { return *_schema; }
    const TabletSchemaCSPtr& schema() const { return _schema; }
    void set_schema(const TabletSchemaCSPtr& schema) { _schema = schema; }

    StatusOr<ChunkIteratorPtr> new_iterator(const Schema& schema, const RowsetReadOptions& options);

    // For each segment in this rowset, create a `ChunkIterator` for it and *APPEND* it into
    // |segment_iterators|. If segments in this rowset has no overlapping, a single `UnionIterator`,
    // instead of multiple `ChunkIterator`s, will be created and appended into |segment_iterators|.
    Status get_segment_iterators(const Schema& schema, const RowsetReadOptions& options,
                                 std::vector<ChunkIteratorPtr>* seg_iterators);

    // estimate the number of compaction segment iterator
    StatusOr<int64_t> estimate_compaction_segment_iterator_num();

    const RowsetMetaSharedPtr& rowset_meta() const { return _rowset_meta; }

    std::vector<SegmentSharedPtr>& segments() { return _segments; }

    // only used for updatable tablets' rowset
    // simply get iterators to iterate all rows without complex options like predicates
    // |schema| read schema
    // |meta| olap meta, used for get delvec, if null do not fetch&use delvec
    // |version| read version, use for get delvec
    // |stats| used for iterator read stats
    // return iterator list, an iterator for each segment,
    // if the segment is empty, put an empty pointer in list
    // caller is also responsible to call rowset's acquire/release
    StatusOr<std::vector<ChunkIteratorPtr>> get_segment_iterators2(const Schema& schema,
                                                                   const TabletSchemaCSPtr& tablet_schema,
                                                                   KVStore* meta, int64_t version,
                                                                   OlapReaderStatistics* stats,
                                                                   KVStore* dcg_meta = nullptr);

    // only used for updatable tablets' rowset in column mode partial update
    // simply get iterators to iterate all rows without complex options like predicates
    // |schema| read schema
    // |stats| used for iterator read stats
    // return iterator list, an iterator for each segment,
    // if the segment is empty, put an empty pointer in list
    // caller is also responsible to call rowset's acquire/release
    StatusOr<std::vector<ChunkIteratorPtr>> get_update_file_iterators(const Schema& schema,
                                                                      OlapReaderStatistics* stats);

    // only used for updatable tablets' rowset in column mode partial update
    // get iterator by update file's id, and it iterate all rows without complex options like predicates
    // |schema| read schema
    // |update_file_id| the index of update file which we want to get iterator from
    // |stats| used for iterator read stats
    // if the segment is empty, return empty iterator
    StatusOr<ChunkIteratorPtr> get_update_file_iterator(const Schema& schema, uint32_t update_file_id,
                                                        OlapReaderStatistics* stats);

    // publish rowset to make it visible to read
    void make_visible(Version version);

    // like make_visible but updatable tablet has different mechanism
    // NOTE: only used for updatable tablet's rowset
    void make_commit(int64_t version, uint32_t rowset_seg_id);

    // helper class to access RowsetMeta
    int64_t start_version() const { return rowset_meta()->version().first; }
    int64_t end_version() const { return rowset_meta()->version().second; }
    size_t data_disk_size() const { return rowset_meta()->total_disk_size(); }
    bool empty() const { return rowset_meta()->empty(); }
    size_t num_rows() const { return rowset_meta()->num_rows(); }
    size_t total_row_size() const { return rowset_meta()->total_row_size(); }
    size_t total_update_row_size() const { return rowset_meta()->total_update_row_size(); }
    Version version() const { return rowset_meta()->version(); }
    RowsetId rowset_id() const { return rowset_meta()->rowset_id(); }
    std::string rowset_id_str() const { return rowset_meta()->rowset_id().to_string(); }
    int64_t creation_time() const { return rowset_meta()->creation_time(); }
    PUniqueId load_id() const { return rowset_meta()->load_id(); }
    int64_t txn_id() const { return rowset_meta()->txn_id(); }
    int64_t partition_id() const { return rowset_meta()->partition_id(); }
    int64_t num_segments() const { return rowset_meta()->num_segments(); }
    uint32_t num_delete_files() const { return rowset_meta()->get_num_delete_files(); }
    uint32_t num_update_files() const { return rowset_meta()->get_num_update_files(); }
    bool has_data_files() const { return num_segments() > 0 || num_delete_files() > 0 || num_update_files() > 0; }
    KeysType keys_type() const { return _keys_type; }

    const TabletSchemaCSPtr tablet_schema() { return rowset_meta()->tablet_schema(); }

    // remove all files in this rowset
    // TODO should we rename the method to remove_files() to be more specific?
    Status remove();

    Status remove_delta_column_group(KVStore* kvstore);

    Status remove_delta_column_group();

    // close to clear the resource owned by rowset
    // including: open files, indexes and so on
    // NOTICE: can not call this function in multithreads
    void close() {
        RowsetState old_state = _rowset_state_machine.rowset_state();
        if (old_state != ROWSET_LOADED) {
            return;
        }
        Status st;
        {
            std::lock_guard<std::mutex> close_lock(_lock);
            uint64_t current_refs = _refs_by_reader;
            old_state = _rowset_state_machine.rowset_state();
            if (old_state != ROWSET_LOADED) {
                return;
            }
            if (current_refs == 0) {
                do_close();
            }
            st = _rowset_state_machine.on_close(current_refs);
        }
        if (!st.ok()) {
            LOG(WARNING) << "state transition failed from:" << st.to_string();
            return;
        }
        VLOG(3) << "rowset is close. rowset state from:" << old_state << " to " << _rowset_state_machine.rowset_state()
                << ", version:" << start_version() << "-" << end_version()
                << ", tabletid:" << _rowset_meta->tablet_id();
    }

    // hard link all files in this rowset to `dir` to form a new rowset with id `new_rowset_id`.
    // `version` is used for link col files, default using INT64_MAX means link all col files
    Status link_files_to(KVStore* kvstore, const std::string& dir, RowsetId new_rowset_id, int64_t version = INT64_MAX);

    // copy all files to `dir`
    Status copy_files_to(KVStore* kvstore, const std::string& dir);

    static std::string segment_file_path(const std::string& segment_dir, const RowsetId& rowset_id, int segment_id);
    static std::string segment_temp_file_path(const std::string& dir, const RowsetId& rowset_id, int segment_id);
    static std::string segment_del_file_path(const std::string& segment_dir, const RowsetId& rowset_id, int segment_id);
    static std::string segment_upt_file_path(const std::string& segment_dir, const RowsetId& rowset_id, int segment_id);
    static std::string delta_column_group_path(const std::string& dir, const RowsetId& rowset_id, int segment_id,
                                               int64_t version, int idx);
    // return an unique identifier string for this rowset
    std::string unique_id() const { return _rowset_path + "/" + rowset_id().to_string(); }

    std::string rowset_path() const { return _rowset_path; }

    bool need_delete_file() const { return _need_delete_file; }

    void set_need_delete_file() { _need_delete_file = true; }

    bool contains_version(Version version) const { return rowset_meta()->version().contains(version); }

    void set_is_compacting(bool flag) { is_compacting.store(flag); }

    bool get_is_compacting() { return is_compacting.load(); }

    DeletePredicatePB* mutable_delete_predicate() { return _rowset_meta->mutable_delete_predicate(); }

    static bool comparator(const RowsetSharedPtr& left, const RowsetSharedPtr& right) {
        return left->end_version() < right->end_version();
    }

    // this function is called by reader to increase reference of rowset
    void acquire() { ++_refs_by_reader; }

    void release() {
        // if the refs by reader is 0 and the rowset is closed, should release the resouce
        uint64_t current_refs = --_refs_by_reader;
        if (current_refs == 0) {
            {
                std::lock_guard<std::mutex> release_lock(_lock);
                // rejudge _refs_by_reader because we do not add lock in create reader
                if (_refs_by_reader == 0 && _rowset_state_machine.rowset_state() == ROWSET_UNLOADING) {
                    // first do close, then change state
                    do_close();
                    WARN_IF_ERROR(_rowset_state_machine.on_release(),
                                  strings::Substitute("rowset state on_release error, $0",
                                                      _rowset_state_machine.rowset_state()));
                }
            }
            if (_rowset_state_machine.rowset_state() == ROWSET_UNLOADED) {
                VLOG(3) << "close the rowset. rowset state from ROWSET_UNLOADING to ROWSET_UNLOADED"
                        << ", version:" << start_version() << "-" << end_version()
                        << ", tabletid:" << _rowset_meta->tablet_id();
            }
        }
    }

    uint64_t refs_by_reader() { return _refs_by_reader; }

    static StatusOr<size_t> get_segment_num(const std::vector<RowsetSharedPtr>& rowsets) {
        size_t num_segments = 0;
        for (const auto& rowset : rowsets) {
            auto iterator_num_res = rowset->estimate_compaction_segment_iterator_num();
            if (!iterator_num_res.ok()) {
                return iterator_num_res.status();
            }
            num_segments += iterator_num_res.value();
        }
        return num_segments;
    }

    static void acquire_readers(const std::vector<RowsetSharedPtr>& rowsets) {
        std::for_each(rowsets.begin(), rowsets.end(), [](const RowsetSharedPtr& rowset) { rowset->acquire(); });
    }

    static void release_readers(const std::vector<RowsetSharedPtr>& rowsets) {
        std::for_each(rowsets.begin(), rowsets.end(), [](const RowsetSharedPtr& rowset) { rowset->release(); });
    }

    static void close_rowsets(const std::vector<RowsetSharedPtr>& rowsets) {
        std::for_each(rowsets.begin(), rowsets.end(), [](const RowsetSharedPtr& rowset) { rowset->close(); });
    }

    bool is_column_mode_partial_update() const { return _rowset_meta->is_column_mode_partial_update(); }

    // only used in unit test
    Status get_segment_sk_index(std::vector<std::string>* sk_index_values);

    Status verify();

protected:
    friend class RowsetFactory;

    // this is non-public because all clients should use RowsetFactory to obtain pointer to initialized Rowset
    Status init();

    // The actual implementation of load(). Guaranteed by to called exactly once.
    Status do_load();

    // release resources in this api
    void do_close();

    // allow subclass to add custom logic when rowset is being published
    virtual void make_visible_extra(Version version) {}

    TabletSchemaCSPtr _schema;
    std::string _rowset_path;
    RowsetMetaSharedPtr _rowset_meta;

    // mutex lock for load/close api because it is costly
    std::mutex _lock;
    bool _need_delete_file = false;
    // variable to indicate how many rowset readers owned this rowset
    std::atomic<uint64_t> _refs_by_reader;
    RowsetStateMachine _rowset_state_machine;

private:
    int64_t _mem_usage() const { return sizeof(Rowset) + _rowset_path.length(); }

    Status _remove_delta_column_group_files(const std::shared_ptr<FileSystem>& fs, KVStore* kvstore);

    Status _link_delta_column_group_files(KVStore* kvstore, const std::string& dir, int64_t version);

    Status _copy_delta_column_group_files(KVStore* kvstore, const std::string& dir, int64_t version);

    std::vector<SegmentSharedPtr> _segments;

    std::atomic<bool> is_compacting{false};

    KeysType _keys_type;
};

class RowsetReleaseGuard {
public:
    explicit RowsetReleaseGuard(std::shared_ptr<Rowset> rowset) : _rowset(std::move(rowset)) { _rowset->acquire(); }
    ~RowsetReleaseGuard() { _rowset->release(); }

private:
    std::shared_ptr<Rowset> _rowset;
};
using TabletSchemaSPtr = std::shared_ptr<TabletSchema>;

} // namespace starrocks
