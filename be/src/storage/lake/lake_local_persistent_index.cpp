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

#include "storage/lake/lake_local_persistent_index.h"

#include "gen_cpp/persistent_index.pb.h"
#include "storage/chunk_helper.h"
#include "storage/lake/lake_primary_index.h"
#include "storage/lake/meta_file.h"
#include "storage/primary_key_encoder.h"
#include "storage/tablet_meta_manager.h"

namespace starrocks::lake {

// TODO refactor load from lake tablet, use same path with load from local tablet.
Status LakeLocalPersistentIndex::load_from_lake_tablet(starrocks::lake::Tablet* tablet, const TabletMetadata& metadata,
                                                       int64_t base_version, const MetaFileBuilder* builder) {
    if (!is_primary_key(metadata)) {
        LOG(WARNING) << "tablet: " << tablet->id() << " is not primary key tablet";
        return Status::NotSupported("Only PrimaryKey table is supported to use persistent index");
    }
    // persistent index' minor compaction is a new strategy to decrease the IO amplification.
    // More detail: https://github.com/StarRocks/starrocks/issues/27581.
    // disable minor_compaction in cloud native table for now, will enable it later
    config::enable_pindex_minor_compaction = false;

    MonotonicStopWatch timer;
    timer.start();

    PersistentIndexMetaPB index_meta;

    // persistent_index_dir has been checked
    Status status = TabletMetaManager::get_persistent_index_meta(
            StorageEngine::instance()->get_persistent_index_store(tablet->id()), tablet->id(), &index_meta);
    if (!status.ok() && !status.is_not_found()) {
        LOG(ERROR) << "get tablet persistent index meta failed, tablet: " << tablet->id()
                   << "version: " << base_version;
        return Status::InternalError("get tablet persistent index meta failed");
    }

    // There are three conditions
    // First is we do not find PersistentIndexMetaPB in TabletMeta, it maybe the first time to
    // enable persistent index
    // Second is we find PersistentIndexMetaPB in TabletMeta, but it's version is behind applied_version
    // in TabletMeta. It could be happened as below:
    //    1. Enable persistent index and apply rowset, applied_version is 1-0
    //    2. Restart be and disable persistent index, applied_version is update to 2-0
    //    3. Restart be and enable persistent index
    // In this case, we don't have all rowset data in persistent index files, so we also need to rebuild it
    // The last is we find PersistentIndexMetaPB and it's version is equal to latest applied version. In this case,
    // we can load from index file directly

    if (status.ok()) {
        // all applied rowsets has save in existing persistent index meta
        // so we can load persistent index according to PersistentIndexMetaPB
        EditVersion version = index_meta.version();

        // Compaction of Lake tablet also generate a new version
        // here just use version.major so that PersistentIndexMetaPB need't to be modified,
        // the minor is meaningless
        if (version.major() == base_version) {
            // We need to rebuild persistent index because the meta structure is changed
            if (index_meta.format_version() != PERSISTENT_INDEX_VERSION_2 &&
                index_meta.format_version() != PERSISTENT_INDEX_VERSION_3 &&
                index_meta.format_version() != PERSISTENT_INDEX_VERSION_4) {
                LOG(WARNING) << "different format version, we need to rebuild persistent index, version: "
                             << index_meta.format_version();
                status = Status::InternalError("different format version");
            } else {
                status = load(index_meta);
            }
            if (status.ok()) {
                LOG(INFO) << "load persistent index tablet:" << tablet->id() << " version:" << version.to_string()
                          << " size: " << _size << " l0_size: " << (_l0 ? _l0->size() : 0)
                          << " l0_capacity:" << (_l0 ? _l0->capacity() : 0)
                          << " #shard: " << (_has_l1 ? _l1_vec[0]->_shards.size() : 0)
                          << " l1_size:" << (_has_l1 ? _l1_vec[0]->_size : 0) << " l2_size:" << _l2_file_size()
                          << " memory: " << memory_usage() << " status: " << status.to_string()
                          << " time:" << timer.elapsed_time() / 1000000 << "ms";
                return status;
            } else {
                LOG(WARNING) << "load persistent index failed, tablet: " << tablet->id() << ", status: " << status;
                if (index_meta.has_l0_meta()) {
                    EditVersion l0_version = index_meta.l0_meta().snapshot().version();
                    std::string l0_file_name =
                            strings::Substitute("index.l0.$0.$1", l0_version.major(), l0_version.minor());
                    Status st = FileSystem::Default()->delete_file(l0_file_name);
                    LOG(WARNING) << "delete error l0 index file: " << l0_file_name << ", status: " << st;
                }
                if (index_meta.has_l1_version()) {
                    EditVersion l1_version = index_meta.l1_version();
                    std::string l1_file_name =
                            strings::Substitute("index.l1.$0.$1", l1_version.major(), l1_version.minor());
                    Status st = FileSystem::Default()->delete_file(l1_file_name);
                    LOG(WARNING) << "delete error l1 index file: " << l1_file_name << ", status: " << st;
                }
                if (index_meta.l2_versions_size() > 0) {
                    for (int i = 0; i < index_meta.l2_versions_size(); i++) {
                        EditVersion l2_version = index_meta.l2_versions(i);
                        std::string l2_file_name =
                                strings::Substitute("index.l2.$0.$1$2", l2_version.major(), l2_version.minor(),
                                                    index_meta.l2_version_merged(i) ? MergeSuffix : "");
                        Status st = FileSystem::Default()->delete_file(l2_file_name);
                        LOG(WARNING) << "delete error l2 index file: " << l2_file_name << ", status: " << st;
                    }
                }
            }
        }
    }

    // 1. create and set key column schema
    std::unique_ptr<TabletSchema> tablet_schema = std::make_unique<TabletSchema>(metadata.schema());
    vector<ColumnId> pk_columns(tablet_schema->num_key_columns());
    for (auto i = 0; i < tablet_schema->num_key_columns(); i++) {
        pk_columns[i] = (ColumnId)i;
    }
    auto pkey_schema = ChunkHelper::convert_schema(*tablet_schema, pk_columns);

    size_t fix_size = PrimaryKeyEncoder::get_encoded_fixed_size(pkey_schema);

    // Init PersistentIndex
    _key_size = fix_size;
    _size = 0;
    _version = EditVersion(base_version, 0);
    auto st = ShardByLengthMutableIndex::create(_key_size, _path);
    if (!st.ok()) {
        LOG(WARNING) << "Build persistent index failed because initialization failed: " << st.status().to_string();
        return st.status();
    }
    _l0 = std::move(st).value();
    ASSIGN_OR_RETURN(_fs, FileSystem::CreateSharedFromString(_path));
    // set _dump_snapshot to true
    // In this case, only do flush or dump snapshot, set _dump_snapshot to avoid append wal
    _dump_snapshot = true;

    // clear l1
    {
        std::unique_lock wrlock(_lock);
        _l1_vec.clear();
        _usage_and_size_by_key_length.clear();
        _l1_merged_num.clear();
    }
    _has_l1 = false;
    for (const auto& [key_size, shard_info] : _l0->_shard_info_by_key_size) {
        auto [l0_shard_offset, l0_shard_size] = shard_info;
        const auto l0_kv_pairs_size = std::accumulate(std::next(_l0->_shards.begin(), l0_shard_offset),
                                                      std::next(_l0->_shards.begin(), l0_shard_offset + l0_shard_size),
                                                      0LL, [](size_t s, const auto& e) { return s + e->size(); });
        const auto l0_kv_pairs_usage = std::accumulate(std::next(_l0->_shards.begin(), l0_shard_offset),
                                                       std::next(_l0->_shards.begin(), l0_shard_offset + l0_shard_size),
                                                       0LL, [](size_t s, const auto& e) { return s + e->usage(); });
        if (auto [_, inserted] =
                    _usage_and_size_by_key_length.insert({key_size, {l0_kv_pairs_usage, l0_kv_pairs_size}});
            !inserted) {
            std::string msg = strings::Substitute(
                    "load persistent index from tablet:$0 failed, insert usage and size by key size failed, key_size: "
                    "$1",
                    tablet->id(), key_size);
            LOG(WARNING) << msg;
            return Status::InternalError(msg);
        }
    }

    // Init PersistentIndexMetaPB
    //   1. reset |version| |key_size|
    //   2. delete WALs because maybe PersistentIndexMetaPB has expired wals
    //   3. reset SnapshotMeta
    //   4. write all data into new tmp _l0 index file (tmp file will be delete in _build_commit())
    index_meta.clear_l0_meta();
    index_meta.clear_l1_version();
    index_meta.clear_l2_versions();
    index_meta.clear_l2_version_merged();
    index_meta.set_key_size(_key_size);
    index_meta.set_format_version(PERSISTENT_INDEX_VERSION_4);
    _version.to_pb(index_meta.mutable_version());
    MutableIndexMetaPB* l0_meta = index_meta.mutable_l0_meta();
    l0_meta->clear_wals();
    IndexSnapshotMetaPB* snapshot = l0_meta->mutable_snapshot();
    _version.to_pb(snapshot->mutable_version());
    PagePointerPB* data = snapshot->mutable_data();
    data->set_offset(0);
    data->set_size(0);

    OlapReaderStatistics stats;
    std::vector<uint32_t> rowset_ids;
    std::unique_ptr<Column> pk_column;
    if (pk_columns.size() > 1) {
        // more than one key column
        if (!PrimaryKeyEncoder::create_column(pkey_schema, &pk_column).ok()) {
            CHECK(false) << "create column for primary key encoder failed";
        }
    }
    vector<uint32_t> rowids;
    rowids.reserve(4096);
    auto chunk_shared_ptr = ChunkHelper::new_chunk(pkey_schema, 4096);
    auto chunk = chunk_shared_ptr.get();
    // 2. scan all rowsets and segments to build primary index
    auto rowsets = tablet->get_rowsets(metadata);
    if (!rowsets.ok()) {
        return rowsets.status();
    }

    size_t total_data_size = 0;
    size_t total_segments = 0;

    // NOTICE: primary index will be builded by segment files in metadata, and delvecs.
    // The delvecs we need are stored in delvec file by base_version and current MetaFileBuilder's cache.
    for (auto& rowset : *rowsets) {
        total_data_size += rowset->data_size();
        total_segments += rowset->num_segments();

        auto res = rowset->get_each_segment_iterator_with_delvec(pkey_schema, base_version, builder, &stats);
        if (!res.ok()) {
            return res.status();
        }
        auto& itrs = res.value();
        CHECK(itrs.size() == rowset->num_segments()) << "itrs.size != num_segments";
        for (size_t i = 0; i < itrs.size(); i++) {
            auto itr = itrs[i].get();
            if (itr == nullptr) {
                continue;
            }
            while (true) {
                chunk->reset();
                rowids.clear();
                auto st = itr->get_next(chunk, &rowids);
                if (st.is_end_of_file()) {
                    break;
                } else if (!st.ok()) {
                    return st;
                } else {
                    Column* pkc = nullptr;
                    if (pk_column) {
                        pk_column->reset_column();
                        PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, chunk->num_rows(), pk_column.get());
                        pkc = pk_column.get();
                    } else {
                        pkc = chunk->columns()[0].get();
                    }
                    uint32_t rssid = rowset->id() + i;
                    uint64_t base = ((uint64_t)rssid) << 32;
                    std::vector<IndexValue> values;
                    values.reserve(pkc->size());
                    DCHECK(pkc->size() <= rowids.size());
                    for (uint32_t i = 0; i < pkc->size(); i++) {
                        values.emplace_back(base + rowids[i]);
                    }
                    Status st;
                    if (pkc->is_binary()) {
                        st = insert(pkc->size(), reinterpret_cast<const Slice*>(pkc->raw_data()), values.data(), false);
                    } else {
                        std::vector<Slice> keys;
                        keys.reserve(pkc->size());
                        const auto* fkeys = pkc->continuous_data();
                        for (size_t i = 0; i < pkc->size(); ++i) {
                            keys.emplace_back(fkeys, _key_size);
                            fkeys += _key_size;
                        }
                        st = insert(pkc->size(), reinterpret_cast<const Slice*>(keys.data()), values.data(), false);
                    }
                    if (!st.ok()) {
                        LOG(ERROR) << "load index failed\n";
                        return st;
                    }
                }
            }
            itr->close();
        }
    }

    // commit: flush _l0 and build _l1
    // write PersistentIndexMetaPB in RocksDB
    status = commit(&index_meta);
    if (!status.ok()) {
        LOG(WARNING) << "build persistent index failed because commit failed: " << status.to_string();
        return status;
    }
    // write pesistent index meta
    status = TabletMetaManager::write_persistent_index_meta(
            StorageEngine::instance()->get_persistent_index_store(tablet->id()), tablet->id(), index_meta);
    if (!status.ok()) {
        LOG(WARNING) << "build persistent index failed because write persistent index meta failed: "
                     << status.to_string();
        return status;
    }

    RETURN_IF_ERROR(_delete_expired_index_file(
            _version, _l1_version,
            _l2_versions.size() > 0 ? _l2_versions[0] : EditVersionWithMerge(INT64_MAX, INT64_MAX, true)));
    _dump_snapshot = false;
    _flushed = false;

    LOG(INFO) << "build persistent index finish tablet: " << tablet->id() << " version:" << base_version
              << " #rowset:" << rowsets->size() << " #segment:" << total_segments << " data_size:" << total_data_size
              << " size: " << _size << " l0_size: " << _l0->size() << " l0_capacity:" << _l0->capacity()
              << " #shard: " << (_has_l1 ? _l1_vec[0]->_shards.size() : 0)
              << " l1_size:" << (_has_l1 ? _l1_vec[0]->_size : 0) << " l2_size:" << _l2_file_size()
              << " memory: " << memory_usage() << " time: " << timer.elapsed_time() / 1000000 << "ms";
    return Status::OK();
}

} // namespace starrocks::lake