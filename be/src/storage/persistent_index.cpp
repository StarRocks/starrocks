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

#include "storage/persistent_index.h"

#include <cstring>
#include <numeric>
#include <utility>

#include "fs/fs.h"
#include "gutil/strings/escaping.h"
#include "gutil/strings/substitute.h"
#include "io/io_profiler.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/persistent_index_tablet_loader.h"
#include "storage/primary_key_dump.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/rowset.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_meta_manager.h"
#include "storage/tablet_updates.h"
#include "storage/update_manager.h"
#include "util/bit_util.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/debug_util.h"
#include "util/defer_op.h"
#include "util/faststring.h"
#include "util/filesystem_util.h"
#include "util/raw_container.h"
#include "util/stopwatch.hpp"
#include "util/xxh3.h"

namespace starrocks {
static bool load_bf_or_not() {
    return config::enable_pindex_filter && StorageEngine::instance()->update_manager()->keep_pindex_bf();
}

static std::string get_l0_index_file_name(std::string& dir, const EditVersion& version) {
    return strings::Substitute("$0/index.l0.$1.$2", dir, version.major_number(), version.minor_number());
}

PersistentIndex::PersistentIndex(std::string path) : _path(std::move(path)) {}

PersistentIndex::~PersistentIndex() {
    if (!_l1_vec.empty()) {
        for (const auto& l1 : _l1_vec) {
            l1->clear();
        }
    }
    if (!_l2_vec.empty()) {
        for (const auto& l2 : _l2_vec) {
            l2->clear();
        }
    }
}

// Create a new empty PersistentIndex
Status PersistentIndex::create(size_t key_size, const EditVersion& version) {
    if (loaded()) {
        return Status::InternalError("PersistentIndex already loaded");
    }
    _key_size = key_size;
    _size = 0;
    _version = version;
    auto st = ShardByLengthMutableIndex::create(_key_size, _path);
    if (!st.ok()) {
        return st.status();
    }
    _l0 = std::move(st).value();
    ASSIGN_OR_RETURN(_fs, FileSystem::CreateSharedFromString(_path));
    return Status::OK();
}

Status PersistentIndex::load(const PersistentIndexMetaPB& index_meta) {
    _key_size = index_meta.key_size();
    _size = 0;
    _version = index_meta.version();
    auto st = ShardByLengthMutableIndex::create(_key_size, _path);
    if (!st.ok()) {
        return st.status();
    }
    _l0 = std::move(st).value();
    ASSIGN_OR_RETURN(_fs, FileSystem::CreateSharedFromString(_path));
    RETURN_IF_ERROR(_load(index_meta));
    // delete expired _l0 file and _l1 file
    const MutableIndexMetaPB& l0_meta = index_meta.l0_meta();
    const IndexSnapshotMetaPB& snapshot_meta = l0_meta.snapshot();
    EditVersion l0_version = snapshot_meta.version();
    RETURN_IF_ERROR(_delete_expired_index_file(
            l0_version, _l1_version,
            _l2_versions.size() > 0 ? _l2_versions[0] : EditVersionWithMerge(INT64_MAX, INT64_MAX, true)));
    _calc_memory_usage();
    return Status::OK();
}

Status PersistentIndex::_reload_usage_and_size_by_key_length(size_t l1_idx_start, size_t l1_idx_end, bool contain_l2) {
    _usage_and_size_by_key_length.clear();
    for (const auto& [key_size, shard_info] : _l0->_shard_info_by_key_size) {
        size_t total_size = 0;
        size_t total_usage = 0;
        auto [l0_shard_offset, l0_shard_size] = shard_info;
        const auto l0_kv_pairs_size = std::accumulate(std::next(_l0->_shards.begin(), l0_shard_offset),
                                                      std::next(_l0->_shards.begin(), l0_shard_offset + l0_shard_size),
                                                      0UL, [](size_t s, const auto& e) { return s + e->size(); });
        const auto l0_kv_pairs_usage = std::accumulate(std::next(_l0->_shards.begin(), l0_shard_offset),
                                                       std::next(_l0->_shards.begin(), l0_shard_offset + l0_shard_size),
                                                       0UL, [](size_t s, const auto& e) { return s + e->usage(); });
        total_size += l0_kv_pairs_size;
        total_usage += l0_kv_pairs_usage;
        for (int i = l1_idx_start; i < l1_idx_end; i++) {
            _get_stat_from_immutable_index(_l1_vec[i].get(), key_size, total_size, total_usage);
        }
        if (contain_l2) {
            // update size and usage by l2
            for (int i = 0; i < _l2_vec.size(); i++) {
                _get_stat_from_immutable_index(_l2_vec[i].get(), key_size, total_size, total_usage);
            }
        }
        if (auto [it, inserted] = _usage_and_size_by_key_length.insert({key_size, {total_usage, total_size}});
            !inserted) {
            LOG(WARNING) << "insert usage and size by key size failed, key_size: " << key_size;
            return Status::InternalError("insert usage and size by key size falied");
        }
    }
    return Status::OK();
}

Status PersistentIndex::_load(const PersistentIndexMetaPB& index_meta, bool reload) {
    size_t key_size = index_meta.key_size();
    _size = index_meta.size();
    if (_size != 0 && index_meta.usage() == 0) {
        if (key_size != 0) {
            _usage = (key_size + kIndexValueSize) * _size;
        } else {
            // if persistent index is varlen and upgrade from old version, we can't estimate accurate usage of index
            // so we use index file size as the _usage and the _usage will be adjusted in subsequent compaction.
            if (index_meta.has_l1_version()) {
                EditVersion version = index_meta.l1_version();
                auto l1_file_path =
                        strings::Substitute("$0/index.l1.$1.$2", _path, version.major_number(), version.minor_number());
                auto l1_st = _fs->get_file_size(l1_file_path);
                if (!l1_st.ok()) {
                    return l1_st.status();
                }
                _usage = l1_st.value();
            } else {
                DCHECK(index_meta.has_l0_meta());
                const MutableIndexMetaPB& l0_meta = index_meta.l0_meta();
                const IndexSnapshotMetaPB& snapshot_meta = l0_meta.snapshot();
                const EditVersion& start_version = snapshot_meta.version();
                std::string l0_file_path = get_l0_index_file_name(_path, start_version);
                auto l0_st = _fs->get_file_size(l0_file_path);
                if (!l0_st.ok()) {
                    return l0_st.status();
                }
                _usage = l0_st.value();
            }
        }
    } else {
        _usage = index_meta.usage();
    }
    DCHECK_EQ(key_size, _key_size);
    if (!index_meta.has_l0_meta()) {
        return Status::InternalError("invalid PersistentIndexMetaPB");
    }
    const MutableIndexMetaPB& l0_meta = index_meta.l0_meta();
    DCHECK(_l0 != nullptr);
    RETURN_IF_ERROR(_l0->load(l0_meta));

    _l1_vec.clear();
    _l1_merged_num.clear();
    _has_l1 = false;
    if (index_meta.has_l1_version()) {
        _l1_version = index_meta.l1_version();
        auto l1_block_path =
                strings::Substitute("$0/index.l1.$1.$2", _path, _l1_version.major_number(), _l1_version.minor_number());
        ASSIGN_OR_RETURN(auto l1_rfile, _fs->new_random_access_file(l1_block_path));
        // TODO
        // we can reduce load bf data diskio after flush or compaction
        auto l1_st = ImmutableIndex::load(std::move(l1_rfile), load_bf_or_not());
        if (!l1_st.ok()) {
            return l1_st.status();
        }
        _l1_vec.emplace_back(std::move(l1_st).value());
        _l1_merged_num.emplace_back(-1);
        _has_l1 = true;
    }

    _l2_versions.clear();
    _l2_vec.clear();
    if (index_meta.l2_versions_size() > 0) {
        DCHECK(index_meta.l2_versions_size() == index_meta.l2_version_merged_size());
        for (int i = 0; i < index_meta.l2_versions_size(); i++) {
            auto l2_block_path = strings::Substitute(
                    "$0/index.l2.$1.$2$3", _path, index_meta.l2_versions(i).major_number(),
                    index_meta.l2_versions(i).minor_number(), index_meta.l2_version_merged(i) ? MergeSuffix : "");
            ASSIGN_OR_RETURN(auto l2_rfile, _fs->new_random_access_file(l2_block_path));
            ASSIGN_OR_RETURN(auto l2_index, ImmutableIndex::load(std::move(l2_rfile), load_bf_or_not()));
            _l2_versions.emplace_back(EditVersionWithMerge(index_meta.l2_versions(i), index_meta.l2_version_merged(i)));
            _l2_vec.emplace_back(std::move(l2_index));
        }
    }
    // if reload, don't update _usage_and_size_by_key_length
    if (!reload) {
        // if has l1, idx range is [0, 1)
        RETURN_IF_ERROR(_reload_usage_and_size_by_key_length(_has_l1 ? 0 : 1, 1, false));
    }

    return Status::OK();
}

void PersistentIndex::_get_stat_from_immutable_index(ImmutableIndex* immu_index, uint32_t key_size, size_t& total_size,
                                                     size_t& total_usage) {
    auto iter = immu_index->_shard_info_by_length.find(key_size);
    if (iter != immu_index->_shard_info_by_length.end()) {
        auto [l1_shard_offset, l1_shard_size] = iter->second;
        const auto l1_kv_pairs_size =
                std::accumulate(std::next(immu_index->_shards.begin(), l1_shard_offset),
                                std::next(immu_index->_shards.begin(), l1_shard_offset + l1_shard_size), 0UL,
                                [](size_t s, const auto& e) { return s + e.size; });
        const auto l1_kv_pairs_usage =
                std::accumulate(std::next(immu_index->_shards.begin(), l1_shard_offset),
                                std::next(immu_index->_shards.begin(), l1_shard_offset + l1_shard_size), 0UL,
                                [](size_t s, const auto& e) { return s + e.data_size; });
        total_size += l1_kv_pairs_size;
        total_usage += l1_kv_pairs_usage;
    }
}

Status PersistentIndex::_build_commit(TabletLoader* loader, PersistentIndexMetaPB& index_meta) {
    // commit: flush _l0 and build _l1
    // write PersistentIndexMetaPB in RocksDB
    Status status = commit(&index_meta);
    if (!status.ok()) {
        LOG(WARNING) << "build persistent index failed because commit failed: " << status.to_string();
        return status;
    }
    // write pesistent index meta
    status = TabletMetaManager::write_persistent_index_meta(loader->data_dir(), loader->tablet_id(), index_meta);
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
    return status;
}

Status PersistentIndex::_insert_rowsets(TabletLoader* loader, const Schema& pkey_schema,
                                        std::unique_ptr<Column> pk_column) {
    std::vector<uint32_t> rowids;
    rowids.reserve(4096);
    auto chunk_shared_ptr = ChunkHelper::new_chunk(pkey_schema, 4096);
    auto chunk = chunk_shared_ptr.get();
    RETURN_IF_ERROR(loader->rowset_iterator(pkey_schema, [&](const std::vector<ChunkIteratorPtr>& itrs,
                                                             uint32_t rowset_id) {
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
                    if (pk_column != nullptr) {
                        pk_column->reset_column();
                        PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, chunk->num_rows(), pk_column.get());
                        pkc = pk_column.get();
                    } else {
                        pkc = chunk->columns()[0].get();
                    }
                    uint32_t rssid = rowset_id + i;
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
                        LOG(ERROR) << "load index failed: tablet=" << loader->tablet_id() << " rowset:" << rowset_id
                                   << " segment:" << i << " reason: " << st.to_string() << " current_size:" << size();
                        return st;
                    }
                }
            }
            itr->close();
        }
        return Status::OK();
    }));
    return Status::OK();
}

bool PersistentIndex::_need_rebuild_index(const PersistentIndexMetaPB& index_meta) {
    if (index_meta.l2_versions_size() > 0 && !config::enable_pindex_minor_compaction) {
        // When l2 exist, and we choose to disable minor compaction, then we need to rebuild index.
        return true;
    }
    if (index_meta.l2_versions_size() != index_meta.l2_version_merged_size()) {
        // Make sure l2 version equal to l2 version merged flag
        return true;
    }

    return false;
}

Status PersistentIndex::load_from_tablet(Tablet* tablet) {
    if (tablet->keys_type() != PRIMARY_KEYS) {
        LOG(WARNING) << "tablet: " << tablet->tablet_id() << " is not primary key tablet";
        return Status::NotSupported("Only PrimaryKey table is supported to use persistent index");
    }

    std::unique_ptr<TabletLoader> loader = std::make_unique<PersistentIndexTabletLoader>(tablet);
    return _load_by_loader(loader.get());
}

Status PersistentIndex::prepare(const EditVersion& version, size_t n) {
    _dump_snapshot = false;
    _flushed = false;
    _version = version;

    if (config::enable_parallel_get_and_bf) {
        _need_bloom_filter = true;
    }
    _set_error(false, "");
    return Status::OK();
}

Status PersistentIndex::abort() {
    _dump_snapshot = false;
    return Status::NotSupported("TODO");
}

uint64_t PersistentIndex::_l1_l2_file_size() const {
    uint64_t total_l1_l2_file_size = 0;
    if (_has_l1) {
        total_l1_l2_file_size += _l1_vec[0]->file_size();
    }
    for (int i = 0; i < _l2_vec.size(); i++) {
        total_l1_l2_file_size += _l2_vec[i]->file_size();
    }
    return total_l1_l2_file_size;
}

uint64_t PersistentIndex::_l2_file_size() const {
    uint64_t total_l2_file_size = 0;
    for (int i = 0; i < _l2_vec.size(); i++) {
        total_l2_file_size += _l2_vec[i]->file_size();
    }
    return total_l2_file_size;
}

bool PersistentIndex::_enable_minor_compaction() {
    if (config::enable_pindex_minor_compaction) {
        if (_l2_versions.size() < config::max_allow_pindex_l2_num) {
            return true;
        } else {
            LOG(WARNING) << "PersistentIndex stop do minor compaction, path: " << _path
                         << " , current l2 cnt: " << _l2_versions.size();
            (void)_reload_usage_and_size_by_key_length(0, _l1_vec.size(), false);
        }
    }
    return false;
}

// There are four cases as below in commit
//   1. _flush_l0
//   2. _merge_compaction or _minor_compaction
//   3. _dump_snapshot
//   4. append_wal
// both case1 and case2 will create a new l1 file and a new empty l0 file
// case3 will write a new snapshot l0
// case4 will append wals into l0 file
Status PersistentIndex::commit(PersistentIndexMetaPB* index_meta, IOStat* stat) {
    MonotonicStopWatch watch;
    watch.start();
    DCHECK_EQ(index_meta->key_size(), _key_size);
    // check if _l0 need be flush, there are two conditions:
    //   1. _l1 is not exist, _flush_l0 and build _l1
    //   2. _l1 is exist, merge _l0 and _l1
    // rebuild _l0 and _l1
    // In addition, there may be I/O waste because we append wals firstly and do _flush_l0 or _merge_compaction.
    uint64_t l1_l2_file_size = _l1_l2_file_size();
    bool do_minor_compaction = false;
    // if l1 is not empty,
    if (_flushed) {
        if (_enable_minor_compaction()) {
            RETURN_IF_ERROR(_minor_compaction(index_meta));
            do_minor_compaction = true;
        } else {
            RETURN_IF_ERROR(_merge_compaction());
        }
        if (stat != nullptr) {
            stat->compaction_cost += watch.elapsed_time();
            watch.reset();
        }
    } else {
        if (l1_l2_file_size != 0) {
            // and l0 memory usage is large enough,
            if (_l0_is_full(l1_l2_file_size)) {
                // do l0 l1 merge compaction
                _flushed = true;
                if (_enable_minor_compaction()) {
                    RETURN_IF_ERROR(_minor_compaction(index_meta));
                    do_minor_compaction = true;
                } else {
                    RETURN_IF_ERROR(_merge_compaction());
                }
                if (stat != nullptr) {
                    stat->compaction_cost += watch.elapsed_time();
                    watch.reset();
                }
            }
            // if l1 is empty, and l0 memory usage is large enough
        } else if (_l0_is_full()) {
            // do flush l0
            _flushed = true;
            RETURN_IF_ERROR(_flush_l0());
            if (stat != nullptr) {
                stat->flush_or_wal_cost += watch.elapsed_time();
                watch.reset();
            }
        }
    }
    // l0_max_file_size: the maximum data size for WAL
    // l0_max_mem_usage: the maximum data size for snapshot
    // So the max l0 file size should less than l0_max_file_size + l0_max_mem_usage
    _dump_snapshot |= !_flushed && _l0->file_size() > config::l0_max_mem_usage + config::l0_max_file_size;
    // for case1 and case2
    if (do_minor_compaction) {
        // clear _l0 and reload l1 and l2s
        RETURN_IF_ERROR(_reload(*index_meta));
    } else if (_flushed) {
        // update PersistentIndexMetaPB
        index_meta->set_size(_size);
        index_meta->set_usage(_usage);
        index_meta->set_format_version(PERSISTENT_INDEX_VERSION_4);
        _version.to_pb(index_meta->mutable_version());
        _version.to_pb(index_meta->mutable_l1_version());
        MutableIndexMetaPB* l0_meta = index_meta->mutable_l0_meta();
        RETURN_IF_ERROR(_l0->commit(l0_meta, _version, kFlush));
        // clear _l0 and reload _l1
        RETURN_IF_ERROR(_reload(*index_meta));
    } else if (_dump_snapshot) {
        index_meta->set_size(_size);
        index_meta->set_usage(_usage);
        index_meta->set_format_version(PERSISTENT_INDEX_VERSION_4);
        _version.to_pb(index_meta->mutable_version());
        MutableIndexMetaPB* l0_meta = index_meta->mutable_l0_meta();
        RETURN_IF_ERROR(_l0->commit(l0_meta, _version, kSnapshot));
    } else {
        index_meta->set_size(_size);
        index_meta->set_usage(_usage);
        index_meta->set_format_version(PERSISTENT_INDEX_VERSION_4);
        _version.to_pb(index_meta->mutable_version());
        MutableIndexMetaPB* l0_meta = index_meta->mutable_l0_meta();
        RETURN_IF_ERROR(_l0->commit(l0_meta, _version, kAppendWAL));
    }
    if (stat != nullptr) {
        stat->reload_meta_cost += watch.elapsed_time();
    }
    _calc_memory_usage();

    LOG(INFO) << strings::Substitute("commit persistent index successfully, version: [$0,$1]", _version.major_number(),
                                     _version.minor_number());

    return Status::OK();
}

Status PersistentIndex::on_commited() {
    if (_flushed || _dump_snapshot) {
        RETURN_IF_ERROR(_delete_expired_index_file(
                _version, _l1_version,
                _l2_versions.size() > 0 ? _l2_versions[0] : EditVersionWithMerge(INT64_MAX, INT64_MAX, true)));
    }
    RETURN_IF_ERROR(_delete_tmp_index_file());
    _dump_snapshot = false;
    _flushed = false;
    _need_bloom_filter = false;

    return Status::OK();
}

Status PersistentIndex::_get_from_immutable_index(size_t n, const Slice* keys, IndexValue* values,
                                                  std::map<size_t, KeysInfo>& keys_info_by_key_size, IOStat* stat) {
    if (_l1_vec.empty() && _l2_vec.empty()) {
        return Status::OK();
    }
    for (auto& [_, keys_info] : keys_info_by_key_size) {
        std::sort(keys_info.key_infos.begin(), keys_info.key_infos.end());
    }

    for (auto& [key_size, keys_info] : keys_info_by_key_size) {
        for (int i = _l1_vec.size(); i > 0; i--) {
            if (keys_info.size() == 0) {
                break;
            }
            KeysInfo found_keys_info;
            // get data from tmp_l1
            RETURN_IF_ERROR(_l1_vec[i - 1]->get(n, keys, keys_info, values, &found_keys_info, key_size, stat));
            if (found_keys_info.size() != 0) {
                std::sort(found_keys_info.key_infos.begin(), found_keys_info.key_infos.end());
                // modify keys_info
                keys_info.set_difference(found_keys_info);
            }
        }
        for (int i = _l2_vec.size(); i > 0; i--) {
            if (keys_info.size() == 0) {
                break;
            }
            KeysInfo found_keys_info;
            // get data from l2
            RETURN_IF_ERROR(_l2_vec[i - 1]->get(n, keys, keys_info, values, &found_keys_info, key_size, stat));
            if (found_keys_info.size() != 0) {
                std::sort(found_keys_info.key_infos.begin(), found_keys_info.key_infos.end());
                // modify keys_info
                keys_info.set_difference(found_keys_info);
            }
        }
    }
    return Status::OK();
}

class GetFromImmutableIndexTask : public Runnable {
public:
    GetFromImmutableIndexTask(size_t num, ImmutableIndex* immu_index, const Slice* keys, IndexValue* values,
                              std::map<size_t, KeysInfo>* keys_info_by_key_size, KeysInfo* found_keys_info,
                              PersistentIndex* index, IOStatEntry* io_stat_entry)
            : _num(num),
              _immu_index(immu_index),
              _keys(keys),
              _values(values),
              _keys_info_by_key_size(keys_info_by_key_size),
              _found_keys_info(found_keys_info),
              _index(index),
              _io_stat_entry(io_stat_entry) {}

    void run() override {
        auto scope = IOProfiler::scope(_io_stat_entry);
        WARN_IF_ERROR(_index->get_from_one_immutable_index(_immu_index, _num, _keys, _values, _keys_info_by_key_size,
                                                           _found_keys_info),
                      "Failed to run GetFromImmutableIndexTask");
    }

private:
    size_t _num;
    ImmutableIndex* _immu_index;
    const Slice* _keys;
    IndexValue* _values;
    std::map<size_t, KeysInfo>* _keys_info_by_key_size;
    KeysInfo* _found_keys_info;
    PersistentIndex* _index;
    IOStatEntry* _io_stat_entry;
};

Status PersistentIndex::get_from_one_immutable_index(ImmutableIndex* immu_index, size_t n, const Slice* keys,
                                                     IndexValue* values,
                                                     std::map<size_t, KeysInfo>* keys_info_by_key_size,
                                                     KeysInfo* found_keys_info) {
    Status st;
    for (auto& [key_size, keys_info] : (*keys_info_by_key_size)) {
        st = immu_index->get(n, keys, keys_info, values, found_keys_info, key_size, nullptr);
        if (!st.ok()) {
            std::string msg = strings::Substitute("get from one immutableindex failed, file: $0, status: $1",
                                                  immu_index->filename(), st.to_string());
            LOG(ERROR) << msg;
            _set_error(true, msg);
            break;
        }
    }
    std::unique_lock<std::mutex> ul(_get_lock);
    _running_get_task--;
    if (_running_get_task == 0) {
        _get_task_finished.notify_all();
    }
    return st;
}

Status PersistentIndex::_get_from_immutable_index_parallel(size_t n, const Slice* keys, IndexValue* values,
                                                           std::map<size_t, KeysInfo>& keys_info_by_key_size) {
    if (_l1_vec.empty() && _l2_vec.empty()) {
        return Status::OK();
    }

    std::unique_lock<std::mutex> ul(_get_lock);
    std::map<size_t, KeysInfo>::iterator iter;
    std::string error_msg;
    std::vector<std::vector<uint64_t>> get_values(_l2_vec.size() + _l1_vec.size(),
                                                  std::vector<uint64_t>(n, NullIndexValue));
    // store keys_info from old to new
    _found_keys_info.resize(_l2_vec.size() + _l1_vec.size());
    for (size_t i = 0; i < _l2_vec.size() + _l1_vec.size(); i++) {
        ImmutableIndex* immu_index = i < _l2_vec.size() ? _l2_vec[i].get() : _l1_vec[i - _l2_vec.size()].get();
        std::shared_ptr<Runnable> r(std::make_shared<GetFromImmutableIndexTask>(
                n, immu_index, keys, reinterpret_cast<IndexValue*>(get_values[i].data()), &keys_info_by_key_size,
                &_found_keys_info[i], this, IOProfiler::get_context()));
        auto st = StorageEngine::instance()->update_manager()->get_pindex_thread_pool()->submit(std::move(r));
        if (!st.ok()) {
            error_msg = strings::Substitute("get from immutable index failed: $0", st.to_string());
            LOG(ERROR) << error_msg;
            return st;
        }
        _running_get_task++;
    }
    while (_running_get_task != 0) {
        _get_task_finished.wait(ul);
    }
    if (is_error()) {
        LOG(ERROR) << _error_msg;
        return Status::InternalError(_error_msg);
    }

    // wait all task finished
    for (int i = 0; i < _found_keys_info.size(); i++) {
        for (int j = 0; j < _found_keys_info[i].size(); j++) {
            auto key_idx = _found_keys_info[i].key_infos[j].first;
            values[key_idx] = get_values[i][key_idx];
        }
    }
    _found_keys_info.clear();

    return Status::OK();
}

void PersistentIndex::_get_l2_stat(const std::vector<std::unique_ptr<ImmutableIndex>>& l2_vec,
                                   std::map<uint32_t, std::pair<int64_t, int64_t>>& usage_and_size_stat) {
    std::for_each(
            l2_vec.begin(), l2_vec.end(), [&usage_and_size_stat](const std::unique_ptr<ImmutableIndex>& immu_index) {
                for (const auto& [key_size, shard_info] : immu_index->_shard_info_by_length) {
                    auto [l2_shard_offset, l2_shard_size] = shard_info;
                    const auto size =
                            std::accumulate(std::next(immu_index->_shards.begin(), l2_shard_offset),
                                            std::next(immu_index->_shards.begin(), l2_shard_offset + l2_shard_size), 0L,
                                            [](size_t s, const auto& e) { return s + e.size; });
                    const auto usage =
                            std::accumulate(std::next(immu_index->_shards.begin(), l2_shard_offset),
                                            std::next(immu_index->_shards.begin(), l2_shard_offset + l2_shard_size), 0L,
                                            [](size_t s, const auto& e) { return s + e.data_size; });

                    auto iter = usage_and_size_stat.find(key_size);
                    if (iter == usage_and_size_stat.end()) {
                        usage_and_size_stat.insert({key_size, {usage, size}});
                    } else {
                        iter->second.first += usage;
                        iter->second.second += size;
                    }
                }
            });
}

Status PersistentIndex::get(size_t n, const Slice* keys, IndexValue* values) {
    std::map<size_t, KeysInfo> not_founds_by_key_size;
    size_t num_found = 0;
    RETURN_IF_ERROR(_l0->get(n, keys, values, &num_found, not_founds_by_key_size));
    if (config::enable_parallel_get_and_bf) {
        return _get_from_immutable_index_parallel(n, keys, values, not_founds_by_key_size);
    }
    return _get_from_immutable_index(n, keys, values, not_founds_by_key_size, nullptr);
}

Status PersistentIndex::_flush_advance_or_append_wal(size_t n, const Slice* keys, const IndexValue* values,
                                                     std::vector<size_t>* replace_idxes) {
    bool need_flush_advance = _need_flush_advance();
    _flushed |= need_flush_advance;

    if (need_flush_advance) {
        RETURN_IF_ERROR(flush_advance());
    }

    if (_need_merge_advance()) {
        RETURN_IF_ERROR(_merge_compaction_advance());
    } else if (!_flushed) {
        _dump_snapshot |= _can_dump_directly();
        if (!_dump_snapshot) {
            if (replace_idxes == nullptr) {
                RETURN_IF_ERROR(_l0->append_wal(n, keys, values));
            } else {
                RETURN_IF_ERROR(_l0->append_wal(keys, values, *replace_idxes));
            }
        }
    }
    _calc_memory_usage();

    return Status::OK();
}

Status PersistentIndex::_update_usage_and_size_by_key_length(
        std::vector<std::pair<int64_t, int64_t>>& add_usage_and_size) {
    if (_key_size > 0) {
        auto iter = _usage_and_size_by_key_length.find(_key_size);
        DCHECK(iter != _usage_and_size_by_key_length.end());
        if (iter == _usage_and_size_by_key_length.end()) {
            std::string msg = strings::Substitute("no key_size: $0 in usage info", _key_size);
            LOG(WARNING) << msg;
            return Status::InternalError(msg);
        } else {
            iter->second.first = std::max(0L, iter->second.first + add_usage_and_size[_key_size].first);
            iter->second.second = std::max(0L, iter->second.second + add_usage_and_size[_key_size].second);
        }
    } else {
        for (int key_size = 1; key_size <= kSliceMaxFixLength; key_size++) {
            if (add_usage_and_size[key_size].second > 0) {
                auto iter = _usage_and_size_by_key_length.find(key_size);
                if (iter == _usage_and_size_by_key_length.end()) {
                    std::string msg = strings::Substitute("no key_size: $0 in usage info", key_size);
                    LOG(WARNING) << msg;
                    return Status::InternalError(msg);
                } else {
                    iter->second.first = std::max(0L, iter->second.first + add_usage_and_size[key_size].first);
                    iter->second.second = std::max(0L, iter->second.second + add_usage_and_size[key_size].second);
                }
            }
        }

        int64_t slice_usage = 0;
        int64_t slice_size = 0;
        for (int key_size = kSliceMaxFixLength + 1; key_size <= kFixedMaxKeySize; key_size++) {
            slice_usage += add_usage_and_size[key_size].first;
            slice_size += add_usage_and_size[key_size].second;
        }
        DCHECK(_key_size == 0);
        auto iter = _usage_and_size_by_key_length.find(_key_size);
        if (iter == _usage_and_size_by_key_length.end()) {
            std::string msg = strings::Substitute("no key_size: $0 in usage info", _key_size);
            LOG(WARNING) << msg;
            return Status::InternalError(msg);
        }
        iter->second.first = std::max(0L, iter->second.first + slice_usage);
        iter->second.second = std::max(0L, iter->second.second + slice_size);
    }
    return Status::OK();
}

Status PersistentIndex::upsert(size_t n, const Slice* keys, const IndexValue* values, IndexValue* old_values,
                               IOStat* stat) {
    std::map<size_t, KeysInfo> not_founds_by_key_size;
    size_t num_found = 0;
    MonotonicStopWatch watch;
    watch.start();
    RETURN_IF_ERROR(_l0->upsert(n, keys, values, old_values, &num_found, not_founds_by_key_size));
    if (stat != nullptr) {
        stat->l0_write_cost += watch.elapsed_time();
        watch.reset();
    }
    if (config::enable_parallel_get_and_bf) {
        RETURN_IF_ERROR(_get_from_immutable_index_parallel(n, keys, old_values, not_founds_by_key_size));
    } else {
        RETURN_IF_ERROR(_get_from_immutable_index(n, keys, old_values, not_founds_by_key_size, stat));
    }
    if (stat != nullptr) {
        stat->l1_l2_read_cost += watch.elapsed_time();
        watch.reset();
    }
    std::vector<std::pair<int64_t, int64_t>> add_usage_and_size(kFixedMaxKeySize + 1,
                                                                std::pair<int64_t, int64_t>(0, 0));
    for (size_t i = 0; i < n; i++) {
        if (old_values[i].get_value() == NullIndexValue) {
            _size++;
            _usage += keys[i].size + kIndexValueSize;
            int64_t len = keys[i].size > kFixedMaxKeySize ? 0 : keys[i].size;
            add_usage_and_size[len].first += keys[i].size + kIndexValueSize;
            add_usage_and_size[len].second++;
        }
    }

    RETURN_IF_ERROR(_update_usage_and_size_by_key_length(add_usage_and_size));
    Status st = _flush_advance_or_append_wal(n, keys, values, nullptr);
    if (stat != nullptr) {
        stat->flush_or_wal_cost += watch.elapsed_time();
    }
    return st;
}

Status PersistentIndex::insert(size_t n, const Slice* keys, const IndexValue* values, bool check_l1) {
    std::set<size_t> check_l1_l2_key_sizes;
    RETURN_IF_ERROR(_l0->insert(n, keys, values, check_l1_l2_key_sizes));
    if (!_l1_vec.empty()) {
        int end_idx = _has_l1 ? 1 : 0;
        for (int i = _l1_vec.size() - 1; i >= end_idx; i--) {
            for (const auto check_l1_l2_key_size : check_l1_l2_key_sizes) {
                RETURN_IF_ERROR(_l1_vec[i]->check_not_exist(n, keys, check_l1_l2_key_size));
            }
        }
    }
    if (_has_l1 && check_l1) {
        for (const auto check_l1_l2_key_size : check_l1_l2_key_sizes) {
            RETURN_IF_ERROR(_l1_vec[0]->check_not_exist(n, keys, check_l1_l2_key_size));
        }
    }
    for (int i = _l2_vec.size() - 1; i >= 0; i--) {
        for (const auto check_l1_l2_key_size : check_l1_l2_key_sizes) {
            RETURN_IF_ERROR(_l2_vec[i]->check_not_exist(n, keys, check_l1_l2_key_size));
        }
    }
    std::vector<std::pair<int64_t, int64_t>> add_usage_and_size(kFixedMaxKeySize + 1,
                                                                std::pair<int64_t, int64_t>(0, 0));
    _size += n;
    for (size_t i = 0; i < n; i++) {
        _usage += keys[i].size + kIndexValueSize;
        int64_t len = keys[i].size > kFixedMaxKeySize ? 0 : keys[i].size;
        add_usage_and_size[len].first += keys[i].size + kIndexValueSize;
        add_usage_and_size[len].second++;
    }
    RETURN_IF_ERROR(_update_usage_and_size_by_key_length(add_usage_and_size));

    return _flush_advance_or_append_wal(n, keys, values, nullptr);
}

Status PersistentIndex::erase(size_t n, const Slice* keys, IndexValue* old_values) {
    std::map<size_t, KeysInfo> not_founds_by_key_size;
    size_t num_erased = 0;
    RETURN_IF_ERROR(_l0->erase(n, keys, old_values, &num_erased, not_founds_by_key_size));
    _dump_snapshot |= _can_dump_directly();
    if (config::enable_parallel_get_and_bf) {
        RETURN_IF_ERROR(_get_from_immutable_index_parallel(n, keys, old_values, not_founds_by_key_size));
    } else {
        RETURN_IF_ERROR(_get_from_immutable_index(n, keys, old_values, not_founds_by_key_size, nullptr));
    }
    std::vector<std::pair<int64_t, int64_t>> add_usage_and_size(kFixedMaxKeySize + 1,
                                                                std::pair<int64_t, int64_t>(0, 0));
    for (size_t i = 0; i < n; i++) {
        if (old_values[i].get_value() != NullIndexValue) {
            _size--;
            _usage -= keys[i].size + kIndexValueSize;
            int64_t len = keys[i].size > kFixedMaxKeySize ? 0 : keys[i].size;
            add_usage_and_size[len].first -= keys[i].size + kIndexValueSize;
            add_usage_and_size[len].second--;
        }
    }
    RETURN_IF_ERROR(_update_usage_and_size_by_key_length(add_usage_and_size));

    return _flush_advance_or_append_wal(n, keys, nullptr, nullptr);
}

[[maybe_unused]] Status PersistentIndex::try_replace(size_t n, const Slice* keys, const IndexValue* values,
                                                     const std::vector<uint32_t>& src_rssid,
                                                     std::vector<uint32_t>* failed) {
    std::vector<IndexValue> found_values;
    found_values.resize(n);
    RETURN_IF_ERROR(get(n, keys, found_values.data()));
    std::vector<size_t> replace_idxes;
    for (size_t i = 0; i < n; ++i) {
        if (found_values[i].get_value() != NullIndexValue &&
            ((uint32_t)(found_values[i].get_value() >> 32)) == src_rssid[i]) {
            replace_idxes.emplace_back(i);
        } else {
            failed->emplace_back(values[i].get_value() & 0xFFFFFFFF);
        }
    }
    RETURN_IF_ERROR(_l0->replace(keys, values, replace_idxes));
    return _flush_advance_or_append_wal(n, keys, values, &replace_idxes);
}

Status PersistentIndex::try_replace(size_t n, const Slice* keys, const IndexValue* values, const uint32_t max_src_rssid,
                                    std::vector<uint32_t>* failed) {
    std::vector<IndexValue> found_values;
    found_values.resize(n);
    RETURN_IF_ERROR(get(n, keys, found_values.data()));
    std::vector<size_t> replace_idxes;
    for (size_t i = 0; i < n; ++i) {
        auto found_value = found_values[i].get_value();
        if (found_value != NullIndexValue && ((uint32_t)(found_value >> 32)) <= max_src_rssid) {
            replace_idxes.emplace_back(i);
        } else {
            failed->emplace_back(values[i].get_value() & 0xFFFFFFFF);
        }
    }
    RETURN_IF_ERROR(_l0->replace(keys, values, replace_idxes));
    return _flush_advance_or_append_wal(n, keys, values, &replace_idxes);
}

Status PersistentIndex::flush_advance() {
    // flush l0 into _l1_vec
    int idx = _l1_vec.size();
    std::string l1_tmp_file = strings::Substitute("$0/index.l1.$1.$2.$3.tmp", _path, _version.major_number(),
                                                  _version.minor_number(), idx);
    RETURN_IF_ERROR(_l0->flush_to_immutable_index(l1_tmp_file, _version, true, true));

    VLOG(1) << "flush tmp l1, idx: " << idx << ", file_path: " << l1_tmp_file << " success";
    // load _l1_vec
    std::unique_ptr<RandomAccessFile> l1_rfile;
    ASSIGN_OR_RETURN(l1_rfile, _fs->new_random_access_file(l1_tmp_file));
    auto l1_st = ImmutableIndex::load(std::move(l1_rfile), load_bf_or_not());
    if (!l1_st.ok()) {
        LOG(ERROR) << "load tmp l1 failed, file_path: " << l1_tmp_file << ", status:" << l1_st.status();
        return l1_st.status();
    }
    _l1_vec.emplace_back(std::move(l1_st).value());
    _l1_merged_num.emplace_back(1);

    // clear l0
    _l0->clear();

    return Status::OK();
}

Status PersistentIndex::_flush_l0() {
    // when l1 or l2 exist, must flush l0 with Delete Flag
    return _l0->flush_to_immutable_index(_path, _version, false, !_l2_vec.empty() || !_l1_vec.empty());
}

Status PersistentIndex::_reload(const PersistentIndexMetaPB& index_meta) {
    auto l0_st = ShardByLengthMutableIndex::create(_key_size, _path);
    if (!l0_st.ok()) {
        return l0_st.status();
    }
    _l0 = std::move(l0_st).value();
    Status st = _load(index_meta, true);
    if (!st.ok()) {
        LOG(WARNING) << "reload persistent index failed, status: " << st.to_string();
    }
    return st;
}

size_t PersistentIndex::_dump_bound() {
    return (_l0 == nullptr) ? 0 : _l0->dump_bound();
}

// TODO: maybe build snapshot is better than append wals when almost
// operations are upsert or erase
bool PersistentIndex::_can_dump_directly() {
    return _dump_bound() <= config::l0_snapshot_size;
}

bool PersistentIndex::_l0_is_full(int64_t l1_l2_size) {
    const auto l0_mem_size = _l0->memory_usage();
    auto manager = StorageEngine::instance()->update_manager();
    // There are three condition that we regard l0 as full:
    // 1. l0's memory exceed config::l0_max_mem_usage
    // 2. l0's memory exceed l1 and l2 files size
    // 3. memory usage of update module is exceed and l0's memory exceed config::l0_min_mem_usage
    bool exceed_max_mem = l0_mem_size >= config::l0_max_mem_usage;
    bool exceed_index_size = (l1_l2_size > 0) ? l0_mem_size >= l1_l2_size : false;
    bool exceed_mem_limit = manager->mem_tracker()->limit_exceeded_by_ratio(config::memory_urgent_level) &&
                            l0_mem_size >= config::l0_min_mem_usage;
    return exceed_max_mem || exceed_index_size || exceed_mem_limit;
}

bool PersistentIndex::_need_flush_advance() {
    return _l0_is_full();
}

bool PersistentIndex::_need_merge_advance() {
    DCHECK(_l1_merged_num.size() == _l1_vec.size());
    if (_l1_merged_num.empty()) {
        return false;
    }
    int merged_num = _l1_merged_num[_l1_merged_num.size() - 1];
    int merged_candidate_num = 1;
    for (int i = _l1_merged_num.size() - 2; i >= 0; i--) {
        if (_l1_merged_num[i] == merged_num) {
            merged_candidate_num++;
        } else {
            break;
        }
    }
    return merged_candidate_num >= config::max_tmp_l1_num;
}

static StatusOr<EditVersionWithMerge> parse_l2_filename(const std::string& filename) {
    int64_t major, minor;
    if (sscanf(filename.c_str(), "index.l2.%" PRId64 ".%" PRId64, &major, &minor) != 2) {
        return Status::InvalidArgument(fmt::format("invalid l2 filename: {}", filename));
    }
    bool merged = StringPiece(filename).ends_with(".merged");
    return EditVersionWithMerge(major, minor, merged);
}

Status PersistentIndex::_delete_expired_index_file(const EditVersion& l0_version, const EditVersion& l1_version,
                                                   const EditVersionWithMerge& min_l2_version) {
    std::string l0_file_name =
            strings::Substitute("index.l0.$0.$1", l0_version.major_number(), l0_version.minor_number());
    std::string l1_file_name =
            strings::Substitute("index.l1.$0.$1", l1_version.major_number(), l1_version.minor_number());
    std::string l0_prefix("index.l0");
    std::string l1_prefix("index.l1");
    std::string l2_prefix("index.l2");
    std::string dir = _path;
    auto cb = [&](std::string_view name) -> bool {
        std::string full(name);
        if ((full.compare(0, l0_prefix.length(), l0_prefix) == 0 && full.compare(l0_file_name) != 0) ||
            (full.compare(0, l1_prefix.length(), l1_prefix) == 0 && full.compare(l1_file_name) != 0)) {
            std::string path = dir + "/" + full;
            VLOG(1) << "delete expired index file " << path;
            Status st = FileSystem::Default()->delete_file(path);
            if (!st.ok()) {
                LOG(WARNING) << "delete exprired index file: " << path << ", failed, status is " << st.to_string();
                return false;
            }
        }
        if (full.compare(0, l2_prefix.length(), l2_prefix) == 0) {
            auto version_st = parse_l2_filename(full);
            if (!version_st.ok()) {
                LOG(ERROR) << "Parse l2 file error: " << version_st.status();
            } else {
                // if l2 not exists now, min_l2_version will be [INT64_MAX, INT64_MAX], to remove all l2 files
                if ((*version_st) < min_l2_version) {
                    // delete expired l2 file
                    std::string path = dir + "/" + full;
                    VLOG(1) << "delete expired index file " << path;
                    Status st = FileSystem::Default()->delete_file(path);
                    if (!st.ok()) {
                        LOG(WARNING) << "delete exprired index file: " << path << ", failed, status is "
                                     << st.to_string();
                        return false;
                    }
                }
            }
        }
        return true;
    };
    return FileSystem::Default()->iterate_dir(_path, cb);
}

static bool major_compaction_tmp_index_file(const std::string& full) {
    std::string suffix = ".merged.tmp";
    if (full.length() >= suffix.length() &&
        full.compare(full.length() - suffix.length(), suffix.length(), suffix) == 0) {
        return true;
    } else {
        return false;
    }
}

Status PersistentIndex::_delete_major_compaction_tmp_index_file() {
    std::string dir = _path;
    auto cb = [&](std::string_view name) -> bool {
        std::string full(name);
        if (major_compaction_tmp_index_file(full)) {
            std::string path = dir + "/" + full;
            VLOG(1) << "delete tmp index file " << path;
            Status st = FileSystem::Default()->delete_file(path);
            if (!st.ok()) {
                LOG(WARNING) << "delete tmp index file: " << path << ", failed, status: " << st.to_string();
                return false;
            }
        }
        return true;
    };
    return FileSystem::Default()->iterate_dir(_path, cb);
}

Status PersistentIndex::_delete_tmp_index_file() {
    std::string dir = _path;
    auto cb = [&](std::string_view name) -> bool {
        std::string suffix = ".tmp";
        std::string full(name);
        if (full.length() >= suffix.length() &&
            full.compare(full.length() - suffix.length(), suffix.length(), suffix) == 0 &&
            !major_compaction_tmp_index_file(full)) {
            std::string path = dir + "/" + full;
            VLOG(1) << "delete tmp index file " << path;
            Status st = FileSystem::Default()->delete_file(path);
            if (!st.ok()) {
                LOG(WARNING) << "delete tmp index file: " << path << ", failed, status: " << st.to_string();
                return false;
            }
        }
        return true;
    };
    return FileSystem::Default()->iterate_dir(_path, cb);
}

template <size_t KeySize>
struct KVRefEq {
    bool operator()(const KVRef& lhs, const KVRef& rhs) const {
        return lhs.hash == rhs.hash && memcmp(lhs.kv_pos, rhs.kv_pos, KeySize) == 0;
    }
};

template <>
struct KVRefEq<0> {
    bool operator()(const KVRef& lhs, const KVRef& rhs) const {
        return lhs.hash == rhs.hash && lhs.size == rhs.size &&
               memcmp(lhs.kv_pos, rhs.kv_pos, lhs.size - kIndexValueSize) == 0;
    }
};

struct KVRefHash {
    uint64_t operator()(const KVRef& kv) const { return kv.hash; }
};

template <size_t KeySize>
Status merge_shard_kvs_fixed_len(std::vector<KVRef>& l0_kvs, std::vector<std::vector<KVRef>>& l1_kvs,
                                 size_t estimated_size, std::vector<KVRef>& ret) {
    phmap::flat_hash_set<KVRef, KVRefHash, KVRefEq<KeySize>> kvs_set;
    kvs_set.reserve(estimated_size);
    DCHECK(!l1_kvs.empty());
    for (const auto& kv : l1_kvs[0]) {
        const auto v = UNALIGNED_LOAD64(kv.kv_pos + KeySize);
        if (v == NullIndexValue) {
            continue;
        }
        const auto [_, inserted] = kvs_set.emplace(kv);
        DCHECK(inserted) << "duplicate key found when in l1 index";
        if (!inserted) {
            return Status::InternalError("duplicate key found in l1 index");
        }
    }
    for (size_t i = 1; i < l1_kvs.size(); i++) {
        for (const auto& kv : l1_kvs[i]) {
            const auto v = UNALIGNED_LOAD64(kv.kv_pos + KeySize);
            if (v == NullIndexValue) {
                kvs_set.erase(kv);
            } else {
                auto [it, inserted] = kvs_set.emplace(kv);
                if (!inserted) {
                    DCHECK(it->hash == kv.hash) << "upsert kv in set, hash should be the same";
                    kvs_set.erase(it);
                    kvs_set.emplace(kv);
                }
            }
        }
    }
    for (const auto& kv : l0_kvs) {
        const auto v = UNALIGNED_LOAD64(kv.kv_pos + KeySize);
        if (v == NullIndexValue) {
            // delete
            kvs_set.erase(kv);
        } else {
            auto [it, inserted] = kvs_set.emplace(kv);
            if (!inserted) {
                DCHECK(it->hash == kv.hash) << "upsert kv in set, hash should be the same";
                // TODO: find a way to modify iterator directly, currently just erase then re-insert
                // it->kv_pos = kv.kv_pos;
                kvs_set.erase(it);
                kvs_set.emplace(kv);
            }
        }
    }
    ret.reserve(ret.size() + kvs_set.size());
    for (const auto& kv : kvs_set) {
        ret.emplace_back(kv);
    }
    return Status::OK();
}

Status merge_shard_kvs_var_len(std::vector<KVRef>& l0_kvs, std::vector<std::vector<KVRef>>& l1_kvs,
                               size_t estimate_size, std::vector<KVRef>& ret) {
    phmap::flat_hash_set<KVRef, KVRefHash, KVRefEq<0>> kvs_set;
    kvs_set.reserve(estimate_size);
    DCHECK(!l1_kvs.empty());
    for (const auto& kv : l1_kvs[0]) {
        const auto v = UNALIGNED_LOAD64(kv.kv_pos + kv.size - kIndexValueSize);
        if (v == NullIndexValue) {
            continue;
        }
        const auto [_, inserted] = kvs_set.emplace(kv);
        DCHECK(inserted) << "duplicate key found when in l1 index";
        if (!inserted) {
            return Status::InternalError("duplicate key found in l1 index");
        }
    }
    for (size_t i = 1; i < l1_kvs.size(); i++) {
        for (const auto& kv : l1_kvs[i]) {
            const auto v = UNALIGNED_LOAD64(kv.kv_pos + kv.size - kIndexValueSize);
            if (v == NullIndexValue) {
                kvs_set.erase(kv);
            } else {
                auto [it, inserted] = kvs_set.emplace(kv);
                if (!inserted) {
                    DCHECK(it->hash == kv.hash) << "upsert kv in set, hash should be the same";
                    kvs_set.erase(it);
                    kvs_set.emplace(kv);
                }
            }
        }
    }
    for (auto& kv : l0_kvs) {
        const uint64_t v = UNALIGNED_LOAD64(kv.kv_pos + kv.size - kIndexValueSize);
        if (v == NullIndexValue) {
            // delete
            kvs_set.erase(kv);
        } else {
            if (auto [it, inserted] = kvs_set.emplace(kv); !inserted) {
                DCHECK(it->hash == kv.hash) << "upsert kv in set, hash should be the same";
                // TODO: find a way to modify iterator directly, currently just erase then re-insert
                // it->kv_pos = kv.kv_pos;
                kvs_set.erase(it);
                kvs_set.emplace(kv);
            }
        }
    }
    ret.reserve(ret.size() + kvs_set.size());
    for (const auto& kv : kvs_set) {
        ret.emplace_back(kv);
    }
    return Status::OK();
}

static Status merge_shard_kvs(size_t key_size, std::vector<KVRef>& l0_kvs, std::vector<std::vector<KVRef>>& l1_kvs,
                              size_t estimated_size, std::vector<KVRef>& ret) {
    if (key_size > 0) {
#define CASE_SIZE(s) \
    case s:          \
        return merge_shard_kvs_fixed_len<s>(l0_kvs, l1_kvs, estimated_size, ret);
#define CASE_SIZE_8(s) \
    CASE_SIZE(s)       \
    CASE_SIZE(s + 1)   \
    CASE_SIZE(s + 2)   \
    CASE_SIZE(s + 3)   \
    CASE_SIZE(s + 4)   \
    CASE_SIZE(s + 5)   \
    CASE_SIZE(s + 6)   \
    CASE_SIZE(s + 7)
        switch (key_size) {
            CASE_SIZE_8(1)
            CASE_SIZE_8(9)
            CASE_SIZE_8(17)
            CASE_SIZE_8(25)
            CASE_SIZE_8(33)
            CASE_SIZE_8(41)
            CASE_SIZE_8(49)
            CASE_SIZE_8(57)
            CASE_SIZE_8(65)
            CASE_SIZE_8(73)
            CASE_SIZE_8(81)
            CASE_SIZE_8(89)
            CASE_SIZE_8(97)
            CASE_SIZE_8(105)
            CASE_SIZE_8(113)
            CASE_SIZE_8(121)
#undef CASE_SIZE_8
#undef CASE_SIZE
        }
    } else if (key_size == 0) {
        return merge_shard_kvs_var_len(l0_kvs, l1_kvs, estimated_size, ret);
    }
    return Status::OK();
}

template <size_t KeySize>
Status merge_shard_kvs_fixed_len_with_delete(std::vector<KVRef>& l0_kvs, std::vector<std::vector<KVRef>>& l1_kvs,
                                             size_t estimated_size, std::vector<KVRef>& ret) {
    phmap::flat_hash_set<KVRef, KVRefHash, KVRefEq<KeySize>> kvs_set;
    kvs_set.reserve(estimated_size);
    DCHECK(!l1_kvs.empty());
    for (auto& l1_kv : l1_kvs) {
        for (const auto& kv : l1_kv) {
            auto [it, inserted] = kvs_set.emplace(kv);
            if (!inserted) {
                DCHECK(it->hash == kv.hash) << "upsert kv in set, hash should be the same";
                kvs_set.erase(it);
                kvs_set.emplace(kv);
            }
        }
    }
    for (const auto& kv : l0_kvs) {
        auto [it, inserted] = kvs_set.emplace(kv);
        if (!inserted) {
            DCHECK(it->hash == kv.hash) << "upsert kv in set, hash should be the same";
            // TODO: find a way to modify iterator directly, currently just erase then re-insert
            // it->kv_pos = kv.kv_pos;
            kvs_set.erase(it);
            kvs_set.emplace(kv);
        }
    }
    ret.reserve(ret.size() + kvs_set.size());
    for (const auto& kv : kvs_set) {
        ret.emplace_back(kv);
    }
    return Status::OK();
}

Status merge_shard_kvs_var_len_with_delete(std::vector<KVRef>& l0_kvs, std::vector<std::vector<KVRef>>& l1_kvs,
                                           size_t estimate_size, std::vector<KVRef>& ret) {
    phmap::flat_hash_set<KVRef, KVRefHash, KVRefEq<0>> kvs_set;
    kvs_set.reserve(estimate_size);
    DCHECK(!l1_kvs.empty());
    for (auto& l1_kv : l1_kvs) {
        for (const auto& kv : l1_kv) {
            auto [it, inserted] = kvs_set.emplace(kv);
            if (!inserted) {
                DCHECK(it->hash == kv.hash) << "upsert kv in set, hash should be the same";
                kvs_set.erase(it);
                kvs_set.emplace(kv);
            }
        }
    }
    for (auto& kv : l0_kvs) {
        if (auto [it, inserted] = kvs_set.emplace(kv); !inserted) {
            DCHECK(it->hash == kv.hash) << "upsert kv in set, hash should be the same";
            // TODO: find a way to modify iterator directly, currently just erase then re-insert
            // it->kv_pos = kv.kv_pos;
            kvs_set.erase(it);
            kvs_set.emplace(kv);
        }
    }
    ret.reserve(ret.size() + kvs_set.size());
    for (const auto& kv : kvs_set) {
        ret.emplace_back(kv);
    }
    return Status::OK();
}

static Status merge_shard_kvs_with_delete(size_t key_size, std::vector<KVRef>& l0_kvs,
                                          std::vector<std::vector<KVRef>>& l1_kvs, size_t estimated_size,
                                          std::vector<KVRef>& ret) {
    if (key_size > 0) {
#define CASE_SIZE(s) \
    case s:          \
        return merge_shard_kvs_fixed_len_with_delete<s>(l0_kvs, l1_kvs, estimated_size, ret);
#define CASE_SIZE_8(s) \
    CASE_SIZE(s)       \
    CASE_SIZE(s + 1)   \
    CASE_SIZE(s + 2)   \
    CASE_SIZE(s + 3)   \
    CASE_SIZE(s + 4)   \
    CASE_SIZE(s + 5)   \
    CASE_SIZE(s + 6)   \
    CASE_SIZE(s + 7)
        switch (key_size) {
            CASE_SIZE_8(1)
            CASE_SIZE_8(9)
            CASE_SIZE_8(17)
            CASE_SIZE_8(25)
            CASE_SIZE_8(33)
            CASE_SIZE_8(41)
            CASE_SIZE_8(49)
            CASE_SIZE_8(57)
            CASE_SIZE_8(65)
            CASE_SIZE_8(73)
            CASE_SIZE_8(81)
            CASE_SIZE_8(89)
            CASE_SIZE_8(97)
            CASE_SIZE_8(105)
            CASE_SIZE_8(113)
            CASE_SIZE_8(121)
#undef CASE_SIZE_8
#undef CASE_SIZE
        }
    } else if (key_size == 0) {
        return merge_shard_kvs_var_len_with_delete(l0_kvs, l1_kvs, estimated_size, ret);
    }
    return Status::OK();
}

Status PersistentIndex::_merge_compaction_internal(ImmutableIndexWriter* writer, int l1_start_idx, int l1_end_idx,
                                                   std::map<uint32_t, std::pair<int64_t, int64_t>>& usage_and_size_stat,
                                                   bool keep_delete) {
    for (const auto& [key_size, shard_info] : _l0->_shard_info_by_key_size) {
        size_t total_usage = 0;
        size_t total_size = 0;
        auto iter = usage_and_size_stat.find(key_size);
        if (iter != usage_and_size_stat.end()) {
            total_usage = iter->second.first;
            total_size = iter->second.second;
        }
        auto [l0_shard_offset, l0_shard_size] = shard_info;
        const auto [nshard, npage_hint] = MutableIndex::estimate_nshard_and_npage(total_usage);
        const auto nbucket = MutableIndex::estimate_nbucket(key_size, total_usage, nshard, npage_hint);
        const auto estimate_size_per_shard = total_size / nshard;
        if (_key_size > 0) {
            l0_shard_offset = 0;
        }
        const auto l0_kv_pairs_size = std::accumulate(std::next(_l0->_shards.begin(), l0_shard_offset),
                                                      std::next(_l0->_shards.begin(), l0_shard_offset + l0_shard_size),
                                                      0UL, [](size_t s, const auto& e) { return s + e->size(); });
        auto l0_kvs_by_shard = _l0->_shards[l0_shard_offset]->get_kv_refs_by_shard(nshard, l0_kv_pairs_size, true);

        int merge_l1_num = l1_end_idx - l1_start_idx;
        std::vector<std::vector<std::vector<KVRef>>> l1_kvs_by_shard;
        std::vector<int32_t> finished_l1_idx(merge_l1_num, -1);
        std::vector<std::pair<size_t, size_t>> l1_shard_info(merge_l1_num, std::make_pair<size_t, size_t>(0, 0));
        size_t index_num = 0;
        for (size_t i = 0; i < merge_l1_num; i++) {
            auto iter = _l1_vec[i + l1_start_idx]->_shard_info_by_length.find(key_size);
            if (iter != _l1_vec[i + l1_start_idx]->_shard_info_by_length.end()) {
                l1_shard_info[i] = iter->second;
                index_num += (iter->second.second / nshard) + 1;
            }
            std::vector<std::vector<KVRef>> elem(nshard);
            l1_kvs_by_shard.emplace_back(elem);
        }
        std::vector<std::unique_ptr<ImmutableIndexShard>> index_shards(index_num);
        uint32_t shard_bits = log2(nshard);
        // shard iteration example:
        //
        // nshard_l1(4) < nshard(8)
        //        l1_shard_idx: 0     1     2     3
        //       cur_shard_idx: 0  1  2  3  4  5  6  7
        //     start_shard_idx: 0  0  1  1  2  2  3  3
        //       end_shard_idx: 0  1  1  2  2  3  3  4
        //
        // nshard_l1(4) = nshard(4)
        //        l1_shard_idx: 0     1     2     3
        //       cur_shard_idx: 0     1     2     3
        //     start_shard_idx: 0     1     2     3
        //       end_shard_idx: 1     2     3     4
        //
        // nshard_l1(8) > nshard(4)
        //        l1_shard_idx: 0  1  2  3  4  5  6  7
        //       cur_shard_idx: 0     1     2     3
        //     start_shard_idx: 0     2     4     6
        //       end_shard_idx: 2     4     6     8
        for (size_t shard_idx = 0; shard_idx < nshard; shard_idx++) {
            size_t index_shard_idx = 0;
            for (size_t l1_idx = 0; l1_idx < merge_l1_num; l1_idx++) {
                if (l1_shard_info[l1_idx].second == 0) {
                    continue;
                }
                int32_t shard_idx_start = shard_idx * l1_shard_info[l1_idx].second / nshard;
                int32_t shard_idx_end = (shard_idx + 1) * l1_shard_info[l1_idx].second / nshard;
                do {
                    if (finished_l1_idx[l1_idx] < shard_idx_start) {
                        //get kv for l1
                        RETURN_IF_ERROR(_l1_vec[l1_idx + l1_start_idx]->_get_kvs_for_shard(
                                l1_kvs_by_shard[l1_idx], l1_shard_info[l1_idx].first + shard_idx_start, shard_bits,
                                &index_shards[index_shard_idx]));
                        finished_l1_idx[l1_idx] = shard_idx_start;
                    }
                    index_shard_idx++;
                    shard_idx_start++;
                } while (shard_idx_start < shard_idx_end);
            }

            //merge_shard_kvs
            std::vector<KVRef> kvs;
            std::vector<std::vector<KVRef>> l1_kvs(merge_l1_num);
            for (size_t i = 0; i < merge_l1_num; i++) {
                l1_kvs[i].swap(l1_kvs_by_shard[i][shard_idx]);
            }
            if (keep_delete) {
                RETURN_IF_ERROR(merge_shard_kvs_with_delete(key_size, l0_kvs_by_shard[shard_idx], l1_kvs,
                                                            estimate_size_per_shard, kvs));
            } else {
                RETURN_IF_ERROR(
                        merge_shard_kvs(key_size, l0_kvs_by_shard[shard_idx], l1_kvs, estimate_size_per_shard, kvs));
            }
            // write shard
            RETURN_IF_ERROR(writer->write_shard(key_size, npage_hint, nbucket, kvs));
            // clear shard
            l0_kvs_by_shard[shard_idx].clear();
            l0_kvs_by_shard[shard_idx].shrink_to_fit();
        }
    }
    return Status::OK();
}

size_t PersistentIndex::_get_tmp_l1_count() {
    return _has_l1 ? _l1_vec.size() - 1 : _l1_vec.size();
}

// There are a few steps in minor compaction:
// 1. flush l0 to l1:
//    a. if there is only one tmp-l1 file, move this tmp-l1 to l1 file.
//    b. if there are > 2 tmp-l1 file, then merge l0 and tmp-l1 files to new l1 file.
//    c. if there is only one l1 file, flush l0 to new l1 file.
//    d. if there is not l1 file exists, flush l0 to l1 file.
// 2. move old l1 to l2. only if old l1 exist
// 3. modify PersistentIndex meta
Status PersistentIndex::_minor_compaction(PersistentIndexMetaPB* index_meta) {
    // 1. flush l0 to l1
    const std::string new_l1_filename =
            strings::Substitute("$0/index.l1.$1.$2", _path, _version.major_number(), _version.minor_number());
    const size_t tmp_l1_cnt = _get_tmp_l1_count();
    // maybe need to dump snapshot in 1.a
    bool need_snapshot = false;
    if (tmp_l1_cnt == 1) {
        // step 1.a
        // move tmp l1 to l1
        std::string tmp_l1_filename = _l1_vec[_has_l1 ? 1 : 0]->filename();
        // Make new file doesn't exist
        (void)FileSystem::Default()->delete_file(new_l1_filename);
        RETURN_IF_ERROR(FileSystem::Default()->link_file(tmp_l1_filename, new_l1_filename));
        if (_l0->size() > 0) {
            // check if need to dump snapshot
            need_snapshot = true;
        }
        LOG(INFO) << "PersistentIndex minor compaction, link from tmp-l1: " << tmp_l1_filename
                  << " to l1: " << new_l1_filename << " snapshot: " << need_snapshot;
    } else if (tmp_l1_cnt > 1) {
        // step 1.b
        auto writer = std::make_unique<ImmutableIndexWriter>();
        RETURN_IF_ERROR(writer->init(new_l1_filename, _version, true));
        // followe this rules:
        // 1, remove delete key when l2 not exist
        // 2. skip merge l1, only merge tmp-l1 and l0
        RETURN_IF_ERROR(_reload_usage_and_size_by_key_length(_has_l1 ? 1 : 0, _l1_vec.size(), false));
        // keep delete flag when l2 or older l1 exist
        RETURN_IF_ERROR(_merge_compaction_internal(writer.get(), _has_l1 ? 1 : 0, _l1_vec.size(),
                                                   _usage_and_size_by_key_length, !_l2_vec.empty() || _has_l1));
        RETURN_IF_ERROR(writer->finish());
        LOG(INFO) << "PersistentIndex minor compaction, merge tmp l1, merge cnt: " << _l1_vec.size()
                  << ", output: " << new_l1_filename;
    } else if (_l1_vec.size() == 1) {
        // step 1.c
        RETURN_IF_ERROR(_flush_l0());
        DCHECK(_has_l1);
        LOG(INFO) << "PersistentIndex minor compaction, flush l0, old l1: " << _l1_version
                  << ", output: " << new_l1_filename;
    } else {
        // step 1.d
        RETURN_IF_ERROR(_flush_l0());
        DCHECK(!_has_l1);
        LOG(INFO) << "PersistentIndex minor compaction, flush l0, "
                  << "output: " << new_l1_filename;
    }
    // 2. move old l1 to l2.
    if (_has_l1) {
        // just link old l1 file to l2
        const std::string l2_file_path =
                strings::Substitute("$0/index.l2.$1.$2", _path, _l1_version.major_number(), _l1_version.minor_number());
        const std::string old_l1_file_path =
                strings::Substitute("$0/index.l1.$1.$2", _path, _l1_version.major_number(), _l1_version.minor_number());
        LOG(INFO) << "PersistentIndex minor compaction, link from " << old_l1_file_path << " to " << l2_file_path;
        // Make new file doesn't exist
        (void)FileSystem::Default()->delete_file(l2_file_path);
        RETURN_IF_ERROR(FileSystem::Default()->link_file(old_l1_file_path, l2_file_path));
        _l1_version.to_pb(index_meta->add_l2_versions());
        index_meta->add_l2_version_merged(false);
    }
    // 3. modify meta
    index_meta->set_size(_size);
    index_meta->set_usage(_usage);
    index_meta->set_format_version(PERSISTENT_INDEX_VERSION_4);
    _version.to_pb(index_meta->mutable_version());
    _version.to_pb(index_meta->mutable_l1_version());
    MutableIndexMetaPB* l0_meta = index_meta->mutable_l0_meta();
    RETURN_IF_ERROR(_l0->commit(l0_meta, _version, need_snapshot ? kSnapshot : kFlush));
    return Status::OK();
}

Status PersistentIndex::_merge_compaction() {
    if (_l1_vec.empty()) {
        return Status::InternalError("cannot do merge_compaction without l1");
    }
    auto writer = std::make_unique<ImmutableIndexWriter>();
    const std::string idx_file_path =
            strings::Substitute("$0/index.l1.$1.$2", _path, _version.major_number(), _version.minor_number());
    RETURN_IF_ERROR(writer->init(idx_file_path, _version, true));
    RETURN_IF_ERROR(_merge_compaction_internal(writer.get(), 0, _l1_vec.size(), _usage_and_size_by_key_length,
                                               !_l2_vec.empty()));
    // _usage should be equal to total_kv_size. But they may be differen because of compatibility problem when we upgrade
    // from old version and _usage maybe not accurate.
    // so we use total_kv_size to correct the _usage.
    if (_usage != writer->total_kv_size()) {
        _usage = writer->total_kv_size();
    }
    if (_l2_vec.size() == 0 && _size != writer->total_kv_num()) {
        std::string msg =
                strings::Substitute("inconsistent kv num after merge compaction, actual:$0, expect:$1, index_file:$2",
                                    writer->total_kv_num(), _size, writer->index_file());
        LOG(ERROR) << msg;
        return Status::InternalError(msg);
    }
    return writer->finish();
}

Status PersistentIndex::_merge_compaction_advance() {
    DCHECK(_l1_vec.size() >= config::max_tmp_l1_num);
    auto writer = std::make_unique<ImmutableIndexWriter>();
    const std::string idx_file_path_tmp = strings::Substitute(
            "$0/index.l1.$1.$2.$3.tmp", _path, _version.major_number(), _version.minor_number(), _l1_vec.size());
    RETURN_IF_ERROR(writer->init(idx_file_path_tmp, _version, false));
    int merge_l1_start_idx = _l1_vec.size() - config::max_tmp_l1_num;
    int merge_l1_end_idx = _l1_vec.size();
    LOG(INFO) << strings::Substitute("merge compaction advance, path: $0, start_idx: $1, end_idx: $2", _path,
                                     merge_l1_start_idx, merge_l1_end_idx);
    // keep delete flag when older l1 or l2 exist
    bool keep_delete = (merge_l1_start_idx != 0) || !_l2_vec.empty();

    std::map<uint32_t, std::pair<int64_t, int64_t>> usage_and_size_stat;
    for (const auto& [key_size, shard_info] : _l0->_shard_info_by_key_size) {
        auto [l0_shard_offset, l0_shard_size] = shard_info;
        const auto l0_kv_pairs_size = std::accumulate(std::next(_l0->_shards.begin(), l0_shard_offset),
                                                      std::next(_l0->_shards.begin(), l0_shard_offset + l0_shard_size),
                                                      0L, [](size_t s, const auto& e) { return s + e->size(); });
        const auto l0_kv_pairs_usage = std::accumulate(std::next(_l0->_shards.begin(), l0_shard_offset),
                                                       std::next(_l0->_shards.begin(), l0_shard_offset + l0_shard_size),
                                                       0L, [](size_t s, const auto& e) { return s + e->usage(); });
        usage_and_size_stat.insert({key_size, {l0_kv_pairs_usage, l0_kv_pairs_size}});
    }
    for (int i = merge_l1_start_idx; i < merge_l1_end_idx; i++) {
        for (const auto& [key_size, shard_info] : _l1_vec[i]->_shard_info_by_length) {
            auto [l1_shard_offset, l1_shard_size] = shard_info;
            const int64_t size =
                    std::accumulate(std::next(_l1_vec[i]->_shards.begin(), l1_shard_offset),
                                    std::next(_l1_vec[i]->_shards.begin(), l1_shard_offset + l1_shard_size), 0L,
                                    [](size_t s, const auto& e) { return s + e.size; });
            const int64_t usage =
                    std::accumulate(std::next(_l1_vec[i]->_shards.begin(), l1_shard_offset),
                                    std::next(_l1_vec[i]->_shards.begin(), l1_shard_offset + l1_shard_size), 0L,
                                    [](size_t s, const auto& e) { return s + e.data_size; });

            auto iter = usage_and_size_stat.find(key_size);
            if (iter == usage_and_size_stat.end()) {
                usage_and_size_stat.insert({static_cast<uint32_t>(key_size), {usage, size}});
            } else {
                iter->second.first += usage;
                iter->second.second += size;
            }
        }
    }

    RETURN_IF_ERROR(_merge_compaction_internal(writer.get(), merge_l1_start_idx, merge_l1_end_idx, usage_and_size_stat,
                                               keep_delete));
    RETURN_IF_ERROR(writer->finish());
    std::vector<std::unique_ptr<ImmutableIndex>> new_l1_vec;
    std::vector<int> new_l1_merged_num;
    size_t merge_num = _l1_merged_num[merge_l1_start_idx];
    for (int i = 0; i < merge_l1_start_idx; i++) {
        new_l1_vec.emplace_back(std::move(_l1_vec[i]));
        new_l1_merged_num.emplace_back(_l1_merged_num[i]);
    }

    for (int i = merge_l1_start_idx; i < _l1_vec.size(); i++) {
        _l1_vec[i]->destroy();
    }

    const std::string idx_file_path = strings::Substitute("$0/index.l1.$1.$2.$3.tmp", _path, _version.major_number(),
                                                          _version.minor_number(), new_l1_vec.size());
    RETURN_IF_ERROR(FileSystem::Default()->rename_file(idx_file_path_tmp, idx_file_path));
    std::unique_ptr<RandomAccessFile> l1_rfile;
    ASSIGN_OR_RETURN(l1_rfile, _fs->new_random_access_file(idx_file_path));
    auto l1_st = ImmutableIndex::load(std::move(l1_rfile), load_bf_or_not());
    if (!l1_st.ok()) {
        return l1_st.status();
    }
    new_l1_vec.emplace_back(std::move(l1_st).value());
    new_l1_merged_num.emplace_back((merge_l1_end_idx - merge_l1_start_idx) * merge_num);
    _l1_vec.swap(new_l1_vec);
    _l1_merged_num.swap(new_l1_merged_num);
    _l0->clear();
    return Status::OK();
}

// generate all possible key size in l1 and l2
static void generate_all_key_size(size_t key_size, std::vector<size_t>& key_size_list) {
    if (key_size > 0) {
        key_size_list.push_back(key_size);
    } else {
        for (size_t i = 0; i <= kSliceMaxFixLength; i++) {
            key_size_list.push_back(i);
        }
    }
}

static void major_compaction_debug_str(const std::vector<EditVersion>& l2_versions,
                                       const std::vector<std::unique_ptr<ImmutableIndex>>& l2_vec,
                                       const EditVersion& output_version,
                                       const std::unique_ptr<ImmutableIndexWriter>& writer,
                                       std::stringstream& debug_str) {
    debug_str << "input : [";
    for (int i = 0; i < l2_versions.size(); i++) {
        debug_str << "(ver: " << l2_versions[i] << ", file_sz: " << l2_vec[i]->file_size()
                  << ", kv_cnt: " << l2_vec[i]->total_size() << ") ";
    }
    debug_str << "] output : (ver: " << output_version << ", file_sz: " << writer->file_size()
              << ", kv_cnt: " << writer->total_kv_size() << ") ";
}

StatusOr<EditVersion> PersistentIndex::_major_compaction_impl(
        const std::vector<EditVersion>& l2_versions, const std::vector<std::unique_ptr<ImmutableIndex>>& l2_vec) {
    DCHECK(l2_versions.size() == l2_vec.size());
    MonotonicStopWatch watch;
    watch.start();
    auto writer = std::make_unique<ImmutableIndexWriter>();
    // use latest l2 edit version as new l2 edit version
    EditVersion new_l2_version = l2_versions.back();
    const std::string idx_file_path = strings::Substitute("$0/index.l2.$1.$2$3", _path, new_l2_version.major_number(),
                                                          new_l2_version.minor_number(), MergeSuffix);
    RETURN_IF_ERROR(writer->init(idx_file_path, new_l2_version, true));
    std::map<uint32_t, std::pair<int64_t, int64_t>> usage_and_size_stat;
    _get_l2_stat(l2_vec, usage_and_size_stat);
    std::vector<size_t> key_size_list;
    generate_all_key_size(_key_size, key_size_list);
    for (const size_t key_size : key_size_list) {
        size_t total_usage = 0;
        size_t total_size = 0;
        auto iter = usage_and_size_stat.find(key_size);
        if (iter != usage_and_size_stat.end()) {
            // we use the average usage and size as total usage and size, to avoid disk waste
            // They are may smaller than real size, but we can increase page count later, so it's ok
            total_usage = iter->second.first / l2_versions.size();
            total_size = iter->second.second / l2_versions.size();
        }

        const auto [nshard, npage_hint] = MutableIndex::estimate_nshard_and_npage(total_usage);
        const auto nbucket = MutableIndex::estimate_nbucket(key_size, total_usage, nshard, npage_hint);
        const auto estimate_size_per_shard = total_size / nshard;

        std::vector<std::vector<std::vector<KVRef>>> l2_kvs_by_shard;
        std::vector<int32_t> finished_l2_idx(l2_vec.size(), -1);
        std::vector<std::pair<size_t, size_t>> l2_shard_info(l2_vec.size(), std::make_pair<size_t, size_t>(0, 0));
        size_t index_num = 0;
        for (int l2_idx = 0; l2_idx < l2_vec.size(); l2_idx++) {
            auto iter = l2_vec[l2_idx]->_shard_info_by_length.find(key_size);
            if (iter != l2_vec[l2_idx]->_shard_info_by_length.end()) {
                l2_shard_info[l2_idx] = iter->second;
                index_num += (iter->second.second / nshard) + 1;
            }
            std::vector<std::vector<KVRef>> elem(nshard);
            l2_kvs_by_shard.emplace_back(elem);
        }
        // use index_shards to store shard info when read from l2
        std::vector<std::unique_ptr<ImmutableIndexShard>> index_shards(index_num);
        uint32_t shard_bits = log2(nshard);

        for (size_t shard_idx = 0; shard_idx < nshard; shard_idx++) {
            size_t index_shard_idx = 0;
            for (int l2_idx = 0; l2_idx < l2_vec.size(); l2_idx++) {
                if (l2_shard_info[l2_idx].second == 0) {
                    continue;
                }
                int32_t shard_idx_start = shard_idx * l2_shard_info[l2_idx].second / nshard;
                int32_t shard_idx_end = (shard_idx + 1) * l2_shard_info[l2_idx].second / nshard;
                do {
                    //get kv for l2
                    if (finished_l2_idx[l2_idx] < shard_idx_start) {
                        RETURN_IF_ERROR(l2_vec[l2_idx]->_get_kvs_for_shard(
                                l2_kvs_by_shard[l2_idx], l2_shard_info[l2_idx].first + shard_idx_start, shard_bits,
                                &index_shards[index_shard_idx]));
                        finished_l2_idx[l2_idx] = shard_idx_start;
                    }
                    index_shard_idx++;
                    shard_idx_start++;
                } while (shard_idx_start < shard_idx_end);
            }

            //merge_shard_kvs
            std::vector<KVRef> kvs;
            std::vector<std::vector<KVRef>> l2_kvs(l2_vec.size());
            for (int l2_idx = 0; l2_idx < l2_vec.size(); l2_idx++) {
                l2_kvs[l2_idx].swap(l2_kvs_by_shard[l2_idx][shard_idx]);
            }
            // empty l0 kvs
            std::vector<KVRef> empty_l0_kvs;
            RETURN_IF_ERROR(merge_shard_kvs(key_size, empty_l0_kvs, l2_kvs, estimate_size_per_shard, kvs));
            // write shard
            RETURN_IF_ERROR(writer->write_shard(key_size, npage_hint, nbucket, kvs));
        }
    }
    RETURN_IF_ERROR(writer->finish());
    std::stringstream debug_str;
    major_compaction_debug_str(l2_versions, l2_vec, new_l2_version, writer, debug_str);
    LOG(INFO) << "PersistentIndex background compact l2 : " << debug_str.str() << " cost: " << watch.elapsed_time();
    return new_l2_version;
}

void PersistentIndex::modify_l2_versions(const std::vector<EditVersion>& input_l2_versions,
                                         const EditVersion& output_l2_version, PersistentIndexMetaPB& index_meta) {
    // delete input l2 versions, and add output l2 version
    std::vector<EditVersion> new_l2_versions;
    std::vector<bool> new_l2_version_merged;
    // put new output l2 version to first position
    new_l2_versions.push_back(output_l2_version);
    new_l2_version_merged.push_back(true);
    for (int i = 0; i < index_meta.l2_versions_size(); i++) {
        bool need_remove = false;
        for (const auto& input_ver : input_l2_versions) {
            if (EditVersion(index_meta.l2_versions(i)) == input_ver) {
                need_remove = true;
                break;
            }
        }
        if (!need_remove) {
            new_l2_versions.emplace_back(EditVersion(index_meta.l2_versions(i)));
            new_l2_version_merged.push_back(index_meta.l2_version_merged(i));
        }
    }
    // rebuild l2 versions in meta
    index_meta.clear_l2_versions();
    index_meta.clear_l2_version_merged();
    for (const auto& ver : new_l2_versions) {
        ver.to_pb(index_meta.add_l2_versions());
    }
    for (const bool merge : new_l2_version_merged) {
        index_meta.add_l2_version_merged(merge);
    }
}

Status PersistentIndex::TEST_major_compaction(PersistentIndexMetaPB& index_meta) {
    if (index_meta.l2_versions_size() <= 1) {
        return Status::OK();
    }
    // 1. load current l2 vec
    std::vector<EditVersion> l2_versions;
    std::vector<std::unique_ptr<ImmutableIndex>> l2_vec;
    DCHECK(index_meta.l2_versions_size() == index_meta.l2_version_merged_size());
    for (int i = 0; i < index_meta.l2_versions_size(); i++) {
        l2_versions.emplace_back(index_meta.l2_versions(i));
        auto l2_block_path = strings::Substitute("$0/index.l2.$1.$2$3", _path, index_meta.l2_versions(i).major_number(),
                                                 index_meta.l2_versions(i).minor_number(),
                                                 index_meta.l2_version_merged(i) ? MergeSuffix : "");
        ASSIGN_OR_RETURN(auto l2_rfile, _fs->new_random_access_file(l2_block_path));
        ASSIGN_OR_RETURN(auto l2_index, ImmutableIndex::load(std::move(l2_rfile), load_bf_or_not()));
        l2_vec.emplace_back(std::move(l2_index));
    }
    // 2. merge l2 files to new l2 file
    ASSIGN_OR_RETURN(EditVersion new_l2_version, _major_compaction_impl(l2_versions, l2_vec));
    modify_l2_versions(l2_versions, new_l2_version, index_meta);
    // delete useless files
    RETURN_IF_ERROR(_reload(index_meta));
    RETURN_IF_ERROR(_delete_expired_index_file(
            _version, _l1_version,
            _l2_versions.size() > 0 ? _l2_versions[0] : EditVersionWithMerge(INT64_MAX, INT64_MAX, true)));
    (void)_delete_major_compaction_tmp_index_file();
    return Status::OK();
}

// Major compaction will merge compaction l2 files, contains a few steps:
// 1. load current l2 vec
// 2. merge l2 files to new l2 file
// 3. modify PersistentIndexMetaPB and make this step atomic.
Status PersistentIndex::major_compaction(DataDir* data_dir, int64_t tablet_id, std::timed_mutex* mutex) {
    if (_cancel_major_compaction) {
        return Status::InternalError("cancel major compaction");
    }
    bool expect_running_state = false;
    if (!_major_compaction_running.compare_exchange_strong(expect_running_state, true)) {
        // already in compaction
        return Status::OK();
    }
    DeferOp defer([&]() {
        _major_compaction_running.store(false);
        _cancel_major_compaction = false;
    });
    // re-use config update_compaction_per_tablet_min_interval_seconds here to control pk index major compaction
    if (UnixSeconds() - _latest_compaction_time <= config::update_compaction_per_tablet_min_interval_seconds) {
        return Status::OK();
    }
    _latest_compaction_time = UnixSeconds();
    // merge all l2 files
    PersistentIndexMetaPB prev_index_meta;
    RETURN_IF_ERROR(TabletMetaManager::get_persistent_index_meta(data_dir, tablet_id, &prev_index_meta));
    if (prev_index_meta.l2_versions_size() <= 1) {
        return Status::OK();
    }
    // 1. load current l2 vec
    std::vector<EditVersion> l2_versions;
    std::vector<std::unique_ptr<ImmutableIndex>> l2_vec;
    DCHECK(prev_index_meta.l2_versions_size() == prev_index_meta.l2_version_merged_size());
    for (int i = 0; i < prev_index_meta.l2_versions_size(); i++) {
        l2_versions.emplace_back(prev_index_meta.l2_versions(i));
        auto l2_block_path = strings::Substitute(
                "$0/index.l2.$1.$2$3", _path, prev_index_meta.l2_versions(i).major_number(),
                prev_index_meta.l2_versions(i).minor_number(), prev_index_meta.l2_version_merged(i) ? MergeSuffix : "");
        ASSIGN_OR_RETURN(auto l2_rfile, _fs->new_random_access_file(l2_block_path));
        ASSIGN_OR_RETURN(auto l2_index, ImmutableIndex::load(std::move(l2_rfile), load_bf_or_not()));
        l2_vec.emplace_back(std::move(l2_index));
    }
    // 2. merge l2 files to new l2 file
    ASSIGN_OR_RETURN(EditVersion new_l2_version, _major_compaction_impl(l2_versions, l2_vec));
    // 3. modify PersistentIndexMetaPB and reload index, protected by index lock
    {
        std::lock_guard lg(*mutex);
        if (_cancel_major_compaction) {
            return Status::OK();
        }
        PersistentIndexMetaPB index_meta;
        RETURN_IF_ERROR(TabletMetaManager::get_persistent_index_meta(data_dir, tablet_id, &index_meta));
        modify_l2_versions(l2_versions, new_l2_version, index_meta);
        RETURN_IF_ERROR(TabletMetaManager::write_persistent_index_meta(data_dir, tablet_id, index_meta));
        // reload new l2 versions
        RETURN_IF_ERROR(_reload(index_meta));
        // delete useless files
        const MutableIndexMetaPB& l0_meta = index_meta.l0_meta();
        EditVersion l0_version = l0_meta.snapshot().version();
        RETURN_IF_ERROR(_delete_expired_index_file(
                l0_version, _l1_version,
                _l2_versions.size() > 0 ? _l2_versions[0] : EditVersionWithMerge(INT64_MAX, INT64_MAX, true)));
        _calc_memory_usage();
    }
    (void)_delete_major_compaction_tmp_index_file();
    return Status::OK();
}

std::vector<int8_t> PersistentIndex::test_get_move_buckets(size_t target, const uint8_t* bucket_packs_in_page) {
    return get_move_buckets(target, kBucketPerPage, bucket_packs_in_page);
}

// This function is only used for unit test and the following code is temporary
// The following test case will be refactor after L0 support varlen keys
Status PersistentIndex::test_flush_varlen_to_immutable_index(const std::string& dir, const EditVersion& version,
                                                             const size_t num_entry, const Slice* keys,
                                                             const IndexValue* values) {
    const auto total_data_size = std::accumulate(keys, keys + num_entry, 0,
                                                 [](size_t s, const auto& e) { return s + e.size + kIndexValueSize; });
    const auto [nshard, npage_hint] = MutableIndex::estimate_nshard_and_npage(total_data_size);
    const auto nbucket =
            MutableIndex::estimate_nbucket(SliceMutableIndex::kKeySizeMagicNum, num_entry, nshard, npage_hint);
    ImmutableIndexWriter writer;
    RETURN_IF_ERROR(writer.init(dir, version, false));
    std::vector<std::vector<KVRef>> kv_ref_by_shard(nshard);
    const auto shard_bits = log2(nshard);
    for (size_t i = 0; i < nshard; i++) {
        kv_ref_by_shard[i].reserve(num_entry / nshard * 100 / 85);
    }
    std::string kv_buf;
    kv_buf.reserve(total_data_size);
    size_t kv_offset = 0;
    for (size_t i = 0; i < num_entry; i++) {
        uint64_t hash = key_index_hash(keys[i].data, keys[i].size);
        kv_buf.append(keys[i].to_string());
        put_fixed64_le(&kv_buf, values[i].get_value());
        kv_ref_by_shard[IndexHash(hash).shard(shard_bits)].emplace_back((uint8_t*)(kv_buf.data() + kv_offset), hash,
                                                                        keys[i].size + kIndexValueSize);
        kv_offset += keys[i].size + kIndexValueSize;
    }
    for (const auto& kvs : kv_ref_by_shard) {
        RETURN_IF_ERROR(writer.write_shard(SliceMutableIndex::kKeySizeMagicNum, npage_hint, nbucket, kvs));
    }
    return writer.finish();
}

double PersistentIndex::major_compaction_score(const PersistentIndexMetaPB& index_meta) {
    // return 0.0, so scheduler can skip this index, if l2 less than 2.
    const size_t l1_count = index_meta.has_l1_version() ? 1 : 0;
    const size_t l2_count = index_meta.l2_versions_size();
    if (l2_count <= 1) return 0.0;
    double l1_l2_count = (double)(l1_count + l2_count);
    // write amplification
    // = 1 + 1 + (l1 and l2 file count + config::l0_l1_merge_ratio) / (l1 and l2 file count) / 0.85
    return 2.0 + (l1_l2_count + (double)config::l0_l1_merge_ratio) / l1_l2_count / 0.85;
}

Status PersistentIndex::reset(Tablet* tablet, EditVersion version, PersistentIndexMetaPB* index_meta) {
    _cancel_major_compaction = true;

    auto tablet_schema_ptr = tablet->tablet_schema();
    vector<ColumnId> pk_columns(tablet_schema_ptr->num_key_columns());
    for (auto i = 0; i < tablet_schema_ptr->num_key_columns(); i++) {
        pk_columns[i] = (ColumnId)i;
    }
    auto pkey_schema = ChunkHelper::convert_schema(tablet_schema_ptr, pk_columns);
    size_t fix_size = PrimaryKeyEncoder::get_encoded_fixed_size(pkey_schema);

    if (_l0) {
        _l0.reset();
    }
    RETURN_IF_ERROR(create(fix_size, version));

    _l1_vec.clear();
    _usage_and_size_by_key_length.clear();
    _l1_merged_num.clear();
    _l2_versions.clear();
    _l2_vec.clear();
    _has_l1 = false;
    _dump_snapshot = true;

    std::string file_path = get_l0_index_file_name(_path, version);
    RETURN_IF_ERROR(_l0->create_index_file(file_path));
    RETURN_IF_ERROR(_reload_usage_and_size_by_key_length(0, 0, false));

    index_meta->clear_l0_meta();
    index_meta->clear_l1_version();
    index_meta->clear_l2_versions();
    index_meta->clear_l2_version_merged();
    index_meta->set_key_size(_key_size);
    index_meta->set_size(0);
    index_meta->set_format_version(PERSISTENT_INDEX_VERSION_4);
    version.to_pb(index_meta->mutable_version());
    MutableIndexMetaPB* l0_meta = index_meta->mutable_l0_meta();
    l0_meta->clear_wals();
    IndexSnapshotMetaPB* snapshot = l0_meta->mutable_snapshot();
    version.to_pb(snapshot->mutable_version());
    PagePointerPB* data = snapshot->mutable_data();
    data->set_offset(0);
    data->set_size(0);
    _calc_memory_usage();

    return Status::OK();
}

void PersistentIndex::reset_cancel_major_compaction() {
    if (!_major_compaction_running.load(std::memory_order_relaxed)) {
        _cancel_major_compaction = false;
    }
}

Status PersistentIndex::_load_by_loader(TabletLoader* loader) {
    starrocks::Schema pkey_schema = loader->generate_pkey_schema();
    DataDir* data_dir = loader->data_dir();
    TTabletId tablet_id = loader->tablet_id();
    ASSIGN_OR_RETURN(EditVersion applied_version, loader->applied_version());
    loader->setting();

    MonotonicStopWatch timer;
    timer.start();

    PersistentIndexMetaPB index_meta;
    Status status = TabletMetaManager::get_persistent_index_meta(data_dir, tablet_id, &index_meta);
    if (!status.ok() && !status.is_not_found()) {
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
        if (version == applied_version) {
            if (_need_rebuild_index(index_meta)) {
                LOG(WARNING) << "we need to rebuild persistent index, tablet id: " << tablet_id;
                status = Status::InternalError("rebuild persistent index");
            } else {
                status = load(index_meta);
            }
            if (status.ok()) {
                LOG(INFO) << "load persistent index tablet:" << tablet_id << " version:" << version.to_string()
                          << " size: " << _size << " l0_size: " << (_l0 ? _l0->size() : 0)
                          << " l0_capacity:" << (_l0 ? _l0->capacity() : 0)
                          << " #shard: " << (_has_l1 ? _l1_vec[0]->_shards.size() : 0)
                          << " l1_size:" << (_has_l1 ? _l1_vec[0]->_size : 0) << " l2_size:" << _l2_file_size()
                          << " memory: " << memory_usage() << " status: " << status.to_string()
                          << " time:" << timer.elapsed_time() / 1000000 << "ms";
                return status;
            } else {
                LOG(WARNING) << "load persistent index failed, tablet: " << tablet_id << ", status: " << status;
                if (index_meta.has_l0_meta()) {
                    EditVersion l0_version = index_meta.l0_meta().snapshot().version();
                    std::string l0_file_name =
                            strings::Substitute("index.l0.$0.$1", l0_version.major_number(), l0_version.minor_number());
                    Status st = FileSystem::Default()->delete_file(l0_file_name);
                    LOG(WARNING) << "delete error l0 index file: " << l0_file_name << ", status: " << st;
                }
                if (index_meta.has_l1_version()) {
                    EditVersion l1_version = index_meta.l1_version();
                    std::string l1_file_name =
                            strings::Substitute("index.l1.$0.$1", l1_version.major_number(), l1_version.minor_number());
                    Status st = FileSystem::Default()->delete_file(l1_file_name);
                    LOG(WARNING) << "delete error l1 index file: " << l1_file_name << ", status: " << st;
                }
                if (index_meta.l2_versions_size() > 0) {
                    DCHECK(index_meta.l2_versions_size() == index_meta.l2_version_merged_size());
                    for (int i = 0; i < index_meta.l2_versions_size(); i++) {
                        EditVersion l2_version = index_meta.l2_versions(i);
                        std::string l2_file_name = strings::Substitute(
                                "index.l2.$0.$1$2", l2_version.major_number(), l2_version.minor_number(),
                                index_meta.l2_version_merged(i) ? MergeSuffix : "");
                        Status st = FileSystem::Default()->delete_file(l2_file_name);
                        LOG(WARNING) << "delete error l2 index file: " << l2_file_name << ", status: " << st;
                    }
                }
            }
        }
    }

    size_t fix_size = PrimaryKeyEncoder::get_encoded_fixed_size(pkey_schema);

    // Init PersistentIndex
    _key_size = fix_size;
    _size = 0;
    _version = applied_version;
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
    _l1_vec.clear();
    _usage_and_size_by_key_length.clear();
    _l1_merged_num.clear();
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
                    "load persistent index from tablet failed, insert usage and size by key size failed, key_size: $0",
                    key_size);
            LOG(WARNING) << msg;
            return Status::InternalError(msg);
        }
    }
    // clear l2
    _l2_vec.clear();
    _l2_versions.clear();

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
    index_meta.set_size(0);
    index_meta.set_format_version(PERSISTENT_INDEX_VERSION_4);
    applied_version.to_pb(index_meta.mutable_version());
    MutableIndexMetaPB* l0_meta = index_meta.mutable_l0_meta();
    l0_meta->clear_wals();
    IndexSnapshotMetaPB* snapshot = l0_meta->mutable_snapshot();
    applied_version.to_pb(snapshot->mutable_version());
    PagePointerPB* data = snapshot->mutable_data();
    data->set_offset(0);
    data->set_size(0);

    std::unique_ptr<Column> pk_column;
    if (pkey_schema.num_fields() > 1) {
        RETURN_IF_ERROR(PrimaryKeyEncoder::create_column(pkey_schema, &pk_column));
    }
    RETURN_IF_ERROR(_insert_rowsets(loader, pkey_schema, std::move(pk_column)));
    RETURN_IF_ERROR(_build_commit(loader, index_meta));
    LOG(INFO) << "build persistent index finish tablet: " << loader->tablet_id() << " version:" << applied_version
              << " #rowset:" << loader->rowset_num() << " #segment:" << loader->total_segments()
              << " data_size:" << loader->total_data_size() << " size: " << _size << " l0_size: " << _l0->size()
              << " l0_capacity:" << _l0->capacity() << " #shard: " << (_has_l1 ? _l1_vec[0]->_shards.size() : 0)
              << " l1_size:" << (_has_l1 ? _l1_vec[0]->_size : 0) << " l2_size:" << _l2_file_size()
              << " memory: " << memory_usage() << " time: " << timer.elapsed_time() / 1000000 << "ms";
    return Status::OK();
}

Status PersistentIndex::pk_dump(PrimaryKeyDump* dump, PrimaryIndexMultiLevelPB* dump_pb) {
    for (const auto& l2 : _l2_vec) {
        PrimaryIndexDumpPB* level = dump_pb->add_primary_index_levels();
        level->set_filename(l2->filename());
        RETURN_IF_ERROR(l2->pk_dump(dump, level));
    }
    for (const auto& l1 : _l1_vec) {
        PrimaryIndexDumpPB* level = dump_pb->add_primary_index_levels();
        level->set_filename(l1->filename());
        RETURN_IF_ERROR(l1->pk_dump(dump, level));
    }
    if (_l0) {
        PrimaryIndexDumpPB* level = dump_pb->add_primary_index_levels();
        level->set_filename("persistent index l0");
        RETURN_IF_ERROR(_l0->pk_dump(dump, level));
    }
    return Status::OK();
}

void PersistentIndex::_calc_memory_usage() {
    size_t memory_usage = _l0 ? _l0->memory_usage() : 0;
    for (int i = 0; i < _l1_vec.size(); i++) {
        memory_usage += _l1_vec[i]->memory_usage();
    }
    for (int i = 0; i < _l2_vec.size(); i++) {
        memory_usage += _l2_vec[i]->memory_usage();
    }
    _memory_usage.store(memory_usage);
}

} // namespace starrocks
