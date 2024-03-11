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

#include "storage/lake/lake_persistent_index.h"

#include <butil/time.h>
#include "fs/fs_util.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/chunk_helper.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/persistent_index_memtable.h"
#include "storage/lake/rowset.h"
#include "storage/lake/sstable/lake_persistent_index_sst.h"
#include "storage/primary_key_encoder.h"
#include "util/trace.h"

namespace starrocks::lake {

LakePersistentIndex::LakePersistentIndex(std::string path) : PersistentIndex(std::move(path)) {
    _memtable = std::make_unique<PersistentIndexMemtable>();
}

LakePersistentIndex::LakePersistentIndex(TabletManager* tablet_mgr, int64_t tablet_id,
                                         PersistentIndexSstableMetaPB sstable_meta)
        : PersistentIndex(""),
          _sstable_meta(std::make_unique<PersistentIndexSstableMetaPB>(sstable_meta)),
          _cache(new_lru_cache(config::lake_pk_index_sst_cache_limit)),
          _tablet_mgr(tablet_mgr),
          _tablet_id(tablet_id) {
    _sstable = std::make_unique<PersistentIndexSstablePB>();
    _memtable = std::make_unique<PersistentIndexMemtable>(tablet_mgr, tablet_id);
}

LakePersistentIndex::~LakePersistentIndex() {
    _memtable->clear();
}

Status LakePersistentIndex::get_from_sstables(size_t n, const Slice* keys, IndexValue* values,
                                              KeyIndexesInfo* key_indexes_info, int64_t version) {
    TRACE_COUNTER_SCOPE_LATENCY_US("get_from_sstables_us");
    if (key_indexes_info->size() == 0) {
        return Status::OK();
    }
    auto sstable_size = _sstable_meta->sstables_size();
    if (sstable_size == 0) {
        return Status::OK();
    }
    std::sort(key_indexes_info->key_index_infos.begin(), key_indexes_info->key_index_infos.end());
    TRACE_COUNTER_INCREMENT("sstable_size", sstable_size);
    for (size_t i = sstable_size; i > 0; --i) {
        if (key_indexes_info->size() == 0) {
            break;
        }
        auto sstable_pb_size = _sstable_meta->sstables(i - 1).sstables_size();
        if (sstable_pb_size == 0) {
            continue;
        }
        for (size_t j = sstable_pb_size; j > 0; --j) {
            if (key_indexes_info->size() == 0) {
                break;
            }
            auto start_ts = butil::gettimeofday_us();
            RandomAccessFileOptions opts{.skip_fill_local_cache = true};
            ASSIGN_OR_RETURN(
                    auto rf,
                    fs::new_random_access_file(
                            opts, _tablet_mgr->sst_location(
                                          _tablet_id, _sstable_meta->sstables(i - 1).sstables(j - 1).filename())));
            auto end_ts = butil::gettimeofday_us();
            TRACE_COUNTER_INCREMENT("random_access_file", end_ts - start_ts);
            auto pindex_sst = std::make_unique<LakePersistentIndexSstable>();
            start_ts = butil::gettimeofday_us();
            RETURN_IF_ERROR(
                    pindex_sst->init(rf.get(), _sstable_meta->sstables(i - 1).sstables(j - 1).filesz(), _cache.get()));
            end_ts = butil::gettimeofday_us();
            TRACE_COUNTER_INCREMENT("sst_init", end_ts - start_ts);
            KeyIndexesInfo found_key_indexes_info;
            RETURN_IF_ERROR(pindex_sst->get(n, keys, values, key_indexes_info, &found_key_indexes_info, version));
            start_ts = butil::gettimeofday_us();
            if (found_key_indexes_info.size() != 0) {
                std::sort(found_key_indexes_info.key_index_infos.begin(), found_key_indexes_info.key_index_infos.end());
                // modify key_indexess_info
                key_indexes_info->set_difference(found_key_indexes_info);
            }
            end_ts = butil::gettimeofday_us();
            TRACE_COUNTER_INCREMENT("set_difference", end_ts - start_ts);
        }
    }
    return Status::OK();
}

Status LakePersistentIndex::get_from_immutable_memtable(size_t n, const Slice* keys, IndexValue* values,
                                                        KeyIndexesInfo* key_indexes_info, int64_t version) {
    TRACE_COUNTER_SCOPE_LATENCY_US("get_from_immutable_memtable_us");
    if (_immutable_memtable == nullptr || key_indexes_info->size() == 0) {
        return Status::OK();
    }
    std::sort(key_indexes_info->key_index_infos.begin(), key_indexes_info->key_index_infos.end());
    KeyIndexesInfo found_key_indexes_info;
    RETURN_IF_ERROR(_immutable_memtable->get(n, keys, values, key_indexes_info, &found_key_indexes_info, version));
    auto start_ts = butil::gettimeofday_us();
    if (found_key_indexes_info.size() != 0) {
        std::sort(found_key_indexes_info.key_index_infos.begin(), found_key_indexes_info.key_index_infos.end());
        // modify key_indexess_info
        key_indexes_info->set_difference(found_key_indexes_info);
    }
    auto end_ts = butil::gettimeofday_us();
    TRACE_COUNTER_INCREMENT("set_difference", end_ts - start_ts);
    return Status::OK();
}

Status LakePersistentIndex::get(size_t n, const Slice* keys, IndexValue* values, int64_t version) {
    TRACE_COUNTER_SCOPE_LATENCY_US("get_us");
    KeyIndexesInfo not_founds;
    size_t num_found;
    RETURN_IF_ERROR(_memtable->get(n, keys, values, &not_founds, &num_found, version));
    if (num_found == n) {
        return Status::OK();
    }
    RETURN_IF_ERROR(get_from_immutable_memtable(n, keys, values, &not_founds, version));
    RETURN_IF_ERROR(get_from_sstables(n, keys, values, &not_founds, version));
    return Status::OK();
}

Status LakePersistentIndex::upsert(size_t n, const Slice* keys, const IndexValue* values, IndexValue* old_values,
                                   IOStat* stat, int64_t version) {
    TRACE_COUNTER_SCOPE_LATENCY_US("upsert_us");
    KeyIndexesInfo not_founds;
    size_t num_found;
    RETURN_IF_ERROR(_memtable->upsert(n, keys, values, old_values, &not_founds, &num_found, version));
    if (num_found == n) {
        return Status::OK();
    }
    RETURN_IF_ERROR(get_from_immutable_memtable(n, keys, old_values, &not_founds, -1));
    RETURN_IF_ERROR(get_from_sstables(n, keys, old_values, &not_founds, -1));
    if (is_memtable_full()) {
        RETURN_IF_ERROR(minor_compact());
        flush_to_immutable_memtable();
    }
    return Status::OK();
}

Status LakePersistentIndex::insert(size_t n, const Slice* keys, const IndexValue* values, bool check_l1,
                                   int64_t version) {
    RETURN_IF_ERROR(_memtable->insert(n, keys, values, version));
    if (is_memtable_full()) {
        RETURN_IF_ERROR(minor_compact());
        flush_to_immutable_memtable();
    }
    return Status::OK();
}

Status LakePersistentIndex::erase(size_t n, const Slice* keys, IndexValue* old_values, int64_t version) {
    TRACE_COUNTER_SCOPE_LATENCY_US("erase_us");
    KeyIndexesInfo not_founds;
    size_t num_found;
    RETURN_IF_ERROR(_memtable->erase(n, keys, old_values, &not_founds, &num_found, version));
    if (num_found == n) {
        return Status::OK();
    }
    RETURN_IF_ERROR(get_from_immutable_memtable(n, keys, old_values, &not_founds, -1));
    RETURN_IF_ERROR(get_from_sstables(n, keys, old_values, &not_founds, -1));
    return Status::OK();
}

Status LakePersistentIndex::try_replace(size_t n, const Slice* keys, const IndexValue* values,
                                        const uint32_t max_src_rssid, std::vector<uint32_t>* failed, int64_t version) {
    TRACE_COUNTER_SCOPE_LATENCY_US("try_replace_us");
    std::vector<IndexValue> found_values;
    found_values.resize(n);
    RETURN_IF_ERROR(get(n, keys, found_values.data()));
    std::vector<size_t> replace_idxes;
    for (size_t i = 0; i < n; ++i) {
        if (found_values[i].get_value() != NullIndexValue &&
            ((uint32_t)(found_values[i].get_value() >> 32)) <= max_src_rssid) {
            replace_idxes.emplace_back(i);
        } else {
            failed->emplace_back(values[i].get_value() & 0xFFFFFFFF);
        }
    }
    RETURN_IF_ERROR(_memtable->replace(keys, values, replace_idxes, version));
    return Status::OK();
}

void LakePersistentIndex::flush_to_immutable_memtable() {
    TRACE_COUNTER_SCOPE_LATENCY_US("flush_to_immutable_memtable_us");
    _immutable_memtable = std::move(_memtable);
    _memtable = std::make_unique<PersistentIndexMemtable>(_tablet_mgr, _tablet_id);
}

Status LakePersistentIndex::minor_compact() {
    TRACE_COUNTER_SCOPE_LATENCY_US("minor_compact_us");
    if (_immutable_memtable != nullptr) {
        SstablePB sstable;
        RETURN_IF_ERROR(_immutable_memtable->flush(_txn_id, &sstable));
        _sstable->mutable_sstables()->Add(std::move(sstable));
        _immutable_memtable = nullptr;
    }
    return Status::OK();
}

Status LakePersistentIndex::major_compact(int64_t min_retain_version) {
    return Status::OK();
}

void LakePersistentIndex::commit(MetaFileBuilder* builder) {
    if (_sstable->sstables_size() > 0) {
        builder->add_sstable(*_sstable);
        _sstable.reset(new PersistentIndexSstablePB());
    }
    _sstable_meta.reset(new PersistentIndexSstableMetaPB(builder->get_sstable_meta()));
}

bool LakePersistentIndex::is_memtable_full() {
    const auto memtable_mem_size = _memtable->memory_usage();
    return memtable_mem_size >= config::l0_max_mem_usage;
}

Status LakePersistentIndex::load_from_lake_tablet(TabletManager* tablet_mgr, const TabletMetadataPtr& metadata,
                                                  int64_t base_version, const MetaFileBuilder* builder) {
    // 1. create and set key column schema
    std::shared_ptr<TabletSchema> tablet_schema = std::make_shared<TabletSchema>(metadata->schema());
    vector<ColumnId> pk_columns(tablet_schema->num_key_columns());
    for (auto i = 0; i < tablet_schema->num_key_columns(); i++) {
        pk_columns[i] = (ColumnId)i;
    }
    auto pkey_schema = ChunkHelper::convert_schema(tablet_schema, pk_columns);
    size_t fix_size = PrimaryKeyEncoder::get_encoded_fixed_size(pkey_schema);

    // Init PersistentIndex
    _key_size = fix_size;

    auto sstables = metadata->pindex_sstable_meta().sstables();
    int64_t max_sstable_version = 0;
    int sstables_size = sstables.size();
    if (sstables_size > 0) {
        max_sstable_version = sstables[sstables_size - 1].version();
    }
    LOG(INFO) << "max sstable version " << max_sstable_version;
    if (max_sstable_version > base_version) {
        return Status::OK();
    }

    OlapReaderStatistics stats;
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
    auto rowsets = Rowset::get_rowsets(tablet_mgr, metadata);
    for (auto& rowset : rowsets) {
        int64_t rowset_version = rowset->version() != 0 ? rowset->version() : base_version;
        if (rowset->version() >= max_sstable_version && rowset->version() <= base_version) {
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
                            RETURN_IF_ERROR(insert(pkc->size(), reinterpret_cast<const Slice*>(pkc->raw_data()),
                                                   values.data(), false, rowset_version));
                        } else {
                            std::vector<Slice> keys;
                            keys.reserve(pkc->size());
                            const auto* fkeys = pkc->continuous_data();
                            for (size_t i = 0; i < pkc->size(); ++i) {
                                keys.emplace_back(fkeys, _key_size);
                                fkeys += _key_size;
                            }
                            RETURN_IF_ERROR(insert(pkc->size(), reinterpret_cast<const Slice*>(keys.data()),
                                                   values.data(), false, rowset_version));
                        }
                    }
                }
                itr->close();
            }
        }
    }
    return Status::OK();
}

} // namespace starrocks::lake
