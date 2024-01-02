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

#include "gen_cpp/lake_types.pb.h"
#include "storage/chunk_helper.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/persistent_index_memtable.h"
#include "storage/lake/rowset.h"
#include "storage/primary_key_encoder.h"

namespace starrocks::lake {

LakePersistentIndex::LakePersistentIndex(std::string path) : PersistentIndex(std::move(path)) {
    _memtable = std::make_unique<PersistentIndexMemtable>();
}

LakePersistentIndex::LakePersistentIndex(TabletManager* tablet_mgr, int64_t tablet_id,
                                         PersistentIndexSstableMetaPB sstable_meta)
        : PersistentIndex(""),
          _sstable_meta(std::make_unique<PersistentIndexSstableMetaPB>(sstable_meta)),
          _tablet_mgr(tablet_mgr),
          _tablet_id(tablet_id) {
    _sstable = _sstable_meta->add_sstables();
    _memtable = std::make_unique<PersistentIndexMemtable>(tablet_mgr, tablet_id);
}

LakePersistentIndex::~LakePersistentIndex() {
    _memtable->clear();
}

Status LakePersistentIndex::get(size_t n, const Slice* keys, IndexValue* values, int64_t version) {
    KeyIndexesInfo not_founds;
    size_t num_found;
    return _memtable->get(n, keys, values, &not_founds, &num_found, version);
}

Status LakePersistentIndex::upsert(size_t n, const Slice* keys, const IndexValue* values, IndexValue* old_values,
                                   IOStat* stat, int64_t version) {
    KeyIndexesInfo not_founds;
    size_t num_found;
    RETURN_IF_ERROR(_memtable->upsert(n, keys, values, old_values, &not_founds, &num_found, version));
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
    KeyIndexesInfo not_founds;
    size_t num_found;
    return _memtable->erase(n, keys, old_values, &not_founds, &num_found, version);
}

Status LakePersistentIndex::try_replace(size_t n, const Slice* keys, const IndexValue* values,
                                        const uint32_t max_src_rssid, std::vector<uint32_t>* failed, int64_t version) {
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
    _immutable_memtable = std::move(_memtable);
    _memtable = std::make_unique<PersistentIndexMemtable>(_tablet_mgr, _tablet_id);
}

Status LakePersistentIndex::minor_compact() {
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
        auto sstable_meta = std::make_shared<PersistentIndexSstableMetaPB>(*_sstable_meta);
        builder->set_sstable_meta(std::move(sstable_meta));
        _sstable = _sstable_meta->add_sstables();
    }
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
    if (max_sstable_version >= base_version) {
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
        if (rowset->version() > max_sstable_version && rowset->version() <= base_version) {
            auto res = rowset->get_each_segment_iterator_with_delvec(pkey_schema, rowset_version, builder, &stats);
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
                            LOG(INFO) << pkc->debug_string();
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
