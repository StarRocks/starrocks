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

#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/lake/filenames.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/persistent_index_memtable.h"
#include "storage/lake/persistent_index_sstable.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/update_manager.h"
#include "storage/lake/utils.h"
#include "storage/primary_key_encoder.h"
#include "storage/sstable/iterator.h"
#include "storage/sstable/merger.h"
#include "storage/sstable/options.h"
#include "storage/sstable/table_builder.h"
#include "util/trace.h"

namespace starrocks::lake {

Status KeyValueMerger::merge(const std::string& key, const std::string& value) {
    IndexValuesWithVerPB index_value_ver;
    if (!index_value_ver.ParseFromString(value)) {
        return Status::InternalError("Failed to parse index value ver");
    }
    if (index_value_ver.values_size() == 0) {
        return Status::OK();
    }

    auto version = index_value_ver.values(0).version();
    auto index_value = build_index_value(index_value_ver.values(0));
    if (_key == key) {
        if (_index_value_vers.empty()) {
            _index_value_vers.emplace_front(version, index_value);
        } else if (version > _index_value_vers.front().first) {
            std::list<std::pair<int64_t, IndexValue>> t;
            t.emplace_front(version, index_value);
            _index_value_vers.swap(t);
        }
    } else {
        flush();
        _key = key;
        _index_value_vers.emplace_front(version, index_value);
    }
    return Status::OK();
}

void KeyValueMerger::flush() {
    if (_index_value_vers.empty()) {
        return;
    }

    IndexValuesWithVerPB index_value_pb;
    for (const auto& index_value_with_ver : _index_value_vers) {
        auto* value = index_value_pb.add_values();
        value->set_version(index_value_with_ver.first);
        value->set_rssid(index_value_with_ver.second.get_rssid());
        value->set_rowid(index_value_with_ver.second.get_rowid());
    }
    _builder->Add(Slice(_key), Slice(index_value_pb.SerializeAsString()));
    _index_value_vers.clear();
}

LakePersistentIndex::LakePersistentIndex(TabletManager* tablet_mgr, int64_t tablet_id)
        : PersistentIndex(""),
          _memtable(std::make_unique<PersistentIndexMemtable>()),
          _tablet_mgr(tablet_mgr),
          _tablet_id(tablet_id) {}

LakePersistentIndex::~LakePersistentIndex() {
    _memtable->clear();
    _sstables.clear();
}

Status LakePersistentIndex::init(const PersistentIndexSstableMetaPB& sstable_meta) {
    for (auto& sstable_pb : sstable_meta.sstables()) {
        ASSIGN_OR_RETURN(auto rf,
                         fs::new_random_access_file(_tablet_mgr->sst_location(_tablet_id, sstable_pb.filename())));
        auto* block_cache = _tablet_mgr->update_mgr()->block_cache();
        if (block_cache == nullptr) {
            return Status::InternalError("Block cache is null.");
        }
        auto sstable = std::make_unique<PersistentIndexSstable>();
        RETURN_IF_ERROR(sstable->init(std::move(rf), sstable_pb, block_cache->cache()));
        _sstables.emplace_back(std::move(sstable));
    }
    return Status::OK();
}

void LakePersistentIndex::set_difference(KeyIndexSet* key_indexes, const KeyIndexSet& found_key_indexes) {
    if (!found_key_indexes.empty()) {
        KeyIndexSet t;
        std::set_difference(key_indexes->begin(), key_indexes->end(), found_key_indexes.begin(),
                            found_key_indexes.end(), std::inserter(t, t.end()));
        key_indexes->swap(t);
    }
}

bool LakePersistentIndex::is_memtable_full() const {
    const auto memtable_mem_size = _memtable->memory_usage();
    return memtable_mem_size >= config::l0_max_mem_usage / 2;
}

Status LakePersistentIndex::minor_compact() {
    TRACE_COUNTER_SCOPE_LATENCY_US("minor_compact_latency_us");
    auto filename = gen_sst_filename();
    auto location = _tablet_mgr->sst_location(_tablet_id, filename);
    ASSIGN_OR_RETURN(auto wf, fs::new_writable_file(location));
    uint64_t filesize = 0;
    RETURN_IF_ERROR(_immutable_memtable->flush(wf.get(), &filesize));
    RETURN_IF_ERROR(wf->close());

    auto sstable = std::make_unique<PersistentIndexSstable>();
    ASSIGN_OR_RETURN(auto rf, fs::new_random_access_file(location));
    PersistentIndexSstablePB sstable_pb;
    sstable_pb.set_filename(filename);
    sstable_pb.set_filesize(filesize);
    sstable_pb.set_version(_immutable_memtable->max_version());
    auto* block_cache = _tablet_mgr->update_mgr()->block_cache();
    if (block_cache == nullptr) {
        return Status::InternalError("Block cache is null.");
    }
    RETURN_IF_ERROR(sstable->init(std::move(rf), sstable_pb, block_cache->cache()));
    _sstables.emplace_back(std::move(sstable));
    TRACE_COUNTER_INCREMENT("minor_compact_times", 1);
    return Status::OK();
}

Status LakePersistentIndex::flush_memtable() {
    if (_immutable_memtable != nullptr) {
        RETURN_IF_ERROR(minor_compact());
    }
    _immutable_memtable = std::make_unique<PersistentIndexMemtable>();
    _memtable.swap(_immutable_memtable);
    return Status::OK();
}

Status LakePersistentIndex::get_from_sstables(size_t n, const Slice* keys, IndexValue* values, KeyIndexSet* key_indexes,
                                              int64_t version) const {
    if (key_indexes->empty() || _sstables.empty()) {
        return Status::OK();
    }
    for (auto iter = _sstables.rbegin(); iter != _sstables.rend(); ++iter) {
        KeyIndexSet found_key_indexes;
        RETURN_IF_ERROR((*iter)->multi_get(keys, *key_indexes, version, values, &found_key_indexes));
        set_difference(key_indexes, found_key_indexes);
        if (key_indexes->empty()) {
            break;
        }
    }
    return Status::OK();
}

Status LakePersistentIndex::get_from_immutable_memtable(const Slice* keys, IndexValue* values,
                                                        const KeyIndexSet& key_indexes, KeyIndexSet* found_key_indexes,
                                                        int64_t version) const {
    if (_immutable_memtable == nullptr || key_indexes.empty()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_immutable_memtable->get(keys, values, key_indexes, found_key_indexes, version));
    return Status::OK();
}

Status LakePersistentIndex::get(size_t n, const Slice* keys, IndexValue* values) {
    KeyIndexSet not_founds;
    // Assuming we always want the latest value now
    RETURN_IF_ERROR(_memtable->get(n, keys, values, &not_founds, -1));
    KeyIndexSet& key_indexes = not_founds;
    KeyIndexSet found_key_indexes;
    RETURN_IF_ERROR(get_from_immutable_memtable(keys, values, key_indexes, &found_key_indexes, -1));
    set_difference(&key_indexes, found_key_indexes);
    RETURN_IF_ERROR(get_from_sstables(n, keys, values, &key_indexes, -1));
    return Status::OK();
}

Status LakePersistentIndex::upsert(size_t n, const Slice* keys, const IndexValue* values, IndexValue* old_values,
                                   IOStat* stat) {
    std::set<KeyIndex> not_founds;
    size_t num_found;
    RETURN_IF_ERROR(_memtable->upsert(n, keys, values, old_values, &not_founds, &num_found, _version.major_number()));
    KeyIndexSet& key_indexes = not_founds;
    KeyIndexSet found_key_indexes;
    RETURN_IF_ERROR(get_from_immutable_memtable(keys, old_values, key_indexes, &found_key_indexes, -1));
    set_difference(&key_indexes, found_key_indexes);
    RETURN_IF_ERROR(get_from_sstables(n, keys, old_values, &key_indexes, -1));
    if (is_memtable_full()) {
        return flush_memtable();
    }
    return Status::OK();
}

Status LakePersistentIndex::insert(size_t n, const Slice* keys, const IndexValue* values, int64_t version) {
    TRACE_COUNTER_SCOPE_LATENCY_US("lake_persistent_index_insert_us");
    RETURN_IF_ERROR(_memtable->insert(n, keys, values, version));
    if (is_memtable_full()) {
        RETURN_IF_ERROR(flush_memtable());
    }
    // TODO: check whether keys exist in immutable_memtable and ssts
    return Status::OK();
}

Status LakePersistentIndex::erase(size_t n, const Slice* keys, IndexValue* old_values) {
    KeyIndexSet not_founds;
    size_t num_found;
    RETURN_IF_ERROR(_memtable->erase(n, keys, old_values, &not_founds, &num_found, _version.major_number()));
    KeyIndexSet& key_indexes = not_founds;
    KeyIndexSet found_key_indexes;
    RETURN_IF_ERROR(get_from_immutable_memtable(keys, old_values, key_indexes, &found_key_indexes, -1));
    set_difference(&key_indexes, found_key_indexes);
    RETURN_IF_ERROR(get_from_sstables(n, keys, old_values, &key_indexes, -1));
    if (is_memtable_full()) {
        return flush_memtable();
    }
    return Status::OK();
}

Status LakePersistentIndex::try_replace(size_t n, const Slice* keys, const IndexValue* values,
                                        const uint32_t max_src_rssid, std::vector<uint32_t>* failed) {
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
    RETURN_IF_ERROR(_memtable->replace(keys, values, replace_idxes, _version.major_number()));
    if (is_memtable_full()) {
        return flush_memtable();
    }
    return Status::OK();
}

Status LakePersistentIndex::replace(size_t n, const Slice* keys, const IndexValue* values,
                                    const std::vector<uint32_t>& replace_indexes) {
    std::vector<size_t> tmp_replace_idxes(replace_indexes.begin(), replace_indexes.end());
    RETURN_IF_ERROR(_memtable->replace(keys, values, tmp_replace_idxes, _version.major_number()));
    if (is_memtable_full()) {
        return flush_memtable();
    }
    return Status::OK();
}

Status LakePersistentIndex::prepare_merging_iterator(
        const TabletMetadata& metadata, TxnLogPB* txn_log,
        std::vector<std::shared_ptr<PersistentIndexSstable>>* merging_sstables,
        std::unique_ptr<sstable::Iterator>* merging_iter_ptr) {
    sstable::ReadOptions read_options;
    // No need to cache input sst's blocks.
    read_options.fill_cache = false;
    std::vector<sstable::Iterator*> iters;
    DeferOp free_iters([&] {
        for (sstable::Iterator* iter : iters) {
            delete iter;
        }
    });

    auto max_compaction_bytes = config::lake_pk_index_sst_max_compaction_bytes;
    iters.reserve(metadata.sstable_meta().sstables().size());
    size_t total_filesize = 0;
    std::stringstream ss_debug;
    for (const auto& sstable_pb : metadata.sstable_meta().sstables()) {
        // build sstable from meta, instead of reuse `_sstables`, to keep it thread safe
        ASSIGN_OR_RETURN(auto rf,
                         fs::new_random_access_file(_tablet_mgr->sst_location(_tablet_id, sstable_pb.filename())));
        auto merging_sstable = std::make_shared<PersistentIndexSstable>();
        RETURN_IF_ERROR(merging_sstable->init(std::move(rf), sstable_pb, nullptr, false /** no filter **/));
        merging_sstables->push_back(merging_sstable);
        sstable::Iterator* iter = merging_sstable->new_iterator(read_options);
        iters.emplace_back(iter);
        total_filesize += sstable_pb.filesize();
        // add input sstable.
        txn_log->mutable_op_compaction()->add_input_sstables()->CopyFrom(merging_sstable->sstable_pb());
        ss_debug << sstable_pb.filename() << " | ";
        if (total_filesize >= max_compaction_bytes &&
            merging_sstables->size() >= config::lake_pk_index_sst_min_compaction_versions) {
            break;
        }
    }
    sstable::Options options;
    (*merging_iter_ptr).reset(sstable::NewMergingIterator(options.comparator, &iters[0], iters.size()));
    (*merging_iter_ptr)->SeekToFirst();
    iters.clear(); // Clear the vector without deleting iterators since they are now managed by merge_iter_ptr.
    VLOG(2) << "prepare sst for merge : " << ss_debug.str();
    return Status::OK();
}

Status LakePersistentIndex::merge_sstables(std::unique_ptr<sstable::Iterator> iter_ptr,
                                           sstable::TableBuilder* builder) {
    auto merger = std::make_unique<KeyValueMerger>(iter_ptr->key().to_string(), builder);
    while (iter_ptr->Valid()) {
        RETURN_IF_ERROR(merger->merge(iter_ptr->key().to_string(), iter_ptr->value().to_string()));
        iter_ptr->Next();
    }
    RETURN_IF_ERROR(iter_ptr->status());
    merger->finish();
    return builder->Finish();
}

Status LakePersistentIndex::major_compact(const TabletMetadata& metadata, int64_t min_retain_version,
                                          TxnLogPB* txn_log) {
    if (metadata.sstable_meta().sstables_size() < config::lake_pk_index_sst_min_compaction_versions) {
        return Status::OK();
    }

    std::vector<std::shared_ptr<PersistentIndexSstable>> sstable_vec;
    std::unique_ptr<sstable::Iterator> merging_iter_ptr;
    // build merge iterator
    RETURN_IF_ERROR(prepare_merging_iterator(metadata, txn_log, &sstable_vec, &merging_iter_ptr));
    if (!merging_iter_ptr->Valid()) {
        return merging_iter_ptr->status();
    }

    auto filename = gen_sst_filename();
    auto location = _tablet_mgr->sst_location(_tablet_id, filename);
    ASSIGN_OR_RETURN(auto wf, fs::new_writable_file(location));
    sstable::Options options;
    std::unique_ptr<sstable::FilterPolicy> filter_policy;
    filter_policy.reset(const_cast<sstable::FilterPolicy*>(sstable::NewBloomFilterPolicy(10)));
    options.filter_policy = filter_policy.get();
    sstable::TableBuilder builder(options, wf.get());
    RETURN_IF_ERROR(merge_sstables(std::move(merging_iter_ptr), &builder));
    RETURN_IF_ERROR(wf->close());

    // record output sstable pb
    txn_log->mutable_op_compaction()->mutable_output_sstable()->set_filename(filename);
    txn_log->mutable_op_compaction()->mutable_output_sstable()->set_filesize(builder.FileSize());
    return Status::OK();
}

Status LakePersistentIndex::apply_opcompaction(const TxnLogPB_OpCompaction& op_compaction) {
    if (op_compaction.input_sstables().empty()) {
        return Status::OK();
    }

    PersistentIndexSstablePB sstable_pb;
    sstable_pb.CopyFrom(op_compaction.output_sstable());
    sstable_pb.set_version(op_compaction.input_sstables(op_compaction.input_sstables().size() - 1).version());
    auto sstable = std::make_unique<PersistentIndexSstable>();
    ASSIGN_OR_RETURN(auto rf, fs::new_random_access_file(_tablet_mgr->sst_location(_tablet_id, sstable_pb.filename())));
    auto* block_cache = _tablet_mgr->update_mgr()->block_cache();
    if (block_cache == nullptr) {
        return Status::InternalError("Block cache is null.");
    }
    RETURN_IF_ERROR(sstable->init(std::move(rf), sstable_pb, block_cache->cache()));

    std::unordered_set<std::string> filenames;
    for (const auto& input_sstable : op_compaction.input_sstables()) {
        filenames.insert(input_sstable.filename());
    }
    _sstables.erase(std::remove_if(_sstables.begin(), _sstables.end(),
                                   [&](const std::unique_ptr<PersistentIndexSstable>& sstable) {
                                       return filenames.contains(sstable->sstable_pb().filename());
                                   }),
                    _sstables.end());
    _sstables.insert(_sstables.begin(), std::move(sstable));
    return Status::OK();
}

Status LakePersistentIndex::commit(MetaFileBuilder* builder) {
    PersistentIndexSstableMetaPB sstable_meta;
    int64_t last_version = 0;
    for (auto& sstable : _sstables) {
        int64_t sstable_version = sstable->sstable_pb().version();
        if (last_version > sstable_version) {
            return Status::InternalError("Versions of sstables are not ordered");
        }
        last_version = sstable_version;
        auto* sstable_pb = sstable_meta.add_sstables();
        sstable_pb->CopyFrom(sstable->sstable_pb());
    }
    builder->finalize_sstable_meta(sstable_meta);
    return Status::OK();
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

    // Init PersistentIndex
    _key_size = PrimaryKeyEncoder::get_encoded_fixed_size(pkey_schema);

    const auto& sstables = metadata->sstable_meta().sstables();
    int64_t max_sstable_version = sstables.empty() ? 0 : sstables.rbegin()->version();
    if (max_sstable_version > base_version) {
        return Status::OK();
    }
    TRACE_COUNTER_INCREMENT("max_sstable_version", max_sstable_version);
    TRACE_COUNTER_INCREMENT("new_version", metadata->version());

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
    // Rowset whose version is between max_sstable_version and base_version should be recovered.
    for (auto& rowset : rowsets) {
        TRACE_COUNTER_INCREMENT("total_rowsets", 1);
        TRACE_COUNTER_INCREMENT("total_segments", rowset->num_segments());
        TRACE_COUNTER_INCREMENT("total_datasize_bytes", rowset->data_size());
        TRACE_COUNTER_INCREMENT("total_num_rows", rowset->num_rows());
        // If it is upgraded from old version of sr, the rowset version will be not set.
        // The generated rowset version will be treated as base_version.
        int64_t rowset_version = rowset->version() != 0 ? rowset->version() : base_version;
        // The data whose version is max_sstable_version in memtable may be not flushed to sstable.
        // So rowset whose version is max_sstable_version should also be recovered.
        if (rowset_version < max_sstable_version) {
            continue;
        }
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
                                               values.data(), rowset_version));
                    } else {
                        std::vector<Slice> keys;
                        keys.reserve(pkc->size());
                        const auto* fkeys = pkc->continuous_data();
                        for (size_t i = 0; i < pkc->size(); ++i) {
                            keys.emplace_back(fkeys, _key_size);
                            fkeys += _key_size;
                        }
                        RETURN_IF_ERROR(insert(pkc->size(), reinterpret_cast<const Slice*>(keys.data()), values.data(),
                                               rowset_version));
                    }
                }
            }
            itr->close();
        }
        TRACE_COUNTER_INCREMENT("loaded_rowsets", 1);
        TRACE_COUNTER_INCREMENT("loaded_segments", rowset->num_segments());
        TRACE_COUNTER_INCREMENT("loaded_datasize_bytes", rowset->data_size());
        TRACE_COUNTER_INCREMENT("loaded_num_rows", rowset->num_rows());
    }
    return Status::OK();
}

size_t LakePersistentIndex::memory_usage() const {
    size_t mem_usage = 0;
    if (_memtable != nullptr) {
        mem_usage += _memtable->memory_usage();
    }
    if (_immutable_memtable != nullptr) {
        mem_usage += _immutable_memtable->memory_usage();
    }
    return mem_usage;
}

} // namespace starrocks::lake
