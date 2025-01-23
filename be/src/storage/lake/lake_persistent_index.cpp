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
#include "fs/key_cache.h"
#include "serde/column_array_serde.h"
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

Status KeyValueMerger::merge(const std::string& key, const std::string& value, uint64_t max_rss_rowid) {
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
            _max_rss_rowid = max_rss_rowid;
            _index_value_vers.emplace_front(version, index_value);
        } else if ((version > _index_value_vers.front().first) ||
                   (version == _index_value_vers.front().first && max_rss_rowid > _max_rss_rowid) ||
                   (version == _index_value_vers.front().first && max_rss_rowid == _max_rss_rowid &&
                    index_value.get_value() == NullIndexValue)) {
            // NOTICE: we need both version and max_rss_rowid here to decide the order of keys.
            // Consider the following 3 scenarios:
            // 1. Same keys are from two different Rowsets, and we can decide their order by version recorded
            //    in Rowset.
            //   | ------- ver1 --------- | + | -------- ver2 ----------|
            //   | k1 k2 k3(1)            |   | k3(2) k4                |
            //
            //   =
            //   | ------- ver2 --------- |
            //   | k1 k2 k3(2) k4         |
            //   k3 in ver2 will replace k3 in ver1, because it has a larger version.
            //
            // 2. Same keys are from same Rowset, and they have same version. Now we use `max_rss_rowid` in sst to
            //    decide their order.
            //   | ------- ver1 --------- | + | -------- ver1 ----------|
            //   | k1 k2 k3(1)            |   | k3(2) k4                |
            //   | max_rss_rowid = 2      |   | max_rss_rowid = 4       |
            //   =
            //   | ------- ver1 --------- |
            //   | k1 k2 k3(2) k4         |
            //   | max_rss_rowid = 4      |
            //
            //   k3 with larger max_rss_rowid will replace previous one, because max_rss_rowid is incremental,
            //   larger max_rss_rowid means it was generated later.
            //
            // 3. Same keys are from same Rowset, and they have same version. And they also have same `max_rss_rowid`
            //    because one of them is delete flag.
            //   | ------- ver1 --------- | + | -------- ver1 ----------|
            //   | k1 k2 k3 k4(del)       |   | k3(del)      k4(del)    |
            //   | max_rss_rowid = MAX    |   | max_rss_rowid = MAX     |
            //   =
            //   | ------- ver1 --------- |
            //   | k1 k2                  |
            //   | max_rss_rowid = MAX    |
            //
            //   Because we use UINT32_TMAX as delete flag key's rowid, so two sst will have same
            //   max_rss_rowid, when the second one is only contains delete flag keys.
            //   k3 with delete flag will replace previous one.
            _max_rss_rowid = max_rss_rowid;
            std::list<std::pair<int64_t, IndexValue>> t;
            t.emplace_front(version, index_value);
            _index_value_vers.swap(t);
        }
    } else {
        flush();
        _key = key;
        _max_rss_rowid = max_rss_rowid;
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
        if (_merge_base_level && index_value_with_ver.second == IndexValue(NullIndexValue)) {
            // deleted
            continue;
        }
        auto* value = index_value_pb.add_values();
        value->set_version(index_value_with_ver.first);
        value->set_rssid(index_value_with_ver.second.get_rssid());
        value->set_rowid(index_value_with_ver.second.get_rowid());
    }
    if (index_value_pb.values_size() > 0) {
        _builder->Add(Slice(_key), Slice(index_value_pb.SerializeAsString()));
    }
    _index_value_vers.clear();
}

LakePersistentIndex::LakePersistentIndex(TabletManager* tablet_mgr, int64_t tablet_id)
        : PersistentIndex(""), _tablet_mgr(tablet_mgr), _tablet_id(tablet_id) {}

LakePersistentIndex::~LakePersistentIndex() {
    _memtable->clear();
    _sstables.clear();
}

Status LakePersistentIndex::init(const PersistentIndexSstableMetaPB& sstable_meta) {
    uint64_t max_rss_rowid = 0;
    for (auto& sstable_pb : sstable_meta.sstables()) {
        RandomAccessFileOptions opts;
        if (!sstable_pb.encryption_meta().empty()) {
            ASSIGN_OR_RETURN(auto info, KeyCache::instance().unwrap_encryption_meta(sstable_pb.encryption_meta()));
            opts.encryption_info = std::move(info);
        }
        ASSIGN_OR_RETURN(auto rf, fs::new_random_access_file(
                                          opts, _tablet_mgr->sst_location(_tablet_id, sstable_pb.filename())));
        auto* block_cache = _tablet_mgr->update_mgr()->block_cache();
        if (block_cache == nullptr) {
            return Status::InternalError("Block cache is null.");
        }
        auto sstable = std::make_unique<PersistentIndexSstable>();
        RETURN_IF_ERROR(sstable->init(std::move(rf), sstable_pb, block_cache->cache()));
        _sstables.emplace_back(std::move(sstable));
        max_rss_rowid = std::max(max_rss_rowid, sstable_pb.max_rss_rowid());
    }
    // create memtable with previous rebuild `max_rss_rowid`,
    // to make sure we can generate sst order by `max_rss_rowid`.
    _memtable = std::make_unique<PersistentIndexMemtable>(max_rss_rowid);
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
    const bool mem_size_exceed = memtable_mem_size >= config::l0_max_mem_usage;
    // When update memory is urgent, using a lower limit (`l0_min_mem_usage`).
    const bool mem_tracker_exceed =
            _tablet_mgr->update_mgr()->mem_tracker()->limit_exceeded_by_ratio(config::memory_urgent_level) &&
            memtable_mem_size >= config::l0_min_mem_usage;
    return mem_size_exceed || mem_tracker_exceed;
}

bool LakePersistentIndex::too_many_rebuild_files() const {
    return _need_rebuild_file_cnt >= config::cloud_native_pk_index_rebuild_files_threshold;
}

Status LakePersistentIndex::minor_compact() {
    TRACE_COUNTER_SCOPE_LATENCY_US("minor_compact_latency_us");
    auto filename = gen_sst_filename();
    auto location = _tablet_mgr->sst_location(_tablet_id, filename);
    WritableFileOptions wopts;
    std::string encryption_meta;
    if (config::enable_transparent_data_encryption) {
        ASSIGN_OR_RETURN(auto pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
        wopts.encryption_info = pair.info;
        encryption_meta.swap(pair.encryption_meta);
    }
    ASSIGN_OR_RETURN(auto wf, fs::new_writable_file(wopts, location));
    uint64_t filesize = 0;
    RETURN_IF_ERROR(_memtable->flush(wf.get(), &filesize));
    RETURN_IF_ERROR(wf->close());

    auto sstable = std::make_unique<PersistentIndexSstable>();
    RandomAccessFileOptions opts;
    if (!encryption_meta.empty()) {
        opts.encryption_info = wopts.encryption_info;
    }
    ASSIGN_OR_RETURN(auto rf, fs::new_random_access_file(opts, location));
    PersistentIndexSstablePB sstable_pb;
    sstable_pb.set_filename(filename);
    sstable_pb.set_filesize(filesize);
    sstable_pb.set_max_rss_rowid(_memtable->max_rss_rowid());
    sstable_pb.set_encryption_meta(encryption_meta);
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
    RETURN_IF_ERROR(minor_compact());
    auto max_rss_rowid = _memtable->max_rss_rowid();
    _memtable.reset();
    _memtable = std::make_unique<PersistentIndexMemtable>(max_rss_rowid);
    // Reset rebuild file count, avoid useless flush.
    _need_rebuild_file_cnt = 0;
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

Status LakePersistentIndex::get(size_t n, const Slice* keys, IndexValue* values) {
    KeyIndexSet not_founds;
    // Assuming we always want the latest value now
    RETURN_IF_ERROR(_memtable->get(n, keys, values, &not_founds, -1));
    KeyIndexSet& key_indexes = not_founds;
    RETURN_IF_ERROR(get_from_sstables(n, keys, values, &key_indexes, -1));
    return Status::OK();
}

Status LakePersistentIndex::upsert(size_t n, const Slice* keys, const IndexValue* values, IndexValue* old_values,
                                   IOStat* stat) {
    std::set<KeyIndex> not_founds;
    size_t num_found;
    RETURN_IF_ERROR(_memtable->upsert(n, keys, values, old_values, &not_founds, &num_found, _version.major_number()));
    KeyIndexSet& key_indexes = not_founds;
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

// Used to rebuild delete operation.
Status LakePersistentIndex::replay_erase(size_t n, const Slice* keys, const std::vector<bool>& filter, int64_t version,
                                         uint32_t rowset_id) {
    TRACE_COUNTER_SCOPE_LATENCY_US("lake_persistent_index_insert_delete_us");
    RETURN_IF_ERROR(_memtable->erase_with_filter(n, keys, filter, version, rowset_id));
    if (is_memtable_full()) {
        RETURN_IF_ERROR(flush_memtable());
    }
    return Status::OK();
}

Status LakePersistentIndex::erase(size_t n, const Slice* keys, IndexValue* old_values, uint32_t rowset_id) {
    KeyIndexSet not_founds;
    size_t num_found;
    RETURN_IF_ERROR(_memtable->erase(n, keys, old_values, &not_founds, &num_found, _version.major_number(), rowset_id));
    KeyIndexSet& key_indexes = not_founds;
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

void LakePersistentIndex::pick_sstables_for_merge(const PersistentIndexSstableMetaPB& sstable_meta,
                                                  std::vector<PersistentIndexSstablePB>* sstables,
                                                  bool* merge_base_level) {
    // There are two levels in persistent index:
    //  1) base level. It contains only one sst file.
    //  2) cumulative level. Sst files that except base level.
    // And there are two kinds of merge:
    //  1) base merge. Merge all sst files.
    //  2) cumulative merge. Only merge cumulative sst files.
    //
    // And we use this strategy to decide whether to use base merge or cumulative merge:
    // 1. When total size of cumulative level sst files reach 1/10 of base level, use base merge.
    // 2. Otherwise, use cumulative merge.
    DCHECK(sstable_meta.sstables_size() > 0);
    int64_t base_level_bytes = 0;
    int64_t cumulative_level_bytes = 0;
    std::vector<PersistentIndexSstablePB> cumulative_sstables;
    for (int i = 0; i < sstable_meta.sstables_size(); i++) {
        if (i == 0) {
            base_level_bytes = sstable_meta.sstables(i).filesize();
        } else {
            cumulative_level_bytes += sstable_meta.sstables(i).filesize();
            cumulative_sstables.push_back(sstable_meta.sstables(i));
        }
    }

    if ((double)base_level_bytes * config::lake_pk_index_cumulative_base_compaction_ratio >
        (double)cumulative_level_bytes) {
        // cumulative merge
        sstables->swap(cumulative_sstables);
        *merge_base_level = false;
    } else {
        // base merge
        sstables->push_back(sstable_meta.sstables(0));
        sstables->insert(sstables->end(), cumulative_sstables.begin(), cumulative_sstables.end());
        *merge_base_level = true;
    }
    // Limit max sstable count that can do merge, to avoid cost too much memory.
    const int32_t max_limit = config::lake_pk_index_sst_max_compaction_versions;
    if (sstables->size() > max_limit) {
        sstables->resize(max_limit);
    }
}

Status LakePersistentIndex::prepare_merging_iterator(
        TabletManager* tablet_mgr, const TabletMetadata& metadata, TxnLogPB* txn_log,
        std::vector<std::shared_ptr<PersistentIndexSstable>>* merging_sstables,
        std::unique_ptr<sstable::Iterator>* merging_iter_ptr, bool* merge_base_level) {
    sstable::ReadOptions read_options;
    // No need to cache input sst's blocks.
    read_options.fill_cache = false;
    std::vector<sstable::Iterator*> iters;
    DeferOp free_iters([&] {
        for (sstable::Iterator* iter : iters) {
            delete iter;
        }
    });

    iters.reserve(metadata.sstable_meta().sstables().size());
    std::stringstream ss_debug;
    std::vector<PersistentIndexSstablePB> sstables_to_merge;
    // Pick sstable for merge, decide to use base merge or cumulative merge.
    pick_sstables_for_merge(metadata.sstable_meta(), &sstables_to_merge, merge_base_level);
    if (sstables_to_merge.size() <= 1) {
        // no need to do merge
        return Status::OK();
    }
    for (const auto& sstable_pb : sstables_to_merge) {
        // build sstable from meta, instead of reuse `_sstables`, to keep it thread safe
        RandomAccessFileOptions opts;
        if (!sstable_pb.encryption_meta().empty()) {
            ASSIGN_OR_RETURN(auto info, KeyCache::instance().unwrap_encryption_meta(sstable_pb.encryption_meta()));
            opts.encryption_info = std::move(info);
        }
        ASSIGN_OR_RETURN(auto rf, fs::new_random_access_file(
                                          opts, tablet_mgr->sst_location(metadata.id(), sstable_pb.filename())));
        auto merging_sstable = std::make_shared<PersistentIndexSstable>();
        RETURN_IF_ERROR(merging_sstable->init(std::move(rf), sstable_pb, nullptr, false /** no filter **/));
        merging_sstables->push_back(merging_sstable);
        // Pass `max_rss_rowid` to iterator, will be used when compaction.
        read_options.max_rss_rowid = sstable_pb.max_rss_rowid();
        sstable::Iterator* iter = merging_sstable->new_iterator(read_options);
        iters.emplace_back(iter);
        // add input sstable.
        txn_log->mutable_op_compaction()->add_input_sstables()->CopyFrom(merging_sstable->sstable_pb());
        ss_debug << sstable_pb.filename() << " | ";
    }
    sstable::Options options;
    (*merging_iter_ptr).reset(sstable::NewMergingIterator(options.comparator, &iters[0], iters.size()));
    (*merging_iter_ptr)->SeekToFirst();
    iters.clear(); // Clear the vector without deleting iterators since they are now managed by merge_iter_ptr.
    VLOG(2) << "prepare sst for merge : " << ss_debug.str();
    return Status::OK();
}

Status LakePersistentIndex::merge_sstables(std::unique_ptr<sstable::Iterator> iter_ptr, sstable::TableBuilder* builder,
                                           bool base_level_merge) {
    auto merger = std::make_unique<KeyValueMerger>(iter_ptr->key().to_string(), iter_ptr->max_rss_rowid(), builder,
                                                   base_level_merge);
    while (iter_ptr->Valid()) {
        RETURN_IF_ERROR(
                merger->merge(iter_ptr->key().to_string(), iter_ptr->value().to_string(), iter_ptr->max_rss_rowid()));
        iter_ptr->Next();
    }
    RETURN_IF_ERROR(iter_ptr->status());
    merger->finish();
    return builder->Finish();
}

Status LakePersistentIndex::major_compact(TabletManager* tablet_mgr, const TabletMetadata& metadata,
                                          TxnLogPB* txn_log) {
    if (metadata.sstable_meta().sstables_size() < config::lake_pk_index_sst_min_compaction_versions) {
        return Status::OK();
    }

    std::vector<std::shared_ptr<PersistentIndexSstable>> sstable_vec;
    std::unique_ptr<sstable::Iterator> merging_iter_ptr;
    bool merge_base_level = false;
    // build merge iterator
    RETURN_IF_ERROR(prepare_merging_iterator(tablet_mgr, metadata, txn_log, &sstable_vec, &merging_iter_ptr,
                                             &merge_base_level));
    if (merging_iter_ptr == nullptr) {
        // no need to do merge
        return Status::OK();
    }
    if (!merging_iter_ptr->Valid()) {
        return merging_iter_ptr->status();
    }

    auto filename = gen_sst_filename();
    auto location = tablet_mgr->sst_location(metadata.id(), filename);
    WritableFileOptions wopts;
    std::string encryption_meta;
    if (config::enable_transparent_data_encryption) {
        ASSIGN_OR_RETURN(auto pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
        wopts.encryption_info = pair.info;
        encryption_meta.swap(pair.encryption_meta);
    }
    ASSIGN_OR_RETURN(auto wf, fs::new_writable_file(wopts, location));
    sstable::Options options;
    std::unique_ptr<sstable::FilterPolicy> filter_policy;
    filter_policy.reset(const_cast<sstable::FilterPolicy*>(sstable::NewBloomFilterPolicy(10)));
    options.filter_policy = filter_policy.get();
    sstable::TableBuilder builder(options, wf.get());
    RETURN_IF_ERROR(merge_sstables(std::move(merging_iter_ptr), &builder, merge_base_level));
    RETURN_IF_ERROR(wf->close());

    // record output sstable pb
    txn_log->mutable_op_compaction()->mutable_output_sstable()->set_filename(filename);
    txn_log->mutable_op_compaction()->mutable_output_sstable()->set_filesize(builder.FileSize());
    txn_log->mutable_op_compaction()->mutable_output_sstable()->set_encryption_meta(encryption_meta);
    return Status::OK();
}

Status LakePersistentIndex::apply_opcompaction(const TxnLogPB_OpCompaction& op_compaction) {
    if (op_compaction.input_sstables().empty() || !op_compaction.has_output_sstable()) {
        return Status::OK();
    }

    PersistentIndexSstablePB sstable_pb;
    sstable_pb.CopyFrom(op_compaction.output_sstable());
    sstable_pb.set_max_rss_rowid(
            op_compaction.input_sstables(op_compaction.input_sstables().size() - 1).max_rss_rowid());
    auto sstable = std::make_unique<PersistentIndexSstable>();
    RandomAccessFileOptions opts;
    if (!sstable_pb.encryption_meta().empty()) {
        ASSIGN_OR_RETURN(auto info, KeyCache::instance().unwrap_encryption_meta(sstable_pb.encryption_meta()));
        opts.encryption_info = std::move(info);
    }
    ASSIGN_OR_RETURN(auto rf,
                     fs::new_random_access_file(opts, _tablet_mgr->sst_location(_tablet_id, sstable_pb.filename())));
    auto* block_cache = _tablet_mgr->update_mgr()->block_cache();
    if (block_cache == nullptr) {
        return Status::InternalError("Block cache is null.");
    }
    RETURN_IF_ERROR(sstable->init(std::move(rf), sstable_pb, block_cache->cache()));

    std::unordered_set<std::string> filenames;
    for (const auto& input_sstable : op_compaction.input_sstables()) {
        filenames.insert(input_sstable.filename());
    }
    // Erase merged sstable from sstable list
    _sstables.erase(std::remove_if(_sstables.begin(), _sstables.end(),
                                   [&](const std::unique_ptr<PersistentIndexSstable>& sstable) {
                                       return filenames.contains(sstable->sstable_pb().filename());
                                   }),
                    _sstables.end());
    // Insert sstable to sstable list by `max_rss_rowid` order.
    auto lower_it = std::lower_bound(
            _sstables.begin(), _sstables.end(), sstable,
            [](const std::unique_ptr<PersistentIndexSstable>& a, const std::unique_ptr<PersistentIndexSstable>& b) {
                return a->sstable_pb().max_rss_rowid() < b->sstable_pb().max_rss_rowid();
            });
    _sstables.insert(lower_it, std::move(sstable));
    return Status::OK();
}

Status LakePersistentIndex::commit(MetaFileBuilder* builder) {
    if (too_many_rebuild_files() && !_memtable->empty()) {
        // If we have too many files need to be rebuilt,
        // we need to do flush to reduce index rebuild cost later.
        RETURN_IF_ERROR(flush_memtable());
    }
    PersistentIndexSstableMetaPB sstable_meta;
    int64_t last_max_rss_rowid = 0;
    for (auto& sstable : _sstables) {
        int64_t max_rss_rowid = sstable->sstable_pb().max_rss_rowid();
        if (last_max_rss_rowid > max_rss_rowid) {
            return Status::InternalError(
                    fmt::format("sstables are not ordered, last_max_rss_rowid={} : max_rss_rowid={}",
                                last_max_rss_rowid, max_rss_rowid));
        }
        last_max_rss_rowid = max_rss_rowid;
        auto* sstable_pb = sstable_meta.add_sstables();
        sstable_pb->CopyFrom(sstable->sstable_pb());
    }
    builder->finalize_sstable_meta(sstable_meta);
    _need_rebuild_file_cnt = need_rebuild_file_cnt(*builder->tablet_meta(), sstable_meta);
    return Status::OK();
}

// Rebuild index's memtable via del files, it will read from del file and write to index.
// If it fail, SR will retry publish txn, and this index's memtable will be release and rebuild again.
Status LakePersistentIndex::load_dels(const RowsetPtr& rowset, const Schema& pkey_schema, int64_t rowset_version) {
    TRACE_COUNTER_SCOPE_LATENCY_US("rebuild_index_del_cost_us");
    // Build pk column struct from schema
    std::unique_ptr<Column> pk_column;
    RETURN_IF_ERROR(PrimaryKeyEncoder::create_column(pkey_schema, &pk_column));
    // Iterate all del files and insert into index.
    for (int del_idx = 0; del_idx < rowset->metadata().del_files_size(); ++del_idx) {
        TRACE_COUNTER_INCREMENT("rebuild_index_del_cnt", 1);
        const auto& del = rowset->metadata().del_files(del_idx);
        RandomAccessFileOptions ropts;
        if (!del.encryption_meta().empty()) {
            ASSIGN_OR_RETURN(ropts.encryption_info, KeyCache::instance().unwrap_encryption_meta(del.encryption_meta()));
        }
        ASSIGN_OR_RETURN(auto read_file,
                         fs::new_random_access_file(ropts, _tablet_mgr->del_location(_tablet_id, del.name())));
        ASSIGN_OR_RETURN(auto read_buffer, read_file->read_all());
        // serialize to column
        auto pkc = pk_column->clone();
        if (serde::ColumnArraySerde::deserialize(reinterpret_cast<const uint8_t*>(read_buffer.data()), pkc.get()) ==
            nullptr) {
            // Deserialze will fail when del file is corrupted.
            return Status::InternalError("column deserialization failed");
        }
        // We can't insert delete operation to index directly, because some delete operation is
        // older than current item, and we need to igore these delete operations.
        std::vector<IndexValue> found_values(pkc->size(), IndexValue(NullIndexValue));
        std::vector<bool> filter(pkc->size(), false);
        auto generate_filter_fn = [&]() {
            if (rowset->id() != del.origin_rowset_id()) {
                // del file in origin rowset doesn't need to skip.
                for (int i = 0; i < pkc->size(); i++) {
                    if (found_values[i] != IndexValue(NullIndexValue) &&
                        found_values[i].get_rssid() > del.origin_rowset_id() + del.op_offset()) {
                        // Use `rowset_id + op_offset` as delete file's rssid.
                        // delete operation is too old for this key.
                        filter[i] = true;
                    }
                }
            }
        };
        // Rssid of delete files is equal to `rowset_id + op_offset`, and delete is always after upsert now,
        // so we use max segment id as `op_offset`.
        // TODO : support real order of mix upsert and delete in one transaction.
        const uint32_t del_rebuild_rssid = rowset->id() + std::max(rowset->num_segments(), (int64_t)1) - 1;
        if (pkc->is_binary()) {
            // When PK table have multi pk columns or one pk column with varchar type,
            // we treat it as binary column.
            // 1. Get from pk index, to find out if this delete operation is too old.
            RETURN_IF_ERROR(get(pkc->size(), reinterpret_cast<const Slice*>(pkc->raw_data()), found_values.data()));
            generate_filter_fn();
            // 2. insert delete operations to pk index.
            RETURN_IF_ERROR(replay_erase(pkc->size(), reinterpret_cast<const Slice*>(pkc->raw_data()), filter,
                                         rowset_version, del_rebuild_rssid));
        } else {
            std::vector<Slice> keys;
            keys.reserve(pkc->size());
            const auto* fkeys = pkc->continuous_data();
            for (size_t i = 0; i < pkc->size(); ++i) {
                keys.emplace_back(fkeys, _key_size);
                fkeys += _key_size;
            }
            // 1. Get from pk index, to find out if this delete operation is too old.
            RETURN_IF_ERROR(get(pkc->size(), reinterpret_cast<const Slice*>(keys.data()), found_values.data()));
            generate_filter_fn();
            // 2. insert delete operations to pk index.
            RETURN_IF_ERROR(replay_erase(pkc->size(), reinterpret_cast<const Slice*>(keys.data()), filter,
                                         rowset_version, del_rebuild_rssid));
        }
    }
    return Status::OK();
}

// Check if this rowset need to rebuild, return `True` means need to rebuild this rowset.
bool LakePersistentIndex::needs_rowset_rebuild(const RowsetMetadataPB& rowset, uint32_t rebuild_rss_id) {
    if (rowset.segments_size() > 0 && (rowset.id() + rowset.segments_size() <= rebuild_rss_id)) {
        // All segments and del files under this rowset are not need to rebuild.
        // E.g.
        // If `rebuild_rss_id` is 12, and
        // 1. `id` = 10, `segments_size` = 2, we can skip this rowset, because two segment's id is
        //     10 and 11, both smaller than 12.
        // 2. `id` = 10, `segments_size` = 3, we can't skip this rowset, because last segment's id
        //     is 12 which is equal to 12, it may not dump to sst yet.
        return false;
    }
    if (rowset.segments_size() == 0 && (rowset.id() < rebuild_rss_id)) {
        // Rowset with empty segments may has del files, and need to rebuild them.
        // E.g.
        // If `rebuild_rss_id` is 12, and
        // 1. `id` = 11, can skip. it means this rowset's del files has been dump to sst.
        // 2. `id` = 12, can't skip. this rowset's del files may not dump to sst yet.
        return false;
    }
    return true;
}

// Return the files cnt that need to rebuild.
size_t LakePersistentIndex::need_rebuild_file_cnt(const TabletMetadataPB& metadata,
                                                  const PersistentIndexSstableMetaPB& sstable_meta) {
    size_t cnt = 0;
    const auto& sstables = sstable_meta.sstables();
    const uint32_t rebuild_rss_id = sstables.empty() ? 0 : sstables.rbegin()->max_rss_rowid() >> 32;
    for (const auto& rowset : metadata.rowsets()) {
        if (!needs_rowset_rebuild(rowset, rebuild_rss_id)) {
            continue; // skip rowset
        }
        cnt += rowset.del_files_size();
        // rowset id + segment id < rebuild_rss_id can be skip.
        // so only some segments in this rowset need to rebuild
        cnt += std::min(rowset.id() + rowset.segments_size() - rebuild_rss_id + 1, (uint32_t)rowset.segments_size());
    }
    return cnt;
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

    _need_rebuild_file_cnt = need_rebuild_file_cnt(*metadata, metadata->sstable_meta());

    // Init PersistentIndex
    _key_size = PrimaryKeyEncoder::get_encoded_fixed_size(pkey_schema);

    const auto& sstables = metadata->sstable_meta().sstables();
    // Rebuild persistent index from `rebuild_rss_rowid_point`
    const uint64_t rebuild_rss_rowid_point = sstables.empty() ? 0 : sstables.rbegin()->max_rss_rowid();
    const uint32_t rebuild_rss_id = rebuild_rss_rowid_point >> 32;
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
        TRACE_COUNTER_INCREMENT("total_segment_cnt", rowset->num_segments());
        TRACE_COUNTER_INCREMENT("total_num_rows", rowset->num_rows());
        if (!needs_rowset_rebuild(rowset->metadata(), rebuild_rss_id)) {
            continue;
        }
        const int64_t rowset_version = rowset->version() != 0 ? rowset->version() : base_version;
        auto res = rowset->get_each_segment_iterator_with_delvec(pkey_schema, base_version, builder, &stats);
        if (!res.ok()) {
            return res.status();
        }
        auto& itrs = res.value();
        CHECK(itrs.size() == rowset->num_segments()) << "itrs.size != num_segments";
        for (size_t i = 0; i < itrs.size(); i++) {
            TRACE_COUNTER_SCOPE_LATENCY_US("rebuild_index_segment_cost_us");
            auto itr = itrs[i].get();
            if (itr == nullptr) {
                continue;
            }
            DeferOp close_iter([&] { itr->close(); });
            if (rowset->id() + i < rebuild_rss_id) {
                // lower than rebuild point, skip
                // Notice: segment id that equal `rebuild_rss_id` can't be skip because
                // there are maybe some rows need to rebuild.
                continue;
            }
            TRACE_COUNTER_INCREMENT("rebuild_index_segment_cnt", 1);
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
                    if (values.back().get_value() <= rebuild_rss_rowid_point) {
                        // lower AND equal than rebuild point, skip
                        continue;
                    }
                    TRACE_COUNTER_INCREMENT("rebuild_index_num_rows", pkc->size());
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
        }
        // Rebuild from del files
        if (rowset->metadata().del_files_size() > 0) {
            RETURN_IF_ERROR(load_dels(rowset, pkey_schema, rowset_version));
        }
    }
    return Status::OK();
}

size_t LakePersistentIndex::memory_usage() const {
    size_t mem_usage = 0;
    if (_memtable != nullptr) {
        mem_usage += _memtable->memory_usage();
    }
    for (const auto& sst_ptr : _sstables) {
        if (sst_ptr != nullptr) {
            mem_usage += sst_ptr->memory_usage();
        }
    }
    return mem_usage;
}

} // namespace starrocks::lake
