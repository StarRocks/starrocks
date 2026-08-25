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

#include "storage/lake/lake_persistent_index_key_value_merger.h"

#include "base/debug/trace.h"
#include "base/testutil/sync_point.h"
#include "column/serde/column_array_serde.h"
#include "common/config_primary_key_fwd.h"
#include "common/config_rowset_fwd.h"
#include "fs/fs_util.h"
#include "platform/key_cache.h"
#include "storage/chunk_helper.h"
#include "storage/lake/filenames.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/persistent_index_memtable.h"
#include "storage/lake/persistent_index_sstable.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/update_manager.h"
#include "storage/lake/utils.h"
#include "storage/sstable/iterator.h"
#include "storage/sstable/merger.h"
#include "storage/sstable/options.h"
#include "storage/sstable/table_builder.h"
#include "storage_primitive/primary_key_encoder.h"

namespace starrocks::lake {

KeyValueMerger::~KeyValueMerger() {
    if (_outputs_released) return;

    for (size_t builder_index = 0; builder_index < _output_builders.size(); ++builder_index) {
        auto& builder_wrapper = _output_builders[builder_index];
        if (!builder_wrapper.finish_attempted) {
            TEST_SYNC_POINT_CALLBACK("KeyValueMerger::cleanup:abandon", &builder_index);
            builder_wrapper.table_builder->Abandon();
        }
        builder_wrapper.table_builder.reset();
        if (!builder_wrapper.close_attempted) {
            builder_wrapper.close_attempted = true;
            TEST_SYNC_POINT_CALLBACK("KeyValueMerger::cleanup:close", &builder_index);
            (void)builder_wrapper.wf->close();
        }
        builder_wrapper.wf.reset();
        const auto location = _tablet_mgr->sst_location(_tablet_id, builder_wrapper.filename);
        auto status = fs::delete_file(location);
        LOG_IF(WARNING, !status.ok() && !status.is_not_found())
                << "failed to clean up key-value merger output " << location << ": " << status;
    }
}

Status KeyValueMerger::merge(const sstable::Iterator* iter_ptr) {
    // Iterator-owned slices stay valid until iter_ptr->Next(); both the parse
    // and the equality check below complete before the caller advances the
    // iterator, so we can avoid the per-key std::string heap allocations that
    // Slice::to_string() would force on every input row.
    const Slice key = iter_ptr->key();
    const Slice value = iter_ptr->value();
    uint64_t max_rss_rowid = iter_ptr->max_rss_rowid();

    // Reuse scratch protobuf to avoid allocating/freeing internal storage on every key.
    IndexValuesWithVerPB& index_value_ver = _merge_pb_scratch;
    index_value_ver.Clear();
    if (!index_value_ver.ParseFromArray(value.data, value.size)) {
        // These bytes were written as a serialized IndexValuesWithVerPB into the SST
        // data block, so a parse failure means the persisted content is corrupted
        // (usually a bad local cache copy). Compaction callers key their
        // drop-corrupted-cache handling off is_corruption().
        return Status::Corruption("Failed to parse index value ver");
    }
    if (index_value_ver.values_size() == 0) {
        return Status::OK();
    }
    // filter rows which already been deleted in this sst
    if (iter_ptr->delvec() != nullptr && !iter_ptr->delvec()->empty() &&
        iter_ptr->delvec()->roaring()->contains(index_value_ver.values(0).rowid())) {
        // this row has been deleted in this sst, skip it
        return Status::OK();
    }
    // Tombstone-aware projection: see is_index_tombstone() in storage/lake/utils.h
    // for why the rssid/rowid sentinel must be preserved. Version is independent of
    // the NullIndexValue encoding and must be projected even onto tombstones — the
    // dup-resolution below in this function and the time-travel multi_get path both
    // key off version, and a tombstone left at its source-sstable version would
    // lose ordering against live entries projected to shared_version.
    if (iter_ptr->shared_version() > 0) {
        for (size_t i = 0; i < index_value_ver.values_size(); ++i) {
            index_value_ver.mutable_values(i)->set_version(iter_ptr->shared_version());
            if (is_index_tombstone(index_value_ver.values(i))) continue;
            index_value_ver.mutable_values(i)->set_rssid(iter_ptr->shared_rssid());
        }
    }
    if (iter_ptr->rssid_offset() != 0) {
        const int32_t rssid_offset = iter_ptr->rssid_offset();
        // Per-entry projection: stored bytes are in pre-projection space, so
        // each entry's rssid must be lifted by rssid_offset to land in the
        // current child's effective id space.
        for (size_t i = 0; i < index_value_ver.values_size(); ++i) {
            if (is_index_tombstone(index_value_ver.values(i))) continue;
            const int64_t rssid = static_cast<int64_t>(index_value_ver.values(i).rssid()) + rssid_offset;
            index_value_ver.mutable_values(i)->set_rssid(static_cast<uint32_t>(rssid));
        }
        // `max_rss_rowid` already lives in the source sstable's effective id
        // space. It must not be shifted by rssid_offset again because that
        // would double-count the offset in the same-version tie-break below.
    }

    auto version = index_value_ver.values(0).version();
    auto index_value = build_index_value(index_value_ver.values(0));
    if (Slice(_key) == key) {
        if (!_current_value.has_value()) {
            _max_rss_rowid = max_rss_rowid;
            _current_value.emplace(version, index_value);
        } else if ((version > _current_value->first) ||
                   (version == _current_value->first && max_rss_rowid > _max_rss_rowid) ||
                   (version == _current_value->first && max_rss_rowid == _max_rss_rowid &&
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
            _current_value.emplace(version, index_value);
        }
    } else {
        RETURN_IF_ERROR(flush());
        _key.assign(key.data, key.size);
        _max_rss_rowid = max_rss_rowid;
        _current_value.emplace(version, index_value);
    }
    return Status::OK();
}

Status KeyValueMerger::flush() {
    if (!_current_value.has_value()) {
        return Status::OK();
    }

    const auto& current = *_current_value;
    const bool skip_tombstone = _merge_base_level && current.second == IndexValue(NullIndexValue);
    if (!skip_tombstone) {
        // Reuse scratch protobuf and serialization buffer to avoid allocating fresh
        // RepeatedField storage and a new std::string on every flushed key.
        IndexValuesWithVerPB& index_value_pb = _flush_pb_scratch;
        index_value_pb.Clear();
        auto* value = index_value_pb.add_values();
        value->set_version(current.first);
        value->set_rssid(current.second.get_rssid());
        value->set_rowid(current.second.get_rowid());

        if (_output_builders.empty() ||
            (_enable_multiple_output_files &&
             _output_builders.back().table_builder->FileSize() >= config::pk_index_target_file_size)) {
            // Create a new sst file when current file is empty or exceed target size.
            RETURN_IF_ERROR(create_table_builder());
        }
        _flush_serialized_scratch.clear();
        if (!index_value_pb.SerializeToString(&_flush_serialized_scratch)) {
            return Status::InternalError("Failed to serialize IndexValuesWithVerPB");
        }
        RETURN_IF_ERROR(_output_builders.back().table_builder->Add(Slice(_key), Slice(_flush_serialized_scratch)));
    }
    _current_value.reset();

    return Status::OK();
}

// return list<filename, filesize, encryption_meta>
StatusOr<std::vector<KeyValueMerger::KeyValueMergerOutput>> KeyValueMerger::finish() {
    RETURN_IF_ERROR(flush());
    std::vector<KeyValueMergerOutput> results;
    for (size_t builder_index = 0; builder_index < _output_builders.size(); ++builder_index) {
        auto& builder_wrapper = _output_builders[builder_index];
        TEST_SYNC_POINT_CALLBACK("KeyValueMerger::finish:finish_attempt", &builder_index);
        builder_wrapper.finish_attempted = true;
        RETURN_IF_ERROR(builder_wrapper.table_builder->Finish());
        const uint64_t filesize = builder_wrapper.table_builder->FileSize();
        const auto [start_key, end_key] = builder_wrapper.table_builder->KeyRange();
        builder_wrapper.close_attempted = true;
        Status close_status = builder_wrapper.wf->close();
        std::pair<size_t, Status*> close_result{builder_index, &close_status};
        TEST_SYNC_POINT_CALLBACK("KeyValueMerger::finish:close_status", &close_result);
        if (!close_status.ok()) return close_status;
        results.emplace_back(KeyValueMerger::KeyValueMergerOutput{builder_wrapper.filename, filesize,
                                                                  builder_wrapper.encryption_meta,
                                                                  start_key.to_string(), end_key.to_string()});
    }
    _outputs_released = true;
    return results;
}

Status KeyValueMerger::create_table_builder() {
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
    sstable::Options options;
    std::unique_ptr<sstable::FilterPolicy> filter_policy;
    filter_policy.reset(const_cast<sstable::FilterPolicy*>(sstable::NewBloomFilterPolicy(10)));
    options.filter_policy = filter_policy.get();
    std::unique_ptr<sstable::TableBuilder> table_builder = std::make_unique<sstable::TableBuilder>(options, wf.get());
    _output_builders.emplace_back(TableBuilderWrapper{filename, encryption_meta, std::move(wf),
                                                      std::move(filter_policy), std::move(table_builder)});
    return Status::OK();
}
} // namespace starrocks::lake
