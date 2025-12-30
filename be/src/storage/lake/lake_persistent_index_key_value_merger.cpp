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
#include "storage/sstable/sstable_predicate.h"
#include "storage/sstable/table_builder.h"
#include "util/trace.h"

namespace starrocks::lake {

Status KeyValueMerger::merge(const sstable::Iterator* iter_ptr) {
    const std::string& key = iter_ptr->key().to_string();
    const std::string& value = iter_ptr->value().to_string();
    uint64_t max_rss_rowid = iter_ptr->max_rss_rowid();
    const auto& predicate = iter_ptr->predicate();

    IndexValuesWithVerPB index_value_ver;
    if (!index_value_ver.ParseFromString(value)) {
        return Status::InternalError("Failed to parse index value ver");
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
    // fill shared version & rssid if have
    if (iter_ptr->shared_version() > 0) {
        for (size_t i = 0; i < index_value_ver.values_size(); ++i) {
            index_value_ver.mutable_values(i)->set_version(iter_ptr->shared_version());
            index_value_ver.mutable_values(i)->set_rssid(iter_ptr->shared_rssid());
        }
    }

    /*
     * Do not distinguish between base compaction and cumulative compaction here.
     * Currently we use predicate after tablet split and make predicate available
     * for both base compaction and cumulative compaction is useful and will not
     * cause any problem.
     *
     * But if caller for another purpose to use this predicate here, should pay attention
     * if it is only used for base compaction or cumulative compaction.
    */
    if (predicate != nullptr) {
        uint8_t selection = 0;
        RETURN_IF_ERROR(_predicate_evaluator.evaluate_with_cache(predicate, key, &selection));
        if (!selection) {
            // If the key is not hit, we skip it.
            return Status::OK();
        }
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
        RETURN_IF_ERROR(flush());
        _key = key;
        _max_rss_rowid = max_rss_rowid;
        _index_value_vers.emplace_front(version, index_value);
    }
    return Status::OK();
}

Status KeyValueMerger::flush() {
    if (_index_value_vers.empty()) {
        return Status::OK();
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
        if (_output_builders.empty() ||
            (_enable_multiple_output_files &&
             _output_builders.back().table_builder->FileSize() >= config::pk_index_target_file_size)) {
            // Create a new sst file when current file is empty or exceed target size.
            RETURN_IF_ERROR(create_table_builder());
        }
        RETURN_IF_ERROR(
                _output_builders.back().table_builder->Add(Slice(_key), Slice(index_value_pb.SerializeAsString())));
    }
    _index_value_vers.clear();

    return Status::OK();
}

// return list<filename, filesize, encryption_meta>
StatusOr<std::vector<KeyValueMerger::KeyValueMergerOutput>> KeyValueMerger::finish() {
    RETURN_IF_ERROR(flush());
    std::vector<KeyValueMergerOutput> results;
    for (auto& builder_wrapper : _output_builders) {
        RETURN_IF_ERROR(builder_wrapper.table_builder->Finish());
        RETURN_IF_ERROR(builder_wrapper.wf->close());
        results.emplace_back(KeyValueMerger::KeyValueMergerOutput{
                builder_wrapper.filename, builder_wrapper.table_builder->FileSize(), builder_wrapper.encryption_meta,
                builder_wrapper.table_builder->KeyRange().first.to_string(),
                builder_wrapper.table_builder->KeyRange().second.to_string()});
    }
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