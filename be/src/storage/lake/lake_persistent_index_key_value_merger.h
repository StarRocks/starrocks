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

#pragma once

#include "storage/lake/tablet_metadata.h"
#include "storage/lake/types_fwd.h"
#include "storage/persistent_index.h"
#include "storage/sstable/filter_policy.h"
#include "storage/sstable/sstable_predicate_utils.h"
#include "storage/sstable/table_builder.h"

namespace starrocks {

namespace sstable {
class Iterator;
class TableBuilder;
} // namespace sstable

namespace lake {

class TabletManager;

class KeyValueMerger {
public:
    explicit KeyValueMerger(const std::string& key, uint64_t max_rss_rowid, bool merge_base_level,
                            TabletManager* tablet_mgr, int64_t tablet_id, bool enable_multiple_output_files)
            : _key(std::move(key)),
              _max_rss_rowid(max_rss_rowid),
              _merge_base_level(merge_base_level),
              _tablet_mgr(tablet_mgr),
              _tablet_id(tablet_id),
              _enable_multiple_output_files(enable_multiple_output_files) {}

    ~KeyValueMerger() = default;

    struct TableBuilderWrapper {
        std::string filename;
        std::string encryption_meta;
        std::unique_ptr<WritableFile> wf;
        std::unique_ptr<sstable::FilterPolicy> filter_policy;
        // destroy first.
        std::unique_ptr<sstable::TableBuilder> table_builder;
    };

    struct KeyValueMergerOutput {
        std::string filename;
        uint64_t filesize = 0;
        std::string encryption_meta;
        std::string start_key;
        std::string end_key;
    };

    Status merge(const sstable::Iterator* iter_ptr);

    // return list<filename, filesize, encryption_meta, start_key, end_key>
    StatusOr<std::vector<KeyValueMergerOutput>> finish();

    Status create_table_builder();

private:
    Status flush();

private:
    std::string _key;
    uint64_t _max_rss_rowid = 0;
    std::list<IndexValueWithVer> _index_value_vers;
    // If do merge base level, that means we can delete NullIndexValue items safely.
    bool _merge_base_level = false;
    TabletManager* _tablet_mgr = nullptr;
    int64_t _tablet_id = 0;
    sstable::CachedPredicateEvaluator _predicate_evaluator;
    // Enable multiple output files. We will generate multiple output files when
    // data volume larger than pk_index_target_file_size.
    bool _enable_multiple_output_files = false;
    std::vector<TableBuilderWrapper> _output_builders;
};
} // namespace lake
} // namespace starrocks