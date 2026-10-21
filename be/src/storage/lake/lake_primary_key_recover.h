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

#include <deque>
#include <string>
#include <utility>
#include <vector>

#include "column/schema.h"
#include "common/status.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/types_fwd.h"
#include "storage/primary_key_recover.h"

namespace starrocks {

class Column;

namespace lake {

class Tablet;
class MetaFileBuilder;

class LakePrimaryKeyRecover : public PrimaryKeyRecover {
public:
    explicit LakePrimaryKeyRecover(MetaFileBuilder* builder, Tablet* tablet, MutableTabletMetadataPtr metadata)
            : _builder(builder), _tablet(tablet), _metadata(std::move(metadata)) {}
    ~LakePrimaryKeyRecover() override = default;

    // clean old state, include pk index and delvec
    Status pre_cleanup() override;

    // Primary key schema
    starrocks::Schema generate_pkey_schema() override;

    Status rowset_iterator(
            const starrocks::Schema& pkey_schema, OlapReaderStatistics& stats,
            const std::function<
                    Status(const std::vector<ChunkIteratorPtr>&, const std::vector<std::unique_ptr<RandomAccessFile>>&,
                           const std::vector<uint32_t>&, const std::vector<uint32_t>&, uint32_t)>& handler) override;

    // generate delvec and save
    Status finalize_delvec(const PrimaryIndex::DeletesMap& new_deletes) override;

    int64_t tablet_id() override;

    // A compaction output's rows are as old as its newest input rowset, max_compact_input_rowset_id: the
    // compaction may publish after deletes it did not see, so its own id would make its rows look newer.
    uint32_t data_age(uint32_t rssid) const override;

    StatusOr<PrimaryKeyEncodingType> primary_key_encoding_type() const override;

    // Sorrt rowset by rowsetid
    // also consider sorting in data loading and compact concurrency scenarios
    static Status sort_rowsets(std::vector<RowsetPtr>* rowsets);

private:
    MetaFileBuilder* _builder;
    Tablet* _tablet;
    MutableTabletMetadataPtr _metadata;
    MutableColumnPtr _pk_column;
    // [first rssid, end rssid) and max_compact_input_rowset_id of each compaction output, sorted by first rssid.
    struct CompactedRange {
        uint32_t begin;
        uint32_t end;
        uint32_t data_age;
    };
    std::vector<CompactedRange> _compacted_ranges;
};

} // namespace lake
} // namespace starrocks
