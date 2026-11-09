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
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "column/schema.h"
#include "common/status.h"
#include "storage/del_vector.h"
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
            const std::function<Status(const std::vector<ChunkIteratorPtr>&, const std::vector<DelKeysReader>&,
                                       const std::vector<uint32_t>&, uint32_t)>& handler) override;

    // generate delvec and save
    Status finalize_delvec(const PrimaryIndex::DeletesMap& new_deletes) override;

    int64_t tablet_id() override;

    StatusOr<PrimaryKeyEncodingType> primary_key_encoding_type() const override;

    // Sorts |rowsets| into the order recovery replays them in. A compaction output takes the place of its newest
    // input, so that the writes that raced with the compaction, all newer than every input, replay after it. Any
    // other rowset, the one a position delete appended included, is at its id. Rowsets at one place go by id.
    static Status sort_rowsets(std::vector<RowsetPtr>* rowsets);

private:
    // Where a position delete's mask page starts, (version, offset), which names the page within the tablet. A mask page
    // holds the mask ids of the stale candidate rows of one candidate set, and the row mask of each del file of the set
    // refers to it.
    using PositionDeleteMaskPageId = std::pair<int64_t, uint64_t>;
    static PositionDeleteMaskPageId position_delete_mask_page_id(const DelvecPagePB& page) {
        return {page.version(), page.offset()};
    }

    // A mask page read for the row masks that refer to it: where it is, with the size and checksum that the first of
    // them names it with, and the mask ids it holds.
    struct PositionDeleteMaskPage {
        DelvecPagePB page;
        DelVectorPtr delvec;
    };

    // Reads into _position_delete_mask_pages the mask pages that the del files recovery keeps refer to, those that each
    // rowset wrote itself, before pre_cleanup() drops the delete vector metadata that says which file holds each
    // version, and before it resets the builder: a page this publish wrote is not in a file yet and comes from the
    // builder. A del file is told apart by its own fields: one with mask ids is a position delete's, one of keys alone
    // an ordinary one. Corruption if the fields of a del file disagree with each other, if a rowset marked
    // position_delete holds a del file of keys alone, or if a mask page cannot be read or does not match the row masks
    // that refer to it.
    Status read_position_delete_mask_pages();

    // Appends to |keys| the keys that del file |del| deletes. For an ordinary del file those are all its keys, and the
    // file's checksum is verified when it has one. For a position delete's del file they are the keys of the delete
    // entries its mask page does not mask, those of live candidate rows; Corruption if an entry's mask id is not below
    // the total_rows of the row mask.
    Status read_keys_to_delete(const DelfileWithRowsetId& del, Column* keys);

    MetaFileBuilder* _builder;
    Tablet* _tablet;
    MutableTabletMetadataPtr _metadata;
    MutableColumnPtr _pk_column;
    std::map<PositionDeleteMaskPageId, PositionDeleteMaskPage> _position_delete_mask_pages;
};

} // namespace lake
} // namespace starrocks
