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

#include <string>
#include <vector>

#include "column/vectorized_fwd.h"
#include "runtime/descriptors.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet.h"
#include "storage/meta_reader.h"
#include "storage/olap_common.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/segment.h"

namespace starrocks {
// Params for MetaReader
// mainly include tablet
struct LakeMetaReaderParams : MetaReaderParams {
    LakeMetaReaderParams() = default;
    StatusOr<lake::Tablet> tablet;
    std::shared_ptr<const TabletSchema> tablet_schema;
};

// MetaReader will implements
// 1. read meta info from segment footer
// 2. read dict info from dict page if column is dict encoding type
class LakeMetaReader final : public MetaReader {
public:
    LakeMetaReader();
    ~LakeMetaReader() override = default;

    Status init(const LakeMetaReaderParams& read_params);

    lake::Tablet tablet() { return _tablet.value(); }

    Status do_get_next(ChunkPtr* chunk) override;

private:
    StatusOr<lake::Tablet> _tablet;
    std::shared_ptr<const TabletSchema> _tablet_schema;
    std::vector<lake::Rowset> _rowsets;

    Status _init_params(const LakeMetaReaderParams& read_params);

    Status _build_collect_context(const LakeMetaReaderParams& read_params);

    Status _init_seg_meta_collecters(const LakeMetaReaderParams& read_params);

    Status _get_segments(lake::Tablet tablet, const Version& version, std::vector<SegmentSharedPtr>* segments);
};

} // namespace starrocks
