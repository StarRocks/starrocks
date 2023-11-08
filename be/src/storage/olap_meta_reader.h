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
#include "storage/meta_reader.h"
#include "storage/olap_common.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/segment.h"
#include "storage/tablet.h"

namespace starrocks {
class Tablet;
// Params for MetaReader
// mainly include tablet
struct OlapMetaReaderParams : MetaReaderParams {
    OlapMetaReaderParams() = default;

    TabletSharedPtr tablet;
    TabletSchemaCSPtr tablet_schema;
};

class OlapMetaReader final : public MetaReader {
public:
    OlapMetaReader();
    ~OlapMetaReader() override;

    Status init(const OlapMetaReaderParams& read_params);

    TabletSharedPtr tablet() { return _tablet; }

    Status do_get_next(ChunkPtr* chunk) override;

private:
    TabletSharedPtr _tablet;
    TabletSchemaSPtr _tablet_schema;
    std::vector<RowsetSharedPtr> _rowsets;

    Status _init_params(const OlapMetaReaderParams& read_params);

    Status _build_collect_context(const OlapMetaReaderParams& read_params);

    Status _init_seg_meta_collecters(const OlapMetaReaderParams& read_params);

    Status _get_segments(const TabletSharedPtr& tablet, const Version& version,
                         std::vector<SegmentSharedPtr>* segments);
};

} // namespace starrocks
