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
#include "storage/lake/versioned_tablet.h"
#include "storage/meta_reader.h"
#include "storage/olap_common.h"

#ifdef BE_TEST
#define DECL_FINAL
#define UT_VIRTUAL virtual
#else
#define DECL_FINAL final
#define UT_VIRTUAL
#endif

namespace starrocks {
class TabletSchema;

struct LakeMetaReaderParams : MetaReaderParams {
    LakeMetaReaderParams() = default;
};

namespace lake {
class Rowset;
class VersionedTablet;
} // namespace lake

// MetaReader will implements
// 1. read meta info from segment footer
// 2. read dict info from dict page if column is dict encoding type
class LakeMetaReader DECL_FINAL : public MetaReader {
public:
    LakeMetaReader();
    ~LakeMetaReader() override;

    UT_VIRTUAL Status init(const LakeMetaReaderParams& read_params);

    Status do_get_next(ChunkPtr* chunk) override;

private:
    Status _build_collect_context(const lake::VersionedTablet& tablet, const LakeMetaReaderParams& read_params);

    Status _init_seg_meta_collecters(const lake::VersionedTablet& tablet, const LakeMetaReaderParams& read_params);

    Status _get_segments(const lake::VersionedTablet& tablet, std::vector<SegmentSharedPtr>* segments);
};

} // namespace starrocks

#undef DECL_FINAL
