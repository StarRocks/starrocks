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

#include "column/schema.h"
#include "storage/edit_version.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/persistent_index.h"
#include "storage/rowset/segment.h"

namespace starrocks {

class DataDir;

namespace lake {

class Tablet;
class MetaFileBuilder;

class LakeLocalPersistentIndexTabletLoader : public TabletLoader {
public:
    LakeLocalPersistentIndexTabletLoader(TabletManager* tablet_mgr, const TabletMetadataPtr& metadata,
                                         int64_t base_version, const MetaFileBuilder* builder)
            : _tablet_mgr(tablet_mgr), _metadata(metadata), _base_version(base_version), _builder(builder) {}
    ~LakeLocalPersistentIndexTabletLoader() = default;
    starrocks::Schema generate_pkey_schema() override;
    DataDir* data_dir() override;
    TTabletId tablet_id() override;
    // return latest applied (publish in cloud native) version
    StatusOr<EditVersion> applied_version() override;
    // Do some special setting if need
    void setting() override;
    // iterator all rowset and get their iterator and basic stat
    Status rowset_iterator(
            const Schema& pkey_schema,
            const std::function<Status(const std::vector<ChunkIteratorPtr>&, uint32_t)>& handler) override;

private:
    TabletManager* _tablet_mgr;
    const TabletMetadataPtr _metadata;
    int64_t _base_version;
    const MetaFileBuilder* _builder;
};

} // namespace lake
} // namespace starrocks