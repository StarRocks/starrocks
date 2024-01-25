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

#include "storage/lake/lake_local_persistent_index.h"

#include "storage/chunk_helper.h"
#include "storage/lake/lake_local_persistent_index_tablet_loader.h"
#include "storage/lake/lake_primary_index.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/rowset.h"
#include "storage/primary_key_encoder.h"
#include "storage/tablet_meta_manager.h"

namespace starrocks::lake {

// TODO refactor load from lake tablet, use same path with load from local tablet.
Status LakeLocalPersistentIndex::load_from_lake_tablet(TabletManager* tablet_mgr, const TabletMetadataPtr& metadata,
                                                       int64_t base_version, const MetaFileBuilder* builder) {
    const auto tablet_id = metadata->id();
    if (!is_primary_key(*metadata)) {
        LOG(WARNING) << "tablet: " << tablet_id << " is not primary key tablet";
        return Status::NotSupported("Only PrimaryKey table is supported to use persistent index");
    }

    std::unique_ptr<TabletLoader> loader =
            std::make_unique<LakeLocalPersistentIndexTabletLoader>(tablet_mgr, metadata, base_version, builder);
    return _load_by_loader(loader.get());
}

} // namespace starrocks::lake
