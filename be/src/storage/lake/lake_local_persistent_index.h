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

#include "storage/lake/tablet.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/persistent_index.h"
#include "storage/storage_engine.h"

namespace starrocks::lake {

class MetaFileBuilder;
class LakePrimaryIndex;
class TabletManager;

class LakeLocalPersistentIndex : public PersistentIndex {
public:
    explicit LakeLocalPersistentIndex(std::string path) : PersistentIndex(std::move(path)) {}

    ~LakeLocalPersistentIndex() override = default;

    Status load_from_lake_tablet(TabletManager* tablet_mgr, const TabletMetadataPtr& metadata, int64_t base_version,
                                 const MetaFileBuilder* builder);

    double get_write_amp_score() const { return _write_amp_score.load(); }

    void set_write_amp_score(double score) { _write_amp_score.store(score); }

private:
    std::atomic<double> _write_amp_score{0.0};
};

} // namespace starrocks::lake
