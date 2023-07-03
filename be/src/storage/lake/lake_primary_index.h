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
#include <unordered_map>

#include "storage/lake/tablet_metadata.h"
#include "storage/lake/types_fwd.h"
#include "storage/primary_index.h"

namespace starrocks {

namespace lake {

class Tablet;
class MetaFileBuilder;

class LakePrimaryIndex : public PrimaryIndex {
public:
    LakePrimaryIndex() : PrimaryIndex() {}
    LakePrimaryIndex(const Schema& pk_schema) : PrimaryIndex(pk_schema) {}
    ~LakePrimaryIndex() {}

    // Fetch all primary keys from the tablet associated with this index into memory
    // to build a hash index.
    //
    // [thread-safe]
    Status lake_load(Tablet* tablet, const TabletMetadata& metadata, int64_t base_version,
                     const MetaFileBuilder* builder);

    bool is_load(int64_t base_version);

    int64_t data_version() const { return _data_version; }
    void update_data_version(int64_t version) { _data_version = version; }

private:
    Status _do_lake_load(Tablet* tablet, const TabletMetadata& metadata, int64_t base_version,
                         const MetaFileBuilder* builder);

private:
    // We don't support multi version in PrimaryIndex yet, but we will record latest data version for some checking
    int64_t _data_version = 0;
};

} // namespace lake
} // namespace starrocks
