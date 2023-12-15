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

#include "column/schema.h"
#include "common/status.h"
#include "storage/lake/tablet_metadata.h"

namespace starrocks {

class Column;

namespace lake {

class Tablet;
class MetaFileBuilder;

/**
 * PrimaryKeyRecover is used for error recover when tablet is in inconsistent state.
 * Tablet's inconsistent state means following types of anomalies have occurred:
 * 1. Failure of primary key uniqueness constraints. Duplicate primary keys show up.
 * 2. Internal data correctness check failure.
 * 
 * So when these anomalies show up (Because of Bug or something), and we don't want to drop this partition.
 * That we support a way here to recover from this situation, by `PrimaryKeyRecover`. 
 * `PrimaryKeyRecover` will regard segment files in tablet meta as single source of truth, remove old delvec files and pk index.
 * and then rebuild delvec files and new pk index from scratch.
*/
class PrimaryKeyRecover {
public:
    explicit PrimaryKeyRecover(MetaFileBuilder* builder, Tablet* tablet, TabletMetadata* metadata)
            : _builder(builder), _tablet(tablet), _metadata(metadata) {}
    ~PrimaryKeyRecover() {}

    // Follow the steps below:
    // 1. reset_state
    // 2. rebuild
    //
    // clean up delvec and primary index
    Status pre_cleanup();
    // rebuild delvec and primary index
    Status recover();

private:
    Status _init_schema(const TabletMetadata& metadata);

private:
    MetaFileBuilder* _builder;
    Tablet* _tablet;
    TabletMetadata* _metadata;
    std::unique_ptr<Column> _pk_column;
    starrocks::Schema _pkey_schema;
};

} // namespace lake
} // namespace starrocks