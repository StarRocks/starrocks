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

#include <future>
#include <memory>
#include <string>
#include <vector>

#include "gutil/macros.h"
#include "runtime/global_dict/types_fwd_decl.h"
#include "storage/lake/persistent_index_sstable.h"
#include "storage/lake/tablet_writer.h"
#include "storage/sstable/table_builder.h"

namespace starrocks::lake {

class PersistentIndexSstableStreamBuilder;

class PkTabletSSTWriter {
public:
    PkTabletSSTWriter(const TabletSchemaCSPtr& tablet_schema_ptr, TabletManager* tablet_mgr, int64_t tablet_id)
            : _tablet_schema_ptr(tablet_schema_ptr), _tablet_mgr(tablet_mgr), _tablet_id(tablet_id) {}
    virtual ~PkTabletSSTWriter() = default;
    Status append_sst_record(const Chunk& data);
    Status reset_sst_writer(const std::shared_ptr<LocationProvider>& location_provider,
                            const std::shared_ptr<FileSystem>& fs);
    StatusOr<FileInfo> flush_sst_writer();

private:
    std::unique_ptr<PersistentIndexSstableStreamBuilder> _pk_sst_builder;
    MutableColumnPtr _pk_column;
    Schema _pkey_schema;
    size_t _key_size = 0;
    TabletSchemaCSPtr _tablet_schema_ptr;
    TabletManager* _tablet_mgr;
    int64_t _tablet_id;
};

} // namespace starrocks::lake
