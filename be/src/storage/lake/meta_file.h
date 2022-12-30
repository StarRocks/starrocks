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

#include "column/vectorized_fwd.h"
#include "fs/fs.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/types_fwd.h"
#include "storage/olap_common.h"

namespace starrocks {

class DelVector;

namespace lake {

class UpdateManager;

class MetaFileBuilder {
public:
    explicit MetaFileBuilder(std::shared_ptr<TabletMetadata>& metadata_ptr);
    ~MetaFileBuilder() {}
    //// for PK table
    void append_delvec(DelVectorPtr delvec, uint32_t segment_id);
    void apply_opwrite(const TxnLogPB_OpWrite& op_write);
    Status finalize(LocationProvider* location_provider, UpdateManager* mgr);
    StatusOr<bool> find_delvec(const TabletSegmentId& tsid, DelVectorPtr* pdelvec);

    //// for non-PK table
    Status finalize(LocationProvider* location_provider);

private:
    Status _finalize_delvec(LocationProvider* location_provider, int64_t version);

private:
    std::shared_ptr<TabletMetadata> _tablet_meta;
    Buffer<uint8_t> _buf;
    std::unordered_map<uint32_t, DelvecPairPB> _delvecs;
    std::vector<std::pair<TabletSegmentId, DelVectorPtr>> _cache_delvec_updates;
};

class MetaFileReader {
public:
    explicit MetaFileReader(const std::string& filepath, bool fill_cache);
    ~MetaFileReader() {}
    Status load();
    Status get_del_vec(LocationProvider* location_provider, uint32_t segment_id, DelVector* delvec,
                       int64_t* latest_version);
    StatusOr<TabletMetadataPtr> get_meta();

private:
    std::unique_ptr<RandomAccessFile> _access_file;
    std::shared_ptr<TabletMetadata> _tablet_meta;
    Status _err_status;
    bool _load;
};

} // namespace lake
} // namespace starrocks