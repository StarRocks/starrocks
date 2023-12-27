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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/rowset_writer_context.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "fs/fs.h"
#include "gen_cpp/olap_file.pb.h"
#include "runtime/global_dict/types_fwd_decl.h"
#include "storage/type_utils.h"

namespace starrocks {

class TabletSchema;

enum RowsetWriterType { kHorizontal = 0, kVertical = 1 };

class RowsetWriterContext {
public:
    RowsetWriterContext() {
        load_id.set_hi(0);
        load_id.set_lo(0);
    }

    RowsetWriterContext(const RowsetWriterContext&) = default;
    RowsetWriterContext& operator=(const RowsetWriterContext&) = default;

    std::string rowset_path_prefix;

    TabletSchemaCSPtr tablet_schema = nullptr;
    TabletSchemaCSPtr full_tablet_schema = nullptr;
    bool is_partial_update = false;
    std::vector<int32_t> referenced_column_ids;

    RowsetId rowset_id{};
    int64_t tablet_id = 0;
    int64_t tablet_schema_hash = 0;
    int64_t partition_id = 0;
    int64_t txn_id = 0;
    Version version{};
    TabletUid tablet_uid = {0, 0};
    PUniqueId load_id{};
    bool schema_change_sorting = false;

    RowsetStatePB rowset_state = PREPARED;
    SegmentsOverlapPB segments_overlap = OVERLAP_UNKNOWN;

    // segment file use uint32 to represent row number, therefore the maximum is UINT32_MAX.
    // the default is set to INT32_MAX to avoid overflow issue when casting from uint32_t to int.
    // test cases can change this value to control flush timing
    uint32_t max_rows_per_segment = INT32_MAX;

    GlobalDictByNameMaps* global_dicts = nullptr;

    RowsetWriterType writer_type = kHorizontal;

    std::string merge_condition;

    bool miss_auto_increment_column = false;

    // partial update mode
    PartialUpdateMode partial_update_mode = PartialUpdateMode::UNKNOWN_MODE;
};

} // namespace starrocks
