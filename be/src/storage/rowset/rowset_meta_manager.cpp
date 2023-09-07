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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/rowset_meta_manager.cpp

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

#include "storage/rowset/rowset_meta_manager.h"

#include <fmt/format.h>

#include <string>
#include <vector>

#include "common/logging.h"
#include "common/status.h"
#include "gutil/strings/split.h"
#include "storage/kv_store.h"

namespace starrocks {

const std::string ROWSET_PREFIX = "rst_";

bool RowsetMetaManager::check_rowset_meta(KVStore* meta, const TabletUid& tablet_uid, const RowsetId& rowset_id) {
    std::string key = get_rowset_meta_key(tablet_uid, rowset_id);
    std::string value;
    return meta->get(META_COLUMN_FAMILY_INDEX, key, &value).ok();
}

Status RowsetMetaManager::save(KVStore* meta, const TabletUid& tablet_uid, const RowsetMetaPB& rowset_meta_pb) {
    std::string key = fmt::format("{}{}_{}", ROWSET_PREFIX, tablet_uid.to_string(), rowset_meta_pb.rowset_id());
    std::string value;
    bool ret = rowset_meta_pb.SerializeToString(&value);
    if (!ret) {
        std::string error_msg = "serialize rowset pb failed. rowset id:" + key;
        LOG(WARNING) << error_msg;
        return Status::InternalError("fail to serialize rowset meta");
    }
    return meta->put(META_COLUMN_FAMILY_INDEX, key, value);
}

Status RowsetMetaManager::flush(KVStore* meta) {
    return meta->flushWAL();
}

Status RowsetMetaManager::remove(KVStore* meta, const TabletUid& tablet_uid, const RowsetId& rowset_id) {
    std::string key = get_rowset_meta_key(tablet_uid, rowset_id);
    return meta->remove(META_COLUMN_FAMILY_INDEX, key);
}

string RowsetMetaManager::get_rowset_meta_key(const TabletUid& tablet_uid, const RowsetId& rowset_id) {
    return fmt::format("{}{}_{}", ROWSET_PREFIX, tablet_uid.to_string(), rowset_id.to_string());
}

Status RowsetMetaManager::traverse_rowset_metas(
        KVStore* meta, std::function<bool(const TabletUid&, const RowsetId&, std::string_view)> const& func) {
    auto traverse_rowset_meta_func = [&func](std::string_view key, std::string_view value) -> bool {
        // key format: rst_uuid_rowset_id
        std::vector<StringPiece> parts = strings::Split(StringPiece(key.data(), static_cast<int>(key.size())), "_");
        if (parts.size() != 3) {
            LOG(WARNING) << "invalid rowset key:" << key << ", splitted size:" << parts.size();
            return true;
        }
        RowsetId rowset_id;
        rowset_id.init(std::string_view(parts[2].data(), parts[2].size()));
        std::vector<StringPiece> uid_parts = strings::Split(parts[1], "-");
        if (uid_parts.size() != 2) {
            LOG(WARNING) << "invalid rowset key:" << key << ", uid splitted size:" << uid_parts.size();
            return true;
        }
        std::string_view p1(uid_parts[0].data(), uid_parts[0].size());
        std::string_view p2(uid_parts[1].data(), uid_parts[1].size());
        TabletUid tablet_uid(p1, p2);
        return func(tablet_uid, rowset_id, value);
    };
    return meta->iterate(META_COLUMN_FAMILY_INDEX, ROWSET_PREFIX, traverse_rowset_meta_func);
}

} // namespace starrocks
