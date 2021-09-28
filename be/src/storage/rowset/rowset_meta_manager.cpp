// This file is made available under Elastic License 2.0.
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

#include <boost/algorithm/string/trim.hpp>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "common/logging.h"
#include "common/status.h"
#include "gutil/strings/split.h"
#include "json2pb/json_to_pb.h"
#include "json2pb/pb_to_json.h"
#include "storage/olap_define.h"
#include "storage/storage_engine.h"

namespace starrocks {

const std::string ROWSET_PREFIX = "rst_";

bool RowsetMetaManager::check_rowset_meta(KVStore* meta, const TabletUid& tablet_uid, const RowsetId& rowset_id) {
    std::string key = ROWSET_PREFIX + tablet_uid.to_string() + "_" + rowset_id.to_string();
    std::string value;
    Status s = meta->get(META_COLUMN_FAMILY_INDEX, key, &value);
    return s.ok();
}

Status RowsetMetaManager::save(KVStore* meta, const TabletUid& tablet_uid, const RowsetId& rowset_id,
                               const RowsetMetaPB& rowset_meta_pb) {
    std::string key = ROWSET_PREFIX + tablet_uid.to_string() + "_" + rowset_id.to_string();
    std::string value;
    bool ret = rowset_meta_pb.SerializeToString(&value);
    if (!ret) {
        std::string error_msg = "serialize rowset pb failed. rowset id:" + key;
        LOG(WARNING) << error_msg;
        return Status::InternalError("fail to serialize rowset meta");
    }
    return meta->put(META_COLUMN_FAMILY_INDEX, key, value);
}

Status RowsetMetaManager::remove(KVStore* meta, const TabletUid& tablet_uid, const RowsetId& rowset_id) {
    std::string key = ROWSET_PREFIX + tablet_uid.to_string() + "_" + rowset_id.to_string();
    return meta->remove(META_COLUMN_FAMILY_INDEX, key);
}

string RowsetMetaManager::get_rowset_meta_key(const TabletUid& tablet_uid, const RowsetId& rowset_id) {
    return ROWSET_PREFIX + tablet_uid.to_string() + "_" + rowset_id.to_string();
}

Status RowsetMetaManager::traverse_rowset_metas(
        KVStore* meta, std::function<bool(const TabletUid&, const RowsetId&, const std::string&)> const& func) {
    auto traverse_rowset_meta_func = [&func](std::string_view key, std::string_view value) -> bool {
        std::string key_str(key);
        std::string value_str(value);
        // key format: rst_uuid_rowset_id
        std::vector<std::string> parts = strings::Split(key_str, "_");
        if (parts.size() != 3) {
            LOG(WARNING) << "invalid rowset key:" << key << ", splitted size:" << parts.size();
            return true;
        }
        RowsetId rowset_id;
        rowset_id.init(parts[2]);
        std::vector<std::string> uid_parts = strings::Split(parts[1], "-");
        TabletUid tablet_uid(uid_parts[0], uid_parts[1]);
        return func(tablet_uid, rowset_id, value_str);
    };
    return meta->iterate(META_COLUMN_FAMILY_INDEX, ROWSET_PREFIX, traverse_rowset_meta_func);
}

} // namespace starrocks
