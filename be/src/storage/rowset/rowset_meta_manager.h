// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/rowset_meta_manager.h

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

#ifndef STARROCKS_BE_SRC_OLAP_ROWSET_ROWSET_META_MANAGER_H
#define STARROCKS_BE_SRC_OLAP_ROWSET_ROWSET_META_MANAGER_H

#include <string>

#include "storage/kv_store.h"
#include "storage/rowset/rowset_meta.h"

using std::string;

namespace starrocks {

// Helper class for managing rowset meta of one root path.
class RowsetMetaManager {
public:
    static bool check_rowset_meta(KVStore* meta, const TabletUid& tablet_uid, const RowsetId& rowset_id);

    static Status save(KVStore* meta, const TabletUid& tablet_uid, const RowsetId& rowset_id,
                       const RowsetMetaPB& rowset_meta_pb);

    static Status remove(KVStore* meta, const TabletUid& tablet_uid, const RowsetId& rowset_id);

    static string get_rowset_meta_key(const TabletUid& tablet_uid, const RowsetId& rowset_id);

    static Status traverse_rowset_metas(
            KVStore* meta, std::function<bool(const TabletUid&, const RowsetId&, const std::string&)> const& func);
};

} // namespace starrocks

#endif // STARROCKS_BE_SRC_OLAP_ROWSET_ROWSET_META_MANAGER_H
