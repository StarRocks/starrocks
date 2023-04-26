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

#pragma once

#include <string>
#include <string_view>

#include "storage/rowset/rowset_meta.h"

namespace starrocks {

class KVStore;

// Helper class for managing rowset meta of one root path.
class RowsetMetaManager {
public:
    static bool check_rowset_meta(KVStore* meta, const TabletUid& tablet_uid, const RowsetId& rowset_id);

    static Status save(KVStore* meta, const TabletUid& tablet_uid, const RowsetMetaPB& rowset_meta_pb);

    static Status flush(KVStore* meta);

    static Status remove(KVStore* meta, const TabletUid& tablet_uid, const RowsetId& rowset_id);

    static std::string get_rowset_meta_key(const TabletUid& tablet_uid, const RowsetId& rowset_id);

    static Status traverse_rowset_metas(
            KVStore* meta, std::function<bool(const TabletUid&, const RowsetId&, std::string_view)> const& func);
};

} // namespace starrocks
