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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/olap_meta.h

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

#include <rocksdb/write_batch.h>

#include <functional>
#include <map>
#include <string>
#include <string_view>

#include "common/status.h"
#include "storage/olap_common.h"

namespace rocksdb {
class DB;
class ColumnFamilyHandle;
} // namespace rocksdb

namespace starrocks {

using ColumnFamilyHandle = rocksdb::ColumnFamilyHandle;
using WriteBatch = rocksdb::WriteBatch;

class KVStore {
public:
    explicit KVStore(std::string root_path);

    virtual ~KVStore();

    Status init(bool read_only = false);

    Status get(ColumnFamilyIndex column_family_index, const std::string& key, std::string* value);

    Status put(ColumnFamilyIndex column_family_index, const std::string& key, const std::string& value);

    Status write_batch(WriteBatch* batch);

    Status remove(ColumnFamilyIndex column_family_index, const std::string& key);

    Status iterate(ColumnFamilyIndex column_family_index, const std::string& prefix,
                   std::function<bool(std::string_view, std::string_view)> const& func);

    Status iterate_range(ColumnFamilyIndex column_family_index, const std::string& lower_bound,
                         const std::string& upper_bound,
                         std::function<bool(std::string_view, std::string_view)> const& func);

    Status compact();

    Status flush();

    std::string get_stats();

    bool get_live_sst_files_size(uint64_t* live_sst_files_size);

    std::string get_root_path();

    ColumnFamilyHandle* handle(ColumnFamilyIndex column_family_index) { return _handles[column_family_index]; }

private:
    std::string _root_path;
    rocksdb::DB* _db;
    std::vector<rocksdb::ColumnFamilyHandle*> _handles;
};

} // namespace starrocks
