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

#include <cstdint>

#include "common/status.h"
#include "storage/olap_define.h"

namespace starrocks {

// ClusterIdMgr will keep the cluster id from FE.
// it will add a version suffix after id.
// Cluster id will be checked while staring to avoid wrong data directory.
class ClusterIdMgr {
public:
    ClusterIdMgr(std::string path);
    Status init();
    Status set_cluster_id(int32_t cluster_id);
    int32_t cluster_id() const { return _cluster_id; }

private:
    int32_t _cluster_id = -1;
    std::string _path;
    std::string _cluster_id_path() const { return _path + CLUSTER_ID_PREFIX; }
    Status _read_cluster_id(const std::string& path, int32_t* cluster_id);
    Status _add_version_info_to_cluster_id(const std::string& path);
};

} // namespace starrocks
