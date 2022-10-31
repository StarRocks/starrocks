// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
    ClusterIdMgr(std::string  path);
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
