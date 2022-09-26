// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <map>
#include <string>
#include <vector>

#include "common/status.h"
#include "runtime/client_cache.h"

namespace starrocks {

namespace lake {
class UploadSnapshotsRequest;
class RestoreSnapshotsRequest;
} // namespace lake

class ExecEnv;
struct FileStat;

class LakeSnapshotLoader {
public:
    explicit LakeSnapshotLoader(ExecEnv* env);

    ~LakeSnapshotLoader() = default;

    DISALLOW_COPY_AND_MOVE(LakeSnapshotLoader);

    Status upload(const ::starrocks::lake::UploadSnapshotsRequest* request);

    Status restore(const ::starrocks::lake::RestoreSnapshotsRequest* request);

private:
    Status _get_existing_files_from_remote(BrokerServiceConnection& client, const std::string& remote_path,
                                           const std::map<std::string, std::string>& broker_prop,
                                           std::map<std::string, FileStat>* files);

    Status _rename_remote_file(BrokerServiceConnection& client, const std::string& orig_name,
                               const std::string& new_name, const std::map<std::string, std::string>& broker_prop);

    Status _check_snapshot_paths(const ::starrocks::lake::UploadSnapshotsRequest* request);

private:
    ExecEnv* _env;
};

} // end namespace starrocks
