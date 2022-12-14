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
