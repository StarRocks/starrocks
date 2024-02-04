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

#include <future>
#include <string>
#include <vector>

#include "gen_cpp/lake_service.pb.h"

namespace starrocks {
class Status;
}

namespace starrocks::lake {

class TabletManager;

void vacuum(TabletManager* tablet_mgr, const VacuumRequest& request, VacuumResponse* response);

void vacuum_full(TabletManager* tablet_mgr, const VacuumFullRequest& request, VacuumFullResponse* response);

// REQUIRES:
//  - tablet_mgr != NULL
//  - request.tablet_ids_size() > 0
//  - response != NULL
void delete_tablets(TabletManager* tablet_mgr, const DeleteTabletRequest& request, DeleteTabletResponse* response);

// Batch delete files.
// REQUIRE: All files in |paths| have the same file system scheme.
Status delete_files(const std::vector<std::string>& paths);

// An Async wrapper for delete_files that queues the request into a thread executor.
void delete_files_async(std::vector<std::string> files_to_delete);

// A Callable wrapper for delete_files that returns a future to the operation so that
// it can be executed in parallel to other requests
std::future<Status> delete_files_callable(std::vector<std::string> files_to_delete);

// Run a clear task async
void run_clear_task_async(std::function<void()> task);

Status datafile_gc(std::string_view root_location, std::string_view audit_file_path, int64_t expired_seconds = 86400,
                   bool do_delete = false);

} // namespace starrocks::lake
