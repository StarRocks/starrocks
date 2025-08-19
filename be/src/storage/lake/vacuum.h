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

#include <list>

#include "common/config.h"
#include "common/statusor.h"
#include "gen_cpp/lake_service.pb.h"
#include "storage/lake/async_file_deleter.h"

namespace starrocks {
class Status;
class FileSystem;
class DirEntry;
} // namespace starrocks

namespace starrocks::lake {

class TabletManager;

void vacuum(TabletManager* tablet_mgr, const VacuumRequest& request, VacuumResponse* response);

// REQUIRES:
//  - tablet_mgr != NULL
//  - request.tablet_ids_size() > 0
//  - response != NULL
void delete_tablets(TabletManager* tablet_mgr, const DeleteTabletRequest& request, DeleteTabletResponse* response);

void delete_txn_log(TabletManager* tablet_mgr, const DeleteTxnLogRequest& request, DeleteTxnLogResponse* response);

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

StatusOr<int64_t> datafile_gc(std::string_view root_location, std::string_view audit_file_path,
                              int64_t expired_seconds = 86400, bool do_delete = false);

// Check if there are any garbage files in the given root location.
// Returns the number of garbage files found.
StatusOr<int64_t> garbage_file_check(std::string_view root_location);

Status vacuum_txn_log(std::string_view root_location, int64_t min_active_txn_id, int64_t* vacuumed_files,
                      int64_t* vacuumed_file_size);

StatusOr<std::pair<std::list<std::string>, std::list<std::string>>> list_meta_files(
        FileSystem* fs, const std::string& metadata_root_location);

StatusOr<std::map<std::string, DirEntry>> find_orphan_data_files(FileSystem* fs, std::string_view root_location,
                                                                 int64_t expired_seconds,
                                                                 const std::list<std::string>& meta_files,
                                                                 const std::list<std::string>& bundle_meta_files,
                                                                 std::ostream* audit_ostream);

Status do_delete_files(FileSystem* fs, const std::vector<std::string>& paths);

} // namespace starrocks::lake
