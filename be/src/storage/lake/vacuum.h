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
#include <list>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/statusor.h"
#include "gen_cpp/lake_service.pb.h"

namespace starrocks {
class Status;
class FileSystem;
class DirEntry;
}

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
                                                                 int64_t expired_seconds, const std::list<std::string>& meta_files,
                                                                 const std::list<std::string>& bundle_meta_files,
                                                                 std::ostream* audit_ostream);

Status do_delete_files(FileSystem* fs, const std::vector<std::string>& paths);

// AsyncFileDeleter
// A class for asynchronously deleting files in batches.
//
// The AsyncFileDeleter class provides a mechanism to delete files in batches in an asynchronous manner.
// It allows specifying the batch size, which determines the number of files to be deleted in each batch.
class AsyncFileDeleter {
public:
    using DeleteCallback = std::function<void(const std::vector<std::string>&)>;

    explicit AsyncFileDeleter(int64_t batch_size) : _batch_size(batch_size) {}
    explicit AsyncFileDeleter(int64_t batch_size, DeleteCallback cb) : _batch_size(batch_size), _cb(std::move(cb)) {}
    virtual ~AsyncFileDeleter() = default;

    virtual Status delete_file(std::string path) {
        _batch.emplace_back(std::move(path));
        if (_batch.size() < _batch_size) {
            return Status::OK();
        }
        return submit(&_batch);
    }

    virtual Status finish() {
        if (!_batch.empty()) {
            RETURN_IF_ERROR(submit(&_batch));
        }
        return wait();
    }

    int64_t delete_count() const { return _delete_count; }

private:
    // Wait for all submitted deletion tasks to finish and return task execution results.
    Status wait() {
        if (_prev_task_status.valid()) {
            try {
                return _prev_task_status.get();
            } catch (const std::exception& e) {
                return Status::InternalError(e.what());
            }
        } else {
            return Status::OK();
        }
    }

    Status submit(std::vector<std::string>* files_to_delete) {
        // Await previous task completion before submitting a new deletion.
        RETURN_IF_ERROR(wait());
        _delete_count += files_to_delete->size();
        if (_cb) {
            _cb(*files_to_delete);
        }
        _prev_task_status = delete_files_callable(std::move(*files_to_delete));
        files_to_delete->clear();
        DCHECK(_prev_task_status.valid());
        return Status::OK();
    }

    int64_t _batch_size;
    int64_t _delete_count = 0;
    std::vector<std::string> _batch;
    std::future<Status> _prev_task_status;
    DeleteCallback _cb;
};

// Used for delete files which are shared by multiple tablets, so we can't delete them at first.
// We need to wait for all tablets to finish and decide whether to delete them.
class AsyncBundleFileDeleter : public AsyncFileDeleter {
public:
    explicit AsyncBundleFileDeleter(int64_t batch_size) : AsyncFileDeleter(batch_size) {}

    Status delete_file(std::string path) override {
        _pending_files[path]++;
        return Status::OK();
    }

    Status delay_delete(std::string path) {
        _delay_delete_files.insert(std::move(path));
        return Status::OK();
    }

    Status finish() override {
        for (auto& [path, count] : _pending_files) {
            if (_delay_delete_files.count(path) == 0) {
                if (config::lake_print_delete_log) {
                    LOG(INFO) << "Deleting bundle file: " << path << " ref count: " << count;
                }
                RETURN_IF_ERROR(AsyncFileDeleter::delete_file(path));
            }
        }
        return AsyncFileDeleter::finish();
    }

    bool is_empty() const { return _pending_files.empty(); }

private:
    // file to shared count.
    std::unordered_map<std::string, uint32_t> _pending_files;
    std::unordered_set<std::string> _delay_delete_files;
};

} // namespace starrocks::lake
