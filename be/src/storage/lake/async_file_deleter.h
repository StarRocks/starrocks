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
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/config.h"
#include "common/status.h"

namespace starrocks::lake {
std::future<Status> delete_files_callable(std::vector<std::string> files_to_delete);

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