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
        _total_queued++;
        _batch.emplace_back(std::move(path));
        if (_batch.size() < static_cast<size_t>(_batch_size)) {
            return Status::OK();
        }
        return submit(&_batch);
    }

    virtual Status finish() {
        if (!_batch.empty()) {
            RETURN_IF_ERROR(submit(&_batch));
        }
        return wait_and_account();
    }

    // Total files submitted for deletion (incremented at submit time, not on success).
    // Semantics unchanged from the original _delete_count.
    int64_t delete_count() const { return _delete_count; }

    // Total files ever passed to delete_file(), including those still in the pending batch.
    // Callers can snapshot this after each version to build a cumulative-count map, which
    // is then compared against success_delete_count() to determine which versions had all
    // their data files confirmed deleted after a partial failure.
    int64_t total_queued() const { return _total_queued; }

    // Files whose async delete batch completed successfully.
    // Unlike delete_count(), this excludes the last batch if it failed.
    int64_t success_delete_count() const { return _success_delete_count; }

private:
    // Wait for the in-flight batch; credit its size to _success_delete_count only on success.
    Status wait_and_account() {
        if (!_prev_task_status.valid()) {
            return Status::OK();
        }
        Status st;
        try {
            st = _prev_task_status.get();
        } catch (const std::exception& e) {
            st = Status::InternalError(e.what());
        }
        if (st.ok()) {
            _success_delete_count += _inflight_size;
        }
        _inflight_size = 0;
        return st;
    }

    Status submit(std::vector<std::string>* files_to_delete) {
        // Confirm the previous batch (and credit it to _success_delete_count if OK)
        // before dispatching the next one.
        RETURN_IF_ERROR(wait_and_account());
        _delete_count += files_to_delete->size();
        _inflight_size = files_to_delete->size();
        if (_cb) {
            _cb(*files_to_delete);
        }
        _prev_task_status = delete_files_callable(std::move(*files_to_delete));
        files_to_delete->clear();
        DCHECK(_prev_task_status.valid());
        return Status::OK();
    }

    int64_t _batch_size;
    int64_t _delete_count = 0;         // submitted count (original semantics, unchanged)
    int64_t _success_delete_count = 0; // confirmed-deleted count (only on batch success)
    int64_t _total_queued = 0;         // total files ever passed to delete_file()
    int64_t _inflight_size = 0;        // size of the currently in-flight async batch
    std::vector<std::string> _batch;
    std::future<Status> _prev_task_status;
    DeleteCallback _cb;
};

// Used for deleting files that may be shared by multiple tablets. We first collect
// deletion candidates, then delay-delete files still referenced by alive metadata.
class AsyncSharedFileDeleter : public AsyncFileDeleter {
public:
    explicit AsyncSharedFileDeleter(int64_t batch_size) : AsyncFileDeleter(batch_size) {}

    Status delete_file(std::string path) override {
        _pending_files[std::move(path)]++;
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
                    LOG(INFO) << "Deleting shared file: " << path << " ref count: " << count;
                }
                RETURN_IF_ERROR(AsyncFileDeleter::delete_file(path));
            }
        }
        return AsyncFileDeleter::finish();
    }

    bool is_empty() const { return _pending_files.empty(); }

    void clear() {
        _pending_files.clear();
        _delay_delete_files.clear();
    }

private:
    // file to shared count.
    std::unordered_map<std::string, uint32_t> _pending_files;
    std::unordered_set<std::string> _delay_delete_files;
};

} // namespace starrocks::lake
