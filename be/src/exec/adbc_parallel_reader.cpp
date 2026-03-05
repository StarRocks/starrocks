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

#include "exec/adbc_parallel_reader.h"

#include <arrow/c/bridge.h>
#include <glog/logging.h>

namespace starrocks {

// ================================
// RecordBatchQueue
// ================================

RecordBatchQueue::RecordBatchQueue(size_t capacity) : _capacity(capacity) {}

void RecordBatchQueue::push(std::shared_ptr<arrow::RecordBatch> batch) {
    std::unique_lock<std::mutex> lock(_mutex);
    _not_full.wait(lock, [this] { return _queue.size() < _capacity || _has_error; });
    if (_has_error) return;
    _queue.push(std::move(batch));
    _not_empty.notify_one();
}

std::shared_ptr<arrow::RecordBatch> RecordBatchQueue::pop() {
    std::unique_lock<std::mutex> lock(_mutex);
    _not_empty.wait(lock, [this] { return !_queue.empty() || _active_producers == 0 || _has_error; });
    if (_has_error || (_queue.empty() && _active_producers == 0)) {
        return nullptr;
    }
    auto batch = std::move(_queue.front());
    _queue.pop();
    _not_full.notify_one();
    return batch;
}

void RecordBatchQueue::producer_done() {
    std::lock_guard<std::mutex> lock(_mutex);
    _active_producers--;
    if (_active_producers == 0) {
        _not_empty.notify_all();
    }
}

void RecordBatchQueue::add_producer() {
    std::lock_guard<std::mutex> lock(_mutex);
    _active_producers++;
}

void RecordBatchQueue::set_error(Status status) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (!_has_error) {
        _error = std::move(status);
        _has_error = true;
    }
    // Wake up anyone waiting
    _not_full.notify_all();
    _not_empty.notify_all();
}

Status RecordBatchQueue::error() const {
    std::lock_guard<std::mutex> lock(_mutex);
    if (_has_error) {
        return _error;
    }
    return Status::OK();
}

// ================================
// ADBCParallelReader
// ================================

ADBCParallelReader::ADBCParallelReader(AdbcDatabase* database, const AdbcPartitions& partitions, size_t num_threads)
        : _database(database), _num_threads(num_threads), _queue(16) {
    for (size_t i = 0; i < partitions.num_partitions; i++) {
        _partitions.emplace_back(partitions.partitions[i], partitions.partition_lengths[i]);
    }
}

ADBCParallelReader::~ADBCParallelReader() {
    close();
}

Status ADBCParallelReader::start() {
    if (_started) return Status::OK();
    _started = true;

    // Cap threads at num_threads. Assign partitions round-robin across threads.
    size_t actual_threads = std::min(_partitions.size(), _num_threads);

    // Build per-thread partition assignments
    std::vector<std::vector<size_t>> assignments(actual_threads);
    for (size_t i = 0; i < _partitions.size(); i++) {
        assignments[i % actual_threads].push_back(i);
    }

    // One producer per thread
    for (size_t t = 0; t < actual_threads; t++) {
        _queue.add_producer();
        _threads.emplace_back([this, my_parts = std::move(assignments[t])]() {
            for (size_t idx : my_parts) {
                // Each partition gets its own connection (connection is not thread-safe)
                AdbcConnection conn{};
                AdbcError error = ADBC_ERROR_INIT;

                AdbcStatusCode sc = AdbcConnectionNew(&conn, &error);
                if (sc != ADBC_STATUS_OK) {
                    std::string msg = error.message ? error.message : "Unknown ADBC error";
                    if (error.release) error.release(&error);
                    _queue.set_error(Status::InternalError("ADBC parallel reader: " + msg));
                    break;
                }

                sc = AdbcConnectionInit(&conn, _database, &error);
                if (sc != ADBC_STATUS_OK) {
                    std::string msg = error.message ? error.message : "Unknown ADBC error";
                    if (error.release) error.release(&error);
                    AdbcError re = ADBC_ERROR_INIT;
                    AdbcConnectionRelease(&conn, &re);
                    if (re.release) re.release(&re);
                    _queue.set_error(Status::InternalError("ADBC parallel reader: " + msg));
                    break;
                }

                struct ArrowArrayStream c_stream {};
                sc = AdbcConnectionReadPartition(&conn, _partitions[idx].first, _partitions[idx].second,
                                                 &c_stream, &error);
                if (sc != ADBC_STATUS_OK) {
                    std::string msg = error.message ? error.message : "Unknown ADBC error";
                    if (error.release) error.release(&error);
                    AdbcError re = ADBC_ERROR_INIT;
                    AdbcConnectionRelease(&conn, &re);
                    if (re.release) re.release(&re);
                    _queue.set_error(Status::InternalError("ADBC parallel reader: " + msg));
                    break;
                }

                auto import_result = arrow::ImportRecordBatchReader(&c_stream);
                if (!import_result.ok()) {
                    AdbcError re = ADBC_ERROR_INIT;
                    AdbcConnectionRelease(&conn, &re);
                    if (re.release) re.release(&re);
                    _queue.set_error(
                            Status::InternalError("ADBC parallel reader: " + import_result.status().ToString()));
                    break;
                }

                auto reader = std::move(import_result).ValueUnsafe();
                bool read_error = false;
                while (true) {
                    std::shared_ptr<arrow::RecordBatch> batch;
                    auto read_status = reader->ReadNext(&batch);
                    if (!read_status.ok()) {
                        _queue.set_error(
                                Status::InternalError("ADBC parallel reader: " + read_status.ToString()));
                        read_error = true;
                        break;
                    }
                    if (!batch) break; // end of stream
                    _queue.push(std::move(batch));
                }

                // Release connection
                AdbcError re = ADBC_ERROR_INIT;
                AdbcConnectionRelease(&conn, &re);
                if (re.release) re.release(&re);

                if (read_error) break;
            }
            _queue.producer_done();
        });
    }

    return Status::OK();
}

Status ADBCParallelReader::get_next(std::shared_ptr<arrow::RecordBatch>* batch, bool* eos) {
    RETURN_IF_ERROR(_queue.error());

    auto result = _queue.pop();
    if (!result) {
        RETURN_IF_ERROR(_queue.error());
        *eos = true;
        return Status::OK();
    }

    *batch = std::move(result);
    *eos = false;
    return Status::OK();
}

void ADBCParallelReader::close() {
    if (_closed) return;
    _closed = true;

    // Signal error to unblock any waiting threads
    _queue.set_error(Status::Cancelled("Reader closed"));

    for (auto& t : _threads) {
        if (t.joinable()) {
            t.join();
        }
    }
    _threads.clear();
}

void ADBCParallelReader::_reader_thread(const uint8_t* /*partition_data*/, size_t /*partition_length*/) {
    // Not used directly -- parallel reading is implemented via lambdas in start().
    // Kept for interface compatibility.
}

} // namespace starrocks
