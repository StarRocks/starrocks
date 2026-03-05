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

#include <arrow-adbc/adbc.h>
#include <arrow/record_batch.h>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "common/status.h"

namespace starrocks {

// Thread-safe bounded queue for RecordBatch transfer between reader threads and consumer.
class RecordBatchQueue {
public:
    explicit RecordBatchQueue(size_t capacity = 16);

    // Push a batch (blocks if full).
    void push(std::shared_ptr<arrow::RecordBatch> batch);

    // Pop a batch (blocks if empty). Returns nullptr when all producers are done and queue is empty.
    std::shared_ptr<arrow::RecordBatch> pop();

    // Signal that a producer is done.
    void producer_done();

    // Increment the number of active producers.
    void add_producer();

    void set_error(Status status);
    Status error() const;

private:
    std::queue<std::shared_ptr<arrow::RecordBatch>> _queue;
    mutable std::mutex _mutex;
    std::condition_variable _not_full;
    std::condition_variable _not_empty;
    size_t _capacity;
    int _active_producers{0};
    Status _error;
    bool _has_error = false;
};

// Reads multiple ADBC partitions in parallel, each on its own thread.
class ADBCParallelReader {
public:
    ADBCParallelReader(AdbcDatabase* database, const AdbcPartitions& partitions, size_t num_threads);
    ~ADBCParallelReader();

    Status start();

    // Returns next batch from any endpoint. Sets *eos=true when all done.
    Status get_next(std::shared_ptr<arrow::RecordBatch>* batch, bool* eos);

    void close();

private:
    void _reader_thread(const uint8_t* partition_data, size_t partition_length);

    AdbcDatabase* _database; // borrowed, not owned
    std::vector<std::pair<const uint8_t*, size_t>> _partitions;
    size_t _num_threads;
    RecordBatchQueue _queue;
    std::vector<std::thread> _threads;
    bool _started = false;
    bool _closed = false;
};

} // namespace starrocks
