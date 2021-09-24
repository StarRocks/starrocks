// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/memory/mem_tablet.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "storage/base_tablet.h"
#include "storage/memory/schema.h"

namespace starrocks {
namespace memory {

class MemSubTablet;
class ScanSpec;
class MemTabletScan;
class WriteTxn;

// Tablet class for memory-optimized storage engine.
//
// It stores all its data in-memory, and is designed for tables with
// frequent updates.
//
// By design, MemTablet stores all the schema versions together inside a single
// MemTablet, while olap/Tablet generate a new Tablet after schema change. so their
// behaviors are not compatible, we will address this issue latter after adding schema
// change support, currently MemTablet does not support schema change(only have single
// version of schema).
//
// TODO: will add more functionality as project evolves.
class MemTablet : public BaseTablet {
public:
    static std::shared_ptr<MemTablet> create_tablet_from_meta(MemTracker* mem_tracker,
                                                              const TabletMetaSharedPtr& tablet_meta,
                                                              DataDir* data_dir = nullptr);

    MemTablet(MemTracker* mem_tracker, const TabletMetaSharedPtr& tablet_meta, DataDir* data_dir);

    ~MemTablet() override;

    // Initialize
    Status init();

    // Scan the tablet, return a MemTabletScan object scan, user can specify projections
    // using ScanSpec, currently only support full scan with projection, will support
    // filter/aggregation in the future.
    //
    // Note: spec will be passed to scan object
    // Note: thread-safe, supports multi-reader concurrency.
    Status scan(std::unique_ptr<ScanSpec>* spec, std::unique_ptr<MemTabletScan>* scan);

    // Create a write transaction
    //
    // Note: Thread-safe, can have multiple writetxn at the same time.
    Status create_write_txn(std::unique_ptr<WriteTxn>* wtxn);

    // Apply a write transaction and commit as the specified version
    //
    // Note: commit is done sequentially, protected by internal write lock
    Status commit_write_txn(WriteTxn* wtxn, uint64_t version);

private:
    friend class MemTabletScan;
    // memory::Schema is used internally rather than TabletSchema, so we need an extra
    // copy of _schema with type memory::Schema.
    scoped_refptr<Schema> _mem_schema;

    // TODO: support multiple sub-tablets in the future
    std::unique_ptr<MemSubTablet> _sub_tablet;

    std::mutex _write_lock;

    std::atomic<uint64_t> _max_version;

    DISALLOW_COPY_AND_ASSIGN(MemTablet);
};

} // namespace memory
} // namespace starrocks
