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

#include <memory>
#include <vector>
#include <string>
#include <atomic>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

// Forward declarations
namespace starrocks {

class Status;
class RuntimeState;
class ObjectPool;
class TExpr;
class TTabletWriterInfo;
class TabletSchema;
class PartitionInfo;
class Rowset;
class DelVec;
class RowsetId;
class TxnLogPB;
class CompactionTask;
class Tablet;
class TableInfo;
class OlapTableSchemaParam;
class RowsetWriter;
class ColumnMapping;
class DeletePredicatePB;
class Chunk;


struct TCreateTabletReq;

}

namespace starrocks {

class RuntimeState;
class ObjectPool;
class TExpr;
class TTabletWriterInfo;
class TabletSchema;
class PartitionInfo;
class Rowset;
class DelVec;
class RowsetId;
class TxnLogPB;
class CompactionTask;
class Tablet;
class TableInfo;
class OlapTableSchemaParam;
class RowsetWriter;
class ColumnMapping;
class DeletePredicatePB;

namespace lake {

class TabletManager;
class UpdateManager;
class CompactionScheduler;
class DataCache;
class MetadataCache;
class GCExecutor;
class AsyncCleaner;

class Rowset;
class TabletSchema;

class DeltaWriter {
public:
    DeltaWriter();
    virtual ~DeltaWriter();

    virtual Status init() = 0;
    virtual Status write(const Chunk* chunk) = 0;
    virtual Status close() = 0;
    virtual uint64_t data_size() const = 0;
    virtual uint64_t num_rows() const = 0;
    virtual Status commit() = 0;

    // Additional methods for compatibility
    struct FlushStats {};
    struct WriterStats {};

    std::unique_ptr<FlushStats> get_flush_stats() const;
    std::unique_ptr<WriterStats> get_writer_stat() const;
    int64_t tablet_id() const;
    int64_t partition_id() const;
    bool is_immutable() const;
    int64_t last_write_ts() const;
    int64_t queueing_memtable_num() const;
    std::shared_ptr<const std::unordered_map<std::string, std::string>> global_dict_map() const;
    std::shared_ptr<const std::unordered_set<std::string>> global_dict_columns_valid_info() const;
};

class AsyncDeltaWriter {
public:
    AsyncDeltaWriter();
    ~AsyncDeltaWriter();
};

class TabletSchema {
public:
    TabletSchema();
    ~TabletSchema();

    int64_t id() const;
    int32_t num_columns() const;
    int32_t num_key_columns() const;
    int32_t num_short_key_columns() const;
    int32_t num_rows_per_row_block() const;
    bool is_in_memory() const;
};

class Tablet {
public:
    Tablet();
    ~Tablet();

    int64_t id() const;
    int32_t schema_version() const;
    const TabletSchema& schema() const;
    Status get_compaction_task(std::unique_ptr<CompactionTask>* task);
    Status publish_version(int64_t transaction_id, const std::vector<Rowset*>& rowsets,
                         int64_t version, const std::vector<DelVec*>* del_vecs = nullptr);
    std::string segment_location(std::string_view segment_name) const;
    std::string metadata_location(int64_t version) const;
};

class VersionedTablet {
public:
    VersionedTablet();
    ~VersionedTablet();

    int64_t tablet_id() const;
    int64_t version() const;
    const TabletSchema& schema() const;
    int64_t id() const;
    std::shared_ptr<const TabletSchema> get_schema() const;
    const std::vector<std::shared_ptr<Rowset>>& get_rowsets() const;
};

class TabletManager {
public:
    TabletManager();
    ~TabletManager();

    Status get_tablet(int64_t tablet_id, int64_t version, std::shared_ptr<Tablet>* tablet);
    Status get_tablet(int64_t tablet_id, std::shared_ptr<Tablet>* tablet);
    Status create_tablet(const TCreateTabletReq& req, const TabletSchema& tablet_schema);
    Status drop_tablet(int64_t tablet_id);
    std::string tablet_root_location(int64_t tablet_id) const;
    std::string segment_location(int64_t tablet_id, std::string_view segment_name) const;
};

class UpdateManager {
public:
    UpdateManager();
    ~UpdateManager();

    Status publish_version(int64_t tablet_id, int64_t version,
                         const std::vector<Rowset*>& rowsets,
                         const std::vector<DelVec*>* del_vecs = nullptr);
};

class CompactionScheduler {
public:
    CompactionScheduler();
    ~CompactionScheduler();

    Status schedule_compaction(std::shared_ptr<Tablet> tablet);
    Status update_tablet_compaction_status(int64_t tablet_id, int64_t signature, bool success);
};

class DataCache {
public:
    DataCache();
    ~DataCache();

    Status get(const std::string& key, std::string* value);
    Status put(const std::string& key, const std::string& value);
    Status remove(const std::string& key);
};

class MetadataCache {
public:
    MetadataCache();
    ~MetadataCache();

    Status get_tablet_schema(int64_t tablet_id, TabletSchema* schema);
    Status put_tablet_schema(int64_t tablet_id, const TabletSchema& schema);
    Status remove_tablet_schema(int64_t tablet_id);
};

class GCExecutor {
public:
    GCExecutor();
    ~GCExecutor();

    Status start();
    Status stop();
    Status do_gc();
};

class AsyncCleaner {
public:
    AsyncCleaner();
    ~AsyncCleaner();

    Status start();
    Status stop();
    Status schedule_clean(const std::string& path);
};

} // namespace lake
} // namespace starrocks
