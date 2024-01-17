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

#include <string>
#include <unordered_map>

#include "common/status.h"
#include "storage/chunk_iterator.h"
#include "storage/olap_common.h"
#include "storage/tablet_schema.h"

namespace starrocks {

class WritableFile;
class Tablet;
class SegmentWriter;
class PagePointerPB;
class PartialKVsPB;

/*
* PrimaryKeyDump is desgined for providing more information about primary key tablet when issue debug,
* such as `Tablet in error state` happen, data inconsistency and so on.
* PrimaryKeyDump will try to get all helpfull information and generate file [tablet_id].pkdump .
* In [tablet_id].pkdump, Data Layout:
*
* |  ------ Begin -------  |
* | Primary key columns (segment file format) - part 1 |
* | Primary key columns (segment file format) - part 2 |
* | Primary key columns (segment file format) - part 3 |
* | ... |
* | Primary Index kvs - part 1 |
* | Primary Index kvs - part 2 |
* | Primary Index kvs - part 3 |
* | ... |
* | ---- Serialze by Protobuf PrimaryKeyDumpPB --- |
* | TabletMetaPB |
* | RowsetMetaPB (rowset 1) |
* | RowsetMetaPB (rowset 2) |
* | ... |
* | RowsetStatPB (rowset 1) |
* | RowsetStatPB (rowset 2) |
* | ... |
* | DeltaColumnGroupListIdPB |
* | DeleteVectorStatPB |
* | PrimaryKeyColumnPB (Use PagePointerPB point to columns) |
* | PrimaryIndexMultiLevelPB (Use PagePointerPB point to real kvs) |
* | ---------- end ----------|
*
* I seperate the real primary key column data and primary index data from PrimaryKeyDumpPB,
* because I want to control the peak memory usage when dump and Protobuf also can't support
* too large data (better less than 2GB).
*/
class PrimaryKeyDump {
public:
    PrimaryKeyDump(Tablet* tablet);
    // for UT only
    PrimaryKeyDump(const std::string& dump_filepath);
    ~PrimaryKeyDump() = default;

    Status dump();

    Status init_dump_file();

    // Append primary index' kv into dump file
    Status add_pindex_kvs(const std::string_view& key, uint64_t value, PrimaryIndexDumpPB* dump_pb);
    Status finish_pindex_kvs(PrimaryIndexDumpPB* dump_pb);

    // read PrimaryKeyDumpPB from dump file and deserialize it.
    static Status read_deserialize_from_file(const std::string& dump_filepath, PrimaryKeyDumpPB* dump_pb);

    // deserialize pk column and pk index from dump file.
    static Status deserialize_pkcol_pkindex_from_meta(
            const std::string& dump_filepath, const PrimaryKeyDumpPB& dump_pb,
            const std::function<void(const Chunk&)>& column_key_func,
            const std::function<void(const std::string&, const PartialKVsPB&)>& index_kvs_func);

private:
    Status _dump_tablet_meta();
    Status _dump_rowset_meta();
    Status _dump_primary_index();
    Status _dump_delvec();
    Status _dump_dcg();
    Status _dump_segment_keys();
    Status _dump_rowset_stat();
    Status _dump_to_file();

    // 2GB
    static const int64_t MAX_PROTOBUF_SIZE = 2147483648;

private:
    Tablet* _tablet = nullptr;
    std::string _dump_filepath;
    PrimaryKeyDumpPB _dump_pb;

    // Store pk index kvs
    std::unique_ptr<PartialKVsPB> _partial_pindex_kvs;
    int64_t _partial_pindex_kvs_bytes = 0;
    std::unique_ptr<WritableFile> _dump_wfile;
};

} // namespace starrocks