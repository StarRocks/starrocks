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

#include "storage/table_reader.h"

#include <algorithm>
#include <queue>

#include "exec/tablet_info.h"
#include "gen_cpp/doris_internal_service.pb.h"
#include "serde/protobuf_serde.h"
#include "storage/local_tablet_reader.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_reader.h"
#include "util/brpc_stub_cache.h"
#include "util/ref_count_closure.h"

namespace starrocks {

TableReader::TableReader() = default;

TableReader::~TableReader() = default;

Status TableReader::init(const LocalTableReaderParams& local_params) {
    if (_local_params || _params) {
        return Status::InternalError("TableReader already initialized");
    }
    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(local_params.tablet_id);
    if (tablet == nullptr) {
        return Status::InternalError(strings::Substitute("tablet $0 not found", local_params.tablet_id));
    }
    auto local_tablet_reader = std::make_unique<LocalTabletReader>();
    RETURN_IF_ERROR(local_tablet_reader->init(tablet, local_params.version));
    _local_params = std::make_unique<LocalTableReaderParams>(local_params);
    _local_tablet_reader = std::move(local_tablet_reader);
    return Status::OK();
}

Status TableReader::init(const TableReaderParams& params) {
    if (_local_params || _params) {
        return Status::InternalError("TableReader already initialized");
    }
    _params = std::make_unique<TableReaderParams>(params);
    _schema_param = std::make_shared<OlapTableSchemaParam>();
    RETURN_IF_ERROR(_schema_param->init(params.schema));
    _partition_param = std::make_unique<OlapTablePartitionParam>(_schema_param, params.partition_param);
    // currently RuntimeState and partition expr is not supported
    // so prepare/open/close is not called
    RETURN_IF_ERROR(_partition_param->init(nullptr));
    _location_param = std::make_unique<OlapTableLocationParam>(params.location_param);
    _nodes_info = std::make_unique<StarRocksNodesInfo>(params.nodes_info);
    _row_desc = std::make_unique<RowDescriptor>(_schema_param->tuple_desc(), false);
    return Status::OK();
}

struct TabletMultiGet {
    int64_t tablet_id{0};
    int64_t version{0};
    std::shared_ptr<Chunk> keys;
    std::vector<uint32_t> orig_idxs;

    std::unique_ptr<Chunk> values;
    std::vector<uint32_t> found_idxs;
    size_t num_rows{0};
    size_t cur_row{0};

    void add(Chunk& keys, size_t idx, uint32_t orig_idx) {
        this->keys->append(keys, idx, 1);
        orig_idxs.push_back(orig_idx);
    }

    bool operator<(const TabletMultiGet& other) const { return found_idxs[cur_row] < other.found_idxs[other.cur_row]; }

    bool empty() { return cur_row >= num_rows; }

    Status pop_value_to(Chunk& dest) {
        if (cur_row >= num_rows) {
            return Status::InternalError("pop value from empty TabletMultiGet");
        }
        dest.append(*this->values, cur_row, 1);
        ++cur_row;
        return Status::OK();
    }
};

struct TabletMultiGetPtrCmp {
    bool operator()(const TabletMultiGet* lhs, const TabletMultiGet* rhs) const { return *rhs < *lhs; }
};

Status TableReader::multi_get(Chunk& keys, const std::vector<std::string>& value_columns, std::vector<bool>& found,
                              Chunk& values) {
    if (_local_tablet_reader) {
        return _local_tablet_reader->multi_get(keys, value_columns, found, values);
    }
    size_t num_rows = keys.num_rows();
    found.assign(num_rows, false);
    std::vector<OlapTablePartition*> partitions;
    std::vector<uint32_t> tablet_indexes;
    std::vector<uint8_t> validate_selection;
    std::vector<uint32_t> validate_select_idx;
    validate_selection.assign(num_rows, 1);
    RETURN_IF_ERROR(_partition_param->find_tablets(&keys, &partitions, &tablet_indexes, &validate_selection, nullptr, 0,
                                                   nullptr));
    // Arrange selection_idx by merging _validate_selection
    // If chunk num_rows is 6
    // _validate_selection is [1, 0, 0, 0, 1, 1]
    // selection_idx after arrange will be : [0, 4, 5]
    validate_select_idx.resize(num_rows);
    size_t selected_size = 0;
    for (uint16_t i = 0; i < num_rows; ++i) {
        validate_select_idx[selected_size] = i;
        selected_size += (validate_selection[i] & 0x1);
    }
    validate_select_idx.resize(selected_size);
    if (selected_size == 0) {
        return Status::OK();
    }
    std::unordered_map<uint64_t, std::unique_ptr<TabletMultiGet>> multi_gets_by_tablet;
    for (size_t i = 0; i < selected_size; ++i) {
        size_t key_index = validate_select_idx[i];
        int64_t tablet_id = partitions[key_index]->indexes[0].tablets[tablet_indexes[key_index]];
        auto iter = multi_gets_by_tablet.find(tablet_id);
        TabletMultiGet* multi_get = nullptr;
        if (iter == multi_gets_by_tablet.end()) {
            multi_gets_by_tablet[tablet_id] = std::make_unique<TabletMultiGet>();
            multi_get = multi_gets_by_tablet[tablet_id].get();
            multi_get->tablet_id = tablet_id;
            auto partition_id = partitions[key_index]->id;
            auto itr = _params->partition_versions.find(partition_id);
            if (itr == _params->partition_versions.end()) {
                return Status::InternalError(strings::Substitute(
                        "partition version not found: partition:$0 tablet:$1 not found", partition_id, tablet_id));
            }
            multi_get->version = itr->second;
            multi_get->keys = keys.clone_empty();
        } else {
            multi_get = iter->second.get();
        }
        multi_get->add(keys, key_index, key_index);
    }
    vector<TabletMultiGet*> multi_gets;
    for (auto& iter : multi_gets_by_tablet) {
        multi_gets.push_back(iter.second.get());
    }
    // TODO: parallel get
    // a cache var to init chunk_meta only once
    SchemaPtr& value_schema = values.schema();
    for (auto multi_get : multi_gets) {
        multi_get->values = values.clone_empty();
        vector<bool> found;
        RETURN_IF_ERROR(_tablet_multi_get(multi_get->tablet_id, multi_get->version, *multi_get->keys, value_columns,
                                          found, *multi_get->values, value_schema));
        multi_get->found_idxs.clear();
        DCHECK(multi_get->keys->num_rows() == found.size());
        for (size_t i = 0; i < found.size(); ++i) {
            if (found[i]) {
                multi_get->found_idxs.push_back(multi_get->orig_idxs[i]);
            }
        }
        multi_get->num_rows = multi_get->found_idxs.size();
    }
    std::priority_queue<TabletMultiGet*, std::vector<TabletMultiGet*>, TabletMultiGetPtrCmp> pq;
    for (auto multi_get : multi_gets) {
        if (!multi_get->empty()) {
            pq.push(multi_get);
        }
    }
    while (!pq.empty()) {
        TabletMultiGet* multi_get = pq.top();
        pq.pop();
        size_t idx = multi_get->found_idxs[multi_get->cur_row];
        found[idx] = true;
        RETURN_IF_ERROR(multi_get->pop_value_to(values));
        if (!multi_get->empty()) {
            pq.push(multi_get);
        }
    }
    return Status::OK();
}

Status TableReader::_tablet_multi_get(int64_t tablet_id, int64_t version, Chunk& keys,
                                      const std::vector<std::string>& value_columns, std::vector<bool>& found,
                                      Chunk& values, SchemaPtr& value_schema) {
    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
    // get from local tablet
    // TODO: if local tablet's version is missing, switch to remote replica
    if (tablet != nullptr) {
        auto local_tablet_reader = std::make_unique<LocalTabletReader>();
        RETURN_IF_ERROR(local_tablet_reader->init(tablet, version));
        RETURN_IF_ERROR(local_tablet_reader->multi_get(keys, value_columns, found, values));
        return Status::OK();
    }
    return _tablet_multi_get_remote(tablet_id, version, keys, value_columns, found, values, value_schema);
}

Status TableReader::_tablet_multi_get_remote(int64_t tablet_id, int64_t version, Chunk& keys,
                                             const std::vector<std::string>& value_columns, std::vector<bool>& found,
                                             Chunk& values, SchemaPtr& value_schema) {
    auto location = _location_param->find_tablet(tablet_id);
    if (location == nullptr) {
        return Status::InternalError(strings::Substitute("tablet $0 not found in OlapTableLocationParam", tablet_id));
    }
    Status st;
    for (int64_t node_id : location->node_ids) {
        auto node_info = _nodes_info->find_node(node_id);
        if (node_info == nullptr) {
            string msg = strings::Substitute("multi_get fail: be $0 not found tablet:$1", node_id, tablet_id);
            LOG(WARNING) << msg;
            st = Status::InternalError(msg);
        } else {
            PInternalService_Stub* stub =
                    ExecEnv::GetInstance()->brpc_stub_cache()->get_stub(node_info->host, node_info->brpc_port);
            if (stub == nullptr) {
                string msg = strings::Substitute("multi_get fail to get brpc stub for $0:$1 tablet:$2", node_info->host,
                                                 node_info->brpc_port, tablet_id);
                LOG(WARNING) << msg;
                st = Status::InternalError(msg);
            } else {
                st = _tablet_multi_get_rpc(stub, tablet_id, version, keys, value_columns, found, values, value_schema);
                if (st.ok()) {
                    break;
                }
            }
        }
    }
    return st;
}

Status TableReader::_tablet_multi_get_rpc(PInternalService_Stub* stub, int64_t tablet_id, int64_t version, Chunk& keys,
                                          const std::vector<std::string>& value_columns, std::vector<bool>& found,
                                          Chunk& values, SchemaPtr& value_schema) {
    PTabletReaderMultiGetRequest request;
    request.set_tablet_id(tablet_id);
    request.set_version(version);
    for (const auto& value_column : value_columns) {
        request.add_values_columns(value_column);
    }
    StatusOr<ChunkPB> keys_pb;
    TRY_CATCH_BAD_ALLOC(keys_pb = serde::ProtobufChunkSerde::serialize(keys, nullptr));
    RETURN_IF_ERROR(keys_pb);
    request.mutable_keys()->Swap(&keys_pb.value());
    RefCountClosure<PTabletReaderMultiGetResult>* closure = new RefCountClosure<PTabletReaderMultiGetResult>();
    closure->ref();
    DeferOp op([&]() {
        if (closure->unref()) {
            delete closure;
            closure = nullptr;
        }
    });
    if (_params->timeout_ms > 0) {
        closure->cntl.set_timeout_ms(_params->timeout_ms);
    }
    stub->local_tablet_reader_multi_get(&closure->cntl, &request, &closure->result, closure);
    closure->join();
    if (closure->cntl.Failed()) {
        return Status::InternalError(closure->cntl.ErrorText());
    }
    auto& result = closure->result;
    Status st = result.status();
    if (!st.ok()) {
        return st;
    }
    found.resize(result.found().size());
    for (size_t i = 0; i < result.found().size(); ++i) {
        found[i] = result.found(i);
    }
    auto& values_pb = result.values();
    TRY_CATCH_BAD_ALLOC({
        StatusOr<Chunk> res = serde::deserialize_chunk_pb_with_schema(*value_schema, values_pb.data());
        if (!res.ok()) return res.status();
        values.columns().swap(res.value().columns());
    });
    return Status::OK();
}

StatusOr<ChunkIteratorPtr> TableReader::scan(const std::vector<std::string>& value_columns,
                                             const std::vector<const ColumnPredicate*>& predicates) {
    if (_local_tablet_reader) {
        return _local_tablet_reader->scan(value_columns, predicates);
    }
    return Status::NotSupported("scan for remote table reader not supported");
}

} // namespace starrocks
