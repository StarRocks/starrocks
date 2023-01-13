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

#include "connector/binlog_connector.h"

#include "runtime/descriptors.h"
#include "storage/chunk_helper.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"

namespace starrocks::connector {

DataSourceProviderPtr BinlogConnector::create_data_source_provider(ConnectorScanNode* scan_node,
                                                                   const TPlanNode& plan_node) const {
    return std::make_unique<BinlogDataSourceProvider>(scan_node, plan_node);
}

// ================================

BinlogDataSourceProvider::BinlogDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node)
        : _scan_node(scan_node), _binlog_scan_node(plan_node.stream_scan_node.binlog_scan) {}

DataSourcePtr BinlogDataSourceProvider::create_data_source(const TScanRange& scan_range) {
    return std::make_unique<BinlogDataSource>(this, scan_range);
}

// ================================

BinlogDataSource::BinlogDataSource(const BinlogDataSourceProvider* provider, const TScanRange& scan_range)
        : _provider(provider), _scan_range(scan_range.binlog_scan_range) {}

Status BinlogDataSource::open(RuntimeState* state) {
    const TBinlogScanNode& binlog_scan_node = _provider->_binlog_scan_node;
    _runtime_state = state;
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(binlog_scan_node.tuple_id);
    ASSIGN_OR_RETURN(_tablet, _get_tablet())
    ASSIGN_OR_RETURN(_binlog_read_schema, _build_binlog_schema())
    VLOG(2) << "Tablet id " << _tablet->tablet_uid() << ", version " << _scan_range.offset.version << ", seq_id "
            << _scan_range.offset.lsn << ", binlog read schema " << _binlog_read_schema;
    return Status::OK();
}

void BinlogDataSource::close(RuntimeState* state) {}

Status BinlogDataSource::get_next(RuntimeState* state, ChunkPtr* chunk) {
    SCOPED_RAW_TIMER(&_cpu_time_ns);
    _init_chunk(chunk, state->chunk_size());
    // TODO replace with BinlogReader
    return _mock_chunk(chunk->get());
}

BinlogMetaFieldMap BinlogDataSource::_build_binlog_meta_fields(ColumnId start_cid) {
    BinlogMetaFieldMap fields;
    ColumnId cid = start_cid;
    VectorizedFieldPtr op_field = std::make_shared<VectorizedField>(cid, BINLOG_OP, get_type_info(TYPE_TINYINT),
                                                                    STORAGE_AGGREGATE_NONE, 1, false, false);
    fields.emplace(BINLOG_OP, op_field);
    cid += 1;

    VectorizedFieldPtr version_field = std::make_shared<VectorizedField>(
            cid, BINLOG_VERSION, get_type_info(TYPE_BIGINT), STORAGE_AGGREGATE_NONE, 8, false, false);
    fields.emplace(BINLOG_VERSION, version_field);
    cid += 1;

    VectorizedFieldPtr seq_id_field = std::make_shared<VectorizedField>(cid, BINLOG_SEQ_ID, get_type_info(TYPE_BIGINT),
                                                                        STORAGE_AGGREGATE_NONE, 8, false, false);
    fields.emplace(BINLOG_SEQ_ID, seq_id_field);
    cid += 1;

    VectorizedFieldPtr timestamp_field = std::make_shared<VectorizedField>(
            cid, BINLOG_TIMESTAMP, get_type_info(TYPE_BIGINT), STORAGE_AGGREGATE_NONE, 8, false, false);
    fields.emplace(BINLOG_TIMESTAMP, timestamp_field);
    cid += 1;

    return fields;
}

StatusOr<VectorizedSchema> BinlogDataSource::_build_binlog_schema() {
    BinlogMetaFieldMap binlog_meta_map = _build_binlog_meta_fields(_tablet->tablet_schema().num_columns());
    std::vector<uint32_t> data_column_cids;
    std::vector<uint32_t> meta_column_slot_index;
    VectorizedFields meta_fields;
    int slot_index = -1;
    for (auto slot : _tuple_desc->slots()) {
        DCHECK(slot->is_materialized());
        slot_index += 1;
        int32_t index = _tablet->field_index(slot->col_name());
        if (index >= 0) {
            data_column_cids.push_back(index);
        } else if (slot->col_name() == BINLOG_OP) {
            meta_column_slot_index.push_back(slot_index);
            meta_fields.emplace_back(binlog_meta_map[BINLOG_OP]);
        } else if (slot->col_name() == BINLOG_VERSION) {
            meta_column_slot_index.push_back(slot_index);
            meta_fields.emplace_back(binlog_meta_map[BINLOG_VERSION]);
        } else if (slot->col_name() == BINLOG_SEQ_ID) {
            meta_column_slot_index.push_back(slot_index);
            meta_fields.emplace_back(binlog_meta_map[BINLOG_SEQ_ID]);
        } else if (slot->col_name() == BINLOG_TIMESTAMP) {
            meta_column_slot_index.push_back(slot_index);
            meta_fields.emplace_back(binlog_meta_map[BINLOG_TIMESTAMP]);
        } else {
            std::string msg = fmt::format("invalid field name: {}", slot->col_name());
            LOG(WARNING) << msg;
            return Status::InternalError(msg);
        }
    }

    if (data_column_cids.empty()) {
        return Status::InternalError("failed to build binlog schema, no materialized data slot!");
    }

    const TabletSchema& tablet_schema = _tablet->tablet_schema();
    VectorizedSchema schema = ChunkHelper::convert_schema(tablet_schema, data_column_cids);
    for (int32_t i = 0; i < meta_column_slot_index.size(); i++) {
        uint32_t index = meta_column_slot_index[i];
        if (index >= schema.num_fields()) {
            schema.append(meta_fields[i]);
        } else {
            schema.insert(index, meta_fields[i]);
        }
    }

    return schema;
}

Status BinlogDataSource::_mock_chunk(Chunk* chunk) {
    for (int row = 0; row < 10; row++) {
        for (int col = 0; col < chunk->num_columns(); col++) {
            LogicalType type = _binlog_read_schema.field(col)->type()->type();
            Datum datum;
            switch (type) {
            case TYPE_TINYINT: {
                int8_t val = row + col;
                datum = Datum(val);
                break;
            }
            case TYPE_SMALLINT: {
                int16_t val = row + col;
                datum = Datum(val);
                break;
            }
            case TYPE_INT: {
                int32_t val = row + col;
                datum = Datum(val);
                break;
            }
            case TYPE_BIGINT: {
                int64_t val = row + col;
                datum = Datum(val);
                break;
            }
            case TYPE_LARGEINT: {
                int128_t val = row + col;
                datum = Datum(val);
                break;
            }
            default:
                return Status::InternalError(fmt::format("Not support to mock type {}", type));
            }
            chunk->get_column_by_index(col)->append_datum(datum);
        }
    }
    return Status::OK();
}

StatusOr<TabletSharedPtr> BinlogDataSource::_get_tablet() {
    TTabletId tablet_id = _scan_range.tablet_id;
    std::string err;
    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, true, &err);
    if (!tablet) {
        std::stringstream ss;
        ss << "failed to get tablet. tablet_id=" << tablet_id << ", reason=" << err;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    return tablet;
}

int64_t BinlogDataSource::raw_rows_read() const {
    return _rows_read_number;
}
int64_t BinlogDataSource::num_rows_read() const {
    return _rows_read_number;
}
int64_t BinlogDataSource::num_bytes_read() const {
    return _bytes_read;
}

int64_t BinlogDataSource::cpu_time_spent() const {
    return _cpu_time_ns;
}

} // namespace starrocks::connector
