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
    _is_stream_pipeline = state->fragment_ctx()->is_stream_pipeline();

#ifndef NDEBUG
    // for ut
    if (state->fragment_ctx()->is_stream_test()) {
        return Status::OK();
    }
#endif

    ASSIGN_OR_RETURN(_tablet, _get_tablet())
    RETURN_IF_ERROR(_tablet->support_binlog());
    ASSIGN_OR_RETURN(_binlog_read_schema, _build_binlog_schema())

    BinlogReaderParams reader_params;
    reader_params.chunk_size = state->chunk_size();
    reader_params.output_schema = _binlog_read_schema;
    _binlog_reader = std::make_shared<BinlogReader>(_tablet, reader_params);
    RETURN_IF_ERROR(_binlog_reader->init());

    VLOG(3) << "Open binlog connector, tablet: " << _tablet->full_name()
            << ", binlog reader id: " << _binlog_reader->reader_id() << ", is_stream_pipeline: " << _is_stream_pipeline
            << ", binlog read schema: " << _binlog_read_schema;

    return Status::OK();
}

void BinlogDataSource::close(RuntimeState* state) {
    if (_binlog_reader != nullptr) {
        _binlog_reader->close();
        _binlog_reader.reset();
    }
}

Status BinlogDataSource::get_next(RuntimeState* state, ChunkPtr* chunk) {
    SCOPED_RAW_TIMER(&_cpu_time_ns);

#ifndef NDEBUG
    // for ut
    if (state->fragment_ctx()->is_stream_test()) {
        return _mock_chunk_test(chunk);
    }
#endif
    if (_need_seek_binlog.load(std::memory_order::memory_order_acquire)) {
        if (!_is_stream_pipeline) {
            RETURN_IF_ERROR(_prepare_non_stream_pipeline());
        }
        RETURN_IF_ERROR(_binlog_reader->seek(_start_version, _start_seq_id));
        _need_seek_binlog.store(false);
    }

    _init_chunk(chunk, state->chunk_size());
    Status status = _binlog_reader->get_next(chunk, _max_version_exclusive);
    VLOG_IF(3, !status.ok()) << "Fail to read binlog, tablet: " << _tablet->full_name()
                             << ", binlog reader id: " << _binlog_reader->reader_id()
                             << ", start_version: " << _start_version << ", _start_seq_id: " << _start_seq_id
                             << ", _max_version_exclusive: " << _max_version_exclusive << ", " << status;
    return status;
}

Status BinlogDataSource::_prepare_non_stream_pipeline() {
    BinlogRange binlog_range = _tablet->binlog_manager()->current_binlog_range();
    if (binlog_range.is_empty()) {
        VLOG(3) << "There is no binlog to scan, tablet: " << _tablet->full_name()
                << ", binlog reader id: " << _binlog_reader->reader_id();
        return Status::EndOfFile("There is no binlog");
    }

    _start_version.store(binlog_range.start_version());
    _start_seq_id.store(binlog_range.start_seq_id());
    _max_version_exclusive.store(binlog_range.end_version() + 1);

    VLOG(3) << "Prepare to scan binlog, tablet: " << _tablet->full_name()
            << ", binlog reader id: " << _binlog_reader->reader_id() << ", " << binlog_range.debug_string();

    return Status::OK();
}

Status BinlogDataSource::set_offset(int64_t table_version, int64_t changelog_id) {
    _mock_chunk_num = 0;
    _need_seek_binlog.store(true);
    _start_version.store(table_version);
    _start_seq_id.store(changelog_id);
    // Note MV can't read binlog across versions currently, so the max_version_exclusive is _start_version + 1
    _max_version_exclusive.store(table_version + 1);
    VLOG(3) << "Binlog connector set offset, tablet: " << _tablet->full_name()
            << ", binlog reader id: " << _binlog_reader->reader_id() << ", version: " << table_version
            << ", seq_id: " << changelog_id;
    return Status::OK();
}

Status BinlogDataSource::reset_status() {
    _rows_read_number = 0;
    _bytes_read = 0;
    _cpu_time_ns = 0;
    VLOG(3) << "Binlog connector reset status, tablet: " << _tablet->full_name()
            << ", binlog reader id: " << _binlog_reader->reader_id();
    return Status::OK();
}

BinlogMetaFieldMap BinlogDataSource::_build_binlog_meta_fields(ColumnId start_cid) {
    BinlogMetaFieldMap fields;
    ColumnId cid = start_cid;
    FieldPtr op_field = std::make_shared<Field>(cid, _column_name_constants.BINLOG_OP_COLUMN_NAME,
                                                get_type_info(TYPE_TINYINT), STORAGE_AGGREGATE_NONE, 1, false, false);
    fields.emplace(_column_name_constants.BINLOG_OP_COLUMN_NAME, op_field);
    cid += 1;

    FieldPtr version_field =
            std::make_shared<Field>(cid, _column_name_constants.BINLOG_VERSION_COLUMN_NAME, get_type_info(TYPE_BIGINT),
                                    STORAGE_AGGREGATE_NONE, 8, false, false);
    fields.emplace(_column_name_constants.BINLOG_VERSION_COLUMN_NAME, version_field);
    cid += 1;

    FieldPtr seq_id_field =
            std::make_shared<Field>(cid, _column_name_constants.BINLOG_SEQ_ID_COLUMN_NAME, get_type_info(TYPE_BIGINT),
                                    STORAGE_AGGREGATE_NONE, 8, false, false);
    fields.emplace(_column_name_constants.BINLOG_SEQ_ID_COLUMN_NAME, seq_id_field);
    cid += 1;

    FieldPtr timestamp_field =
            std::make_shared<Field>(cid, _column_name_constants.BINLOG_TIMESTAMP_COLUMN_NAME,
                                    get_type_info(TYPE_BIGINT), STORAGE_AGGREGATE_NONE, 8, false, false);
    fields.emplace(_column_name_constants.BINLOG_TIMESTAMP_COLUMN_NAME, timestamp_field);
    cid += 1;

    return fields;
}

StatusOr<Schema> BinlogDataSource::_build_binlog_schema() {
    BinlogMetaFieldMap binlog_meta_map = _build_binlog_meta_fields(_tablet->tablet_schema().num_columns());
    std::vector<uint32_t> data_column_cids;
    std::vector<uint32_t> meta_column_slot_index;
    Fields meta_fields;
    int slot_index = -1;
    for (auto slot : _tuple_desc->slots()) {
        DCHECK(slot->is_materialized());
        slot_index += 1;
        int32_t index = _tablet->field_index(slot->col_name());
        if (index >= 0) {
            data_column_cids.push_back(index);
        } else if (slot->col_name() == _column_name_constants.BINLOG_OP_COLUMN_NAME) {
            meta_column_slot_index.push_back(slot_index);
            meta_fields.emplace_back(binlog_meta_map[_column_name_constants.BINLOG_OP_COLUMN_NAME]);
        } else if (slot->col_name() == _column_name_constants.BINLOG_VERSION_COLUMN_NAME) {
            meta_column_slot_index.push_back(slot_index);
            meta_fields.emplace_back(binlog_meta_map[_column_name_constants.BINLOG_VERSION_COLUMN_NAME]);
        } else if (slot->col_name() == _column_name_constants.BINLOG_SEQ_ID_COLUMN_NAME) {
            meta_column_slot_index.push_back(slot_index);
            meta_fields.emplace_back(binlog_meta_map[_column_name_constants.BINLOG_SEQ_ID_COLUMN_NAME]);
        } else if (slot->col_name() == _column_name_constants.BINLOG_TIMESTAMP_COLUMN_NAME) {
            meta_column_slot_index.push_back(slot_index);
            meta_fields.emplace_back(binlog_meta_map[_column_name_constants.BINLOG_TIMESTAMP_COLUMN_NAME]);
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
    Schema schema = ChunkHelper::convert_schema(tablet_schema, data_column_cids);
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

Status BinlogDataSource::_mock_chunk_test(ChunkPtr* chunk) {
    VLOG_ROW << "[binlog data source] mock_chunk:";
    int32_t num = _mock_chunk_num.fetch_add(1, std::memory_order_relaxed);
    if (num >= 1) {
        return Status::EndOfFile(fmt::format("Has sent {} chunks", num));
    }
    auto chunk_temp = std::make_shared<Chunk>();
    int64_t start = 0;
    int64_t step = 1;
    int64_t ndv_count = 100;
    for (auto idx = 0; idx < 2; idx++) {
        auto column = Int64Column::create();
        for (int64_t i = 0; i < 4; i++) {
            start += step;
            VLOG_ROW << "Append col:" << idx << ", row:" << start;
            column->append(start % ndv_count);
        }
        chunk_temp->append_column(column, SlotId(idx));
    }

    // ops
    auto ops = Int8Column::create();
    for (int64_t i = 0; i < 1; i++) {
        ops->append(0);
    }
    *chunk = StreamChunkConverter::make_stream_chunk(std::move(chunk_temp), std::move(ops));
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

int64_t BinlogDataSource::num_rows_read_in_epoch() const {
    return _rows_read_number;
}

int64_t BinlogDataSource::cpu_time_spent_in_epoch() const {
    return _cpu_time_ns;
}

} // namespace starrocks::connector