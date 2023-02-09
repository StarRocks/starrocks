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

#include "connector/file_connector.h"

#include "exec/csv_scanner.h"
#include "exec/exec_node.h"
#include "exec/json_scanner.h"
#include "exec/orc_scanner.h"
#include "exec/parquet_scanner.h"
#include "exprs/expr.h"

namespace starrocks::connector {

DataSourceProviderPtr FileConnector::create_data_source_provider(ConnectorScanNode* scan_node,
                                                                 const TPlanNode& plan_node) const {
    return std::make_unique<FileDataSourceProvider>(scan_node, plan_node);
}

// ================================

FileDataSourceProvider::FileDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node)
        : _scan_node(scan_node), _file_scan_node(plan_node.file_scan_node) {}

DataSourcePtr FileDataSourceProvider::create_data_source(const TScanRange& scan_range) {
    return std::make_unique<FileDataSource>(this, scan_range);
}

// ================================
FileDataSource::FileDataSource(const FileDataSourceProvider* provider, const TScanRange& scan_range)
        : _provider(provider), _scan_range(scan_range.broker_scan_range) {
    // remove range desc with empty file
    _scan_range.ranges.clear();
    for (const TBrokerRangeDesc& range_desc : scan_range.broker_scan_range.ranges) {
        // file_size is optional, and is not set in stream load and routine
        // load, so we should check file size is set firstly.
        if (range_desc.__isset.file_size && range_desc.file_size == 0) {
            continue;
        }
        _scan_range.ranges.emplace_back(range_desc);
    }
}

Status FileDataSource::open(RuntimeState* state) {
    DCHECK(state != nullptr);
    RETURN_IF_CANCELLED(state);
    _runtime_state = state;
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_provider->_file_scan_node.tuple_id);
    DCHECK(_tuple_desc != nullptr);
    _init_counter();
    RETURN_IF_ERROR(_create_scanner());
    return Status::OK();
}

Status FileDataSource::_create_scanner() {
    if (_scan_range.ranges.empty()) {
        return Status::EndOfFile("scan range is empty");
    }
    // create scanner object and open
    if (_scan_range.ranges[0].format_type == TFileFormatType::FORMAT_ORC) {
        _scanner = std::make_unique<ORCScanner>(_runtime_state, _runtime_profile, _scan_range, &_counter);
    } else if (_scan_range.ranges[0].format_type == TFileFormatType::FORMAT_PARQUET) {
        _scanner = std::make_unique<ParquetScanner>(_runtime_state, _runtime_profile, _scan_range, &_counter);
    } else if (_scan_range.ranges[0].format_type == TFileFormatType::FORMAT_JSON) {
        _scanner = std::make_unique<JsonScanner>(_runtime_state, _runtime_profile, _scan_range, &_counter);
    } else if (_scan_range.ranges[0].format_type == TFileFormatType::FORMAT_AVRO) {
        // TODO(yangzaorang): we use json as an intermediate format to parse avro format, but there are
        // performance issues here, and we could directly parse avro format data later.
        _scanner = std::make_unique<JsonScanner>(_runtime_state, _runtime_profile, _scan_range, &_counter);
    } else {
        _scanner = std::make_unique<CSVScanner>(_runtime_state, _runtime_profile, _scan_range, &_counter);
    }
    if (_scanner == nullptr) {
        return Status::InternalError("Failed to create scanner");
    }
    RETURN_IF_ERROR(_scanner->open());
    return Status::OK();
}

void FileDataSource::close(RuntimeState* state) {
    if (_closed) {
        return;
    }
    _closed = true;
    if (_scanner != nullptr) {
        _scanner->close();
    }
    Expr::close(_conjunct_ctxs, state);
}

Status FileDataSource::get_next(RuntimeState* state, ChunkPtr* chunk) {
    DCHECK(state != nullptr && chunk != nullptr);
    RETURN_IF_CANCELLED(state);

    // If we have finished all works
    if (_scan_finished) {
        return Status::EndOfFile("file scan finished!");
    }
    while (true) {
        auto res = _scanner->get_next();
        if (!res.ok()) {
            if (res.status().is_end_of_file()) {
                _update_counter();
                _scan_finished = true;
            }
            return res.status();
        }
        *chunk = std::move(res).value();

        size_t before_rows = (*chunk)->num_rows();

        const TQueryOptions& query_options = state->query_options();
        if (query_options.__isset.load_job_type && query_options.load_job_type == TLoadJobType::BROKER) {
            size_t before_size = (*chunk)->bytes_usage();
            state->update_num_rows_load_from_source(before_rows);
            state->update_num_bytes_load_from_source(before_size);
        }

        _counter.filtered_rows_read += before_rows;
        // eval conjuncts
        RETURN_IF_ERROR(ExecNode::eval_conjuncts(_conjunct_ctxs, (*chunk).get()));
        _counter.num_rows_read += (*chunk)->num_rows();
        _counter.num_rows_unselected += (before_rows - (*chunk)->num_rows());
        _counter.num_bytes_read += (*chunk)->bytes_usage();

        // Row batch has been filled, return this
        if ((*chunk)->num_rows() > 0) {
            break;
        }
    }
    return Status::OK();
}

int64_t FileDataSource::raw_rows_read() const {
    return _counter.filtered_rows_read + _counter.num_rows_filtered;
}

int64_t FileDataSource::num_rows_read() const {
    return _counter.num_rows_read;
}

int64_t FileDataSource::num_bytes_read() const {
    return _counter.num_bytes_read;
}

int64_t FileDataSource::cpu_time_spent() const {
    return _counter.total_ns;
}

void FileDataSource::_init_counter() {
    // Profile
    _scanner_total_timer = ADD_TIMER(_runtime_profile, "ScannerTotalTime");
    RuntimeProfile* p = _runtime_profile->create_child("FileScanner", true, true);
    _scanner_fill_timer = ADD_TIMER(p, "FillTime");
    _scanner_read_timer = ADD_TIMER(p, "ReadTime");
    _scanner_cast_chunk_timer = ADD_TIMER(p, "CastChunkTime");
    _scanner_materialize_timer = ADD_TIMER(p, "MaterializeTime");
    _scanner_init_chunk_timer = ADD_TIMER(p, "CreateChunkTime");
    _scanner_file_reader_timer = ADD_TIMER(p->create_child("FilePRead", true, true), "FileReadTime");
}

void FileDataSource::_update_counter() {
    _runtime_state->update_num_rows_load_filtered(_counter.num_rows_filtered);
    _runtime_state->update_num_rows_load_unselected(_counter.num_rows_unselected);

    COUNTER_UPDATE(_scanner_total_timer, _counter.total_ns);
    COUNTER_UPDATE(_scanner_fill_timer, _counter.fill_ns);
    COUNTER_UPDATE(_scanner_read_timer, _counter.read_batch_ns);
    COUNTER_UPDATE(_scanner_cast_chunk_timer, _counter.cast_chunk_ns);
    COUNTER_UPDATE(_scanner_materialize_timer, _counter.materialize_ns);
    COUNTER_UPDATE(_scanner_init_chunk_timer, _counter.init_chunk_ns);
    COUNTER_UPDATE(_scanner_file_reader_timer, _counter.file_read_ns);
}

} // namespace starrocks::connector
