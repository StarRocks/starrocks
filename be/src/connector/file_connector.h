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

#include "column/vectorized_fwd.h"
#include "connector/connector.h"
#include "connector_sink/connector_chunk_sink.h"
#include "exec/file_scanner.h"

namespace starrocks::connector {

class FileConnector final : public Connector {
public:
    ~FileConnector() override = default;

    DataSourceProviderPtr create_data_source_provider(starrocks::ConnectorScanNode* scan_node,
                                                      const TPlanNode& plan_node) const override;

    std::unique_ptr<ConnectorChunkSinkProvider> create_data_sink_provider() const override;

    ConnectorType connector_type() const override { return ConnectorType::FILE; }
};

class FileDataSource;
class FileDataSourceProvider;

class FileDataSourceProvider final : public DataSourceProvider {
public:
    ~FileDataSourceProvider() override = default;
    friend class FileDataSource;
    FileDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node);
    DataSourcePtr create_data_source(const TScanRange& scan_range) override;

    bool insert_local_exchange_operator() const override { return true; }
    bool accept_empty_scan_ranges() const override { return false; }
    const TupleDescriptor* tuple_descriptor(RuntimeState* state) const override;

protected:
    ConnectorScanNode* _scan_node;
    const TFileScanNode _file_scan_node;
};

class FileDataSource final : public DataSource {
public:
    ~FileDataSource() override = default;

    FileDataSource(const FileDataSourceProvider* provider, const TScanRange& scan_range);
    std::string name() const override;
    Status open(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk) override;
    const std::string get_custom_coredump_msg() const override;

    int64_t raw_rows_read() const override;
    int64_t num_rows_read() const override;
    int64_t num_bytes_read() const override;
    int64_t cpu_time_spent() const override;

private:
    const FileDataSourceProvider* _provider;
    mutable TBrokerScanRange _scan_range;

    // =========================
    RuntimeState* _runtime_state = nullptr;
    bool _scan_finished{false};
    bool _closed{false};

    std::unique_ptr<starrocks::FileScanner> _scanner;
    starrocks::ScannerCounter _counter;

    // Profile information
    RuntimeProfile::Counter* _scanner_total_timer = nullptr;
    RuntimeProfile::Counter* _scanner_fill_timer = nullptr;
    RuntimeProfile::Counter* _scanner_read_timer = nullptr;
    RuntimeProfile::Counter* _scanner_cast_chunk_timer = nullptr;
    RuntimeProfile::Counter* _scanner_materialize_timer = nullptr;
    RuntimeProfile::Counter* _scanner_init_chunk_timer = nullptr;
    RuntimeProfile::Counter* _scanner_file_reader_timer = nullptr;

    // =========================
    Status _create_scanner();

    void _init_counter();

    void _update_counter();
};

struct FileChunkSinkContext : public ConnectorChunkSinkContext {
    ~FileChunkSinkContext() override = default;

    std::string path;
    std::vector<std::string> column_names;
    std::vector<ExprContext*> output_exprs;
    std::vector<std::string> partition_columns;
    std::vector<ExprContext*> partition_exprs;
    int64_t max_file_size;
    formats::FileWriter::FileFormat format;
    std::shared_ptr<formats::FileWriter::FileWriterOptions> options;
    PriorityThreadPool* executor;
    TCloudConfiguration cloud_conf;
    pipeline::FragmentContext* fragment_context;
};

class FileDataSinkProvider : public ConnectorChunkSinkProvider {
public:
    ~FileDataSinkProvider() override = default;

    std::unique_ptr<ConnectorChunkSink> create_chunk_sink(std::shared_ptr<ConnectorChunkSinkContext> context,
                                                          int32_t driver_id) override;
};

} // namespace starrocks::connector
