// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/vectorized_fwd.h"
#include "connector/connector.h"
#include "exec/vectorized/file_scanner.h"

namespace starrocks::connector {

class FileConnector final : public Connector {
public:
    ~FileConnector() override = default;

    DataSourceProviderPtr create_data_source_provider(starrocks::vectorized::ConnectorScanNode* scan_node,
                                                      const TPlanNode& plan_node) const override;

    ConnectorType connector_type() const override { return ConnectorType::FILE; }
};

class FileDataSource;
class FileDataSourceProvider;

class FileDataSourceProvider final : public DataSourceProvider {
public:
    ~FileDataSourceProvider() override = default;
    friend class FileDataSource;
    FileDataSourceProvider(vectorized::ConnectorScanNode* scan_node, const TPlanNode& plan_node);
    DataSourcePtr create_data_source(const TScanRange& scan_range) override;

    bool insert_local_exchange_operator() const override { return true; }
    bool accept_empty_scan_ranges() const override { return false; }
    const TupleDescriptor* tuple_descriptor(RuntimeState* state) const override;

protected:
    vectorized::ConnectorScanNode* _scan_node;
    const TFileScanNode _file_scan_node;
};

class FileDataSource final : public DataSource {
public:
    ~FileDataSource() override = default;

    FileDataSource(const FileDataSourceProvider* provider, const TScanRange& scan_range);
    Status open(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    Status get_next(RuntimeState* state, vectorized::ChunkPtr* chunk) override;

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

    std::unique_ptr<starrocks::vectorized::FileScanner> _scanner;
    starrocks::vectorized::ScannerCounter _counter;

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
} // namespace starrocks::connector
