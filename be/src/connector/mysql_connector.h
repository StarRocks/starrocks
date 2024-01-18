// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "connector/connector.h"
#include "exec/mysql_scanner.h"

namespace starrocks {

namespace connector {
class MySQLConnector final : public Connector {
public:
    ~MySQLConnector() override = default;

    DataSourceProviderPtr create_data_source_provider(vectorized::ConnectorScanNode* scan_node,
                                                      const TPlanNode& plan_node) const override;

    ConnectorType connector_type() const override { return ConnectorType::MYSQL; }
};

class MySQLDataSource;
class MySQLDataSourceProvider;

class MySQLDataSourceProvider final : public DataSourceProvider {
public:
    ~MySQLDataSourceProvider() override = default;
    friend class MySQLDataSource;
    MySQLDataSourceProvider(vectorized::ConnectorScanNode* scan_node, const TPlanNode& plan_node);
    DataSourcePtr create_data_source(const TScanRange& scan_range) override;

    bool insert_local_exchange_operator() const override { return true; }
    bool accept_empty_scan_ranges() const override { return false; }
    const TupleDescriptor* tuple_descriptor(RuntimeState* state) const override;

protected:
    vectorized::ConnectorScanNode* _scan_node;
    const TMySQLScanNode _mysql_scan_node;
};

class MySQLDataSource final : public DataSource {
public:
    ~MySQLDataSource() override = default;

    MySQLDataSource(const MySQLDataSourceProvider* provider, const TScanRange& scan_range);
    Status open(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    Status get_next(RuntimeState* state, vectorized::ChunkPtr* chunk) override;

    int64_t raw_rows_read() const override;
    int64_t num_rows_read() const override;
    int64_t num_bytes_read() const override;
    int64_t cpu_time_spent() const override;

private:
    const MySQLDataSourceProvider* _provider;

    // ============= init func =============
    Status _init_params(RuntimeState* state);

    // =====================================
    bool _is_finished = false;

    MysqlScannerParam _my_param;
    // Name of Mysql table
    std::string _table_name;

    // select columns
    std::vector<std::string> _columns;
    // where clause
    std::vector<std::string> _filters;
    // temporal clause
    std::string _temporal_clause;

    // Tuple index in tuple row.
    size_t _slot_num = 0;
    std::unique_ptr<MysqlScanner> _mysql_scanner;

    int64_t _rows_read = 0;
    int64_t _bytes_read = 0;
    int64_t _cpu_time_spent_ns = 0;

    Status fill_chunk(vectorized::ChunkPtr* chunk, char** data, size_t* length);

    Status append_text_to_column(const char* data, const int& len, const SlotDescriptor* slot_desc,
                                 vectorized::Column* column);

    template <PrimitiveType PT, typename CppType = vectorized::RunTimeCppType<PT>>
    void append_value_to_column(vectorized::Column* column, CppType& value);
};

} // namespace connector
} // namespace starrocks
