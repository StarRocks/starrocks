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

#include "connector/adbc_connector.h"

#include <sstream>

#include "exec/adbc_scanner.h"
#include "runtime/descriptors.h"
#include "storage/chunk_helper.h"

namespace starrocks::connector {

// Assemble a SQL query string from TADBCScanNode fields.
// This is a free function so it can be tested independently.
std::string get_adbc_sql(const std::string& table, const std::vector<std::string>& columns,
                         const std::vector<std::string>& filters, int64_t limit) {
    std::ostringstream oss;
    oss << "SELECT";
    for (size_t i = 0; i < columns.size(); i++) {
        oss << (i == 0 ? "" : ",") << " " << columns[i];
    }
    oss << " FROM " << table;
    if (!filters.empty()) {
        oss << " WHERE ";
        for (size_t i = 0; i < filters.size(); i++) {
            oss << (i == 0 ? "" : " AND ") << "(" << filters[i] << ")";
        }
    }
    if (limit != -1) {
        oss << " LIMIT " << limit;
    }
    return oss.str();
}

// ================================

DataSourceProviderPtr ADBCConnector::create_data_source_provider(ConnectorScanNode* scan_node,
                                                                 const TPlanNode& plan_node) const {
    return std::make_unique<ADBCDataSourceProvider>(scan_node, plan_node);
}

// ================================

ADBCDataSourceProvider::ADBCDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node)
        : _scan_node(scan_node), _adbc_scan_node(plan_node.adbc_scan_node) {}

DataSourcePtr ADBCDataSourceProvider::create_data_source(const TScanRange& scan_range) {
    return std::make_unique<ADBCDataSource>(this, scan_range);
}

const TupleDescriptor* ADBCDataSourceProvider::tuple_descriptor(RuntimeState* state) const {
    return state->desc_tbl().get_tuple_descriptor(_adbc_scan_node.tuple_id);
}

// ================================

ADBCDataSource::ADBCDataSource(const ADBCDataSourceProvider* provider, const TScanRange& scan_range)
        : _provider(provider) {}

std::string ADBCDataSource::name() const {
    return "ADBCDataSource";
}

Status ADBCDataSource::open(RuntimeState* state) {
    const TADBCScanNode& adbc_scan_node = _provider->_adbc_scan_node;

    // Extract connection parameters
    std::string driver = adbc_scan_node.adbc_driver;
    std::string uri = adbc_scan_node.adbc_uri;
    std::string username = adbc_scan_node.__isset.adbc_username ? adbc_scan_node.adbc_username : "";
    std::string password = adbc_scan_node.__isset.adbc_password ? adbc_scan_node.adbc_password : "";
    std::string token = adbc_scan_node.__isset.adbc_token ? adbc_scan_node.adbc_token : "";

    // Extract TLS parameters
    std::string ca_cert_file =
            adbc_scan_node.__isset.adbc_tls_ca_cert_file ? adbc_scan_node.adbc_tls_ca_cert_file : "";
    std::string client_cert_file =
            adbc_scan_node.__isset.adbc_tls_client_cert_file ? adbc_scan_node.adbc_tls_client_cert_file : "";
    std::string client_key_file =
            adbc_scan_node.__isset.adbc_tls_client_key_file ? adbc_scan_node.adbc_tls_client_key_file : "";
    // CRITICAL: Default to true when field is not set (Thrift optional bool defaults to false)
    bool tls_verify = adbc_scan_node.__isset.adbc_tls_verify ? adbc_scan_node.adbc_tls_verify : true;

    // Build SQL query string
    std::string sql = get_adbc_sql(adbc_scan_node.table_name, adbc_scan_node.columns, adbc_scan_node.filters,
                                   adbc_scan_node.__isset.limit ? adbc_scan_node.limit : -1);

    // Get tuple descriptor
    const TupleDescriptor* tuple_desc = state->desc_tbl().get_tuple_descriptor(adbc_scan_node.tuple_id);

    // Create scanner
    _scanner = std::make_unique<ADBCScanner>(std::move(driver), std::move(uri), std::move(username),
                                             std::move(password), std::move(token), std::move(sql), tuple_desc,
                                             std::move(ca_cert_file), std::move(client_cert_file),
                                             std::move(client_key_file), tls_verify);

    RETURN_IF_ERROR(_scanner->open(state));

    // Capture connect_time_ms for EXPLAIN ANALYZE stats
    _connect_time_ms = _scanner->connect_time_ms();

    return Status::OK();
}

void ADBCDataSource::close(RuntimeState* state) {
    if (_scanner) {
        _scanner->close(state);
        _scanner.reset();
    }
}

Status ADBCDataSource::get_next(RuntimeState* state, ChunkPtr* chunk) {
    bool eos = false;
    RETURN_IF_ERROR(_init_chunk_if_needed(chunk, 0));
    do {
        RETURN_IF_ERROR(_scanner->get_next(state, chunk, &eos));
    } while (!eos && (*chunk)->num_rows() == 0);

    if (eos) {
        return Status::EndOfFile("");
    }

    _rows_read += (*chunk)->num_rows();
    _bytes_read += (*chunk)->bytes_usage();
    return Status::OK();
}

int64_t ADBCDataSource::raw_rows_read() const {
    return _rows_read;
}

int64_t ADBCDataSource::num_rows_read() const {
    return _rows_read;
}

int64_t ADBCDataSource::num_bytes_read() const {
    return _bytes_read;
}

int64_t ADBCDataSource::cpu_time_spent() const {
    return 0;
}

} // namespace starrocks::connector
