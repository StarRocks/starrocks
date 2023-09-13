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

#include "connector/jdbc_connector.h"

#include <sstream>

#include "exec/exec_node.h"
#include "exec/jdbc_scanner.h"
#include "exprs/expr.h"
#include "runtime/jdbc_driver_manager.h"
#include "storage/chunk_helper.h"
#include "util/slice.h"

namespace starrocks::connector {

// ================================

DataSourceProviderPtr JDBCConnector::create_data_source_provider(ConnectorScanNode* scan_node,
                                                                 const TPlanNode& plan_node) const {
    return std::make_unique<JDBCDataSourceProvider>(scan_node, plan_node);
}

// ================================

JDBCDataSourceProvider::JDBCDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node)
        : _scan_node(scan_node), _jdbc_scan_node(plan_node.jdbc_scan_node) {}

DataSourcePtr JDBCDataSourceProvider::create_data_source(const TScanRange& scan_range) {
    return std::make_unique<JDBCDataSource>(this, scan_range);
}

const TupleDescriptor* JDBCDataSourceProvider::tuple_descriptor(RuntimeState* state) const {
    return state->desc_tbl().get_tuple_descriptor(_jdbc_scan_node.tuple_id);
}

// ================================

static std::string get_jdbc_sql(const Slice jdbc_url, const std::string& table, const std::vector<std::string>& columns,
                                const std::vector<std::string>& filters, int64_t limit) {
    std::string object_identifier = jdbc_url.starts_with("jdbc:mysql") ? "`" : "";
    std::ostringstream oss;
    oss << "SELECT";
    for (size_t i = 0; i < columns.size(); i++) {
        oss << (i == 0 ? "" : ",") << " " << object_identifier << columns[i] << object_identifier;
    }
    oss << " FROM " << object_identifier << table << object_identifier;
    if (!filters.empty()) {
        oss << " WHERE ";
        for (size_t i = 0; i < filters.size(); i++) {
            oss << (i == 0 ? "" : " AND") << "(" << filters[i] << ")";
        }
    }
    if (limit != -1) {
        if (jdbc_url.starts_with("jdbc:oracle")) {
            // oracle doesn't support limit clause, we should generate a subquery to do this
            // ref: https://stackoverflow.com/questions/470542/how-do-i-limit-the-number-of-rows-returned-by-an-oracle-query-after-ordering
            return fmt::format("SELECT * FROM ({}) WHERE ROWNUM <= {}", oss.str(), limit);
        } else {
            oss << " LIMIT " << limit;
        }
    }
    return oss.str();
}

JDBCDataSource::JDBCDataSource(const JDBCDataSourceProvider* provider, const TScanRange& scan_range)
        : _provider(provider) {}

std::string JDBCDataSource::name() const {
    return "JDBCDataSource";
}

Status JDBCDataSource::open(RuntimeState* state) {
    const TJDBCScanNode& jdbc_scan_node = _provider->_jdbc_scan_node;
    _runtime_state = state;
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(jdbc_scan_node.tuple_id);
    RETURN_IF_ERROR(_create_scanner(state));
    return Status::OK();
}

void JDBCDataSource::close(RuntimeState* state) {
    if (_scanner != nullptr) {
        WARN_IF_ERROR(_scanner->close(state), "close jdbc scanner failed");
    }
}

Status JDBCDataSource::get_next(RuntimeState* state, ChunkPtr* chunk) {
    bool eos = false;
    _init_chunk(chunk, 0);
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

int64_t JDBCDataSource::raw_rows_read() const {
    return _rows_read;
}
int64_t JDBCDataSource::num_rows_read() const {
    return _rows_read;
}
int64_t JDBCDataSource::num_bytes_read() const {
    return _bytes_read;
}
int64_t JDBCDataSource::cpu_time_spent() const {
    // TODO: calculte the real cputime
    return 0;
}

Status JDBCDataSource::_create_scanner(RuntimeState* state) {
    const TJDBCScanNode& jdbc_scan_node = _provider->_jdbc_scan_node;
    const auto* jdbc_table = down_cast<const JDBCTableDescriptor*>(_tuple_desc->table_desc());

    Status status;
    std::string driver_name = jdbc_table->jdbc_driver_name();
    std::string driver_url = jdbc_table->jdbc_driver_url();
    std::string driver_checksum = jdbc_table->jdbc_driver_checksum();
    std::string driver_class = jdbc_table->jdbc_driver_class();
    std::string driver_location;

    status = JDBCDriverManager::getInstance()->get_driver_location(driver_name, driver_url, driver_checksum,
                                                                   &driver_location);
    if (!status.ok()) {
        LOG(ERROR) << fmt::format("Get JDBC Driver[{}] error, error is {}", driver_name, status.to_string());
        return status;
    }

    JDBCScanContext scan_ctx;
    scan_ctx.driver_path = driver_location;
    scan_ctx.driver_class_name = driver_class;
    scan_ctx.jdbc_url = jdbc_table->jdbc_url();
    scan_ctx.user = jdbc_table->jdbc_user();
    scan_ctx.passwd = jdbc_table->jdbc_passwd();
    scan_ctx.sql = get_jdbc_sql(scan_ctx.jdbc_url, jdbc_table->jdbc_table(), jdbc_scan_node.columns,
                                jdbc_scan_node.filters, _read_limit);
    _scanner = _pool->add(new JDBCScanner(scan_ctx, _tuple_desc, _runtime_profile));

    RETURN_IF_ERROR(_scanner->open(state));
    return Status::OK();
}

} // namespace starrocks::connector
