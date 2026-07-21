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

#include <vector>

#include "common/statusor.h"
#include "connector_primitive/connector.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/serde/protobuf_chunk_serde.h"

namespace starrocks::connector {

StatusOr<serde::ProtobufChunkMeta> build_remote_scan_chunk_meta(const std::vector<TStarRocksRemoteScanOutput>& outputs);

class StarRocksConnector final : public Connector {
public:
    ~StarRocksConnector() override = default;

    DataSourceProviderPtr create_data_source_provider(ConnectorScanNode* scan_node,
                                                      const TPlanNode& plan_node) const override;

    ConnectorType connector_type() const override { return ConnectorType::STARROCKS; }
};

class StarRocksDataSource;
class StarRocksDataSourceProvider;
class RemoteScanClient;

class StarRocksDataSourceProvider final : public DataSourceProvider {
public:
    ~StarRocksDataSourceProvider() override = default;
    friend class StarRocksDataSource;

    StarRocksDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node);
    DataSourcePtr create_data_source(const TScanRange& scan_range) override;

    bool insert_local_exchange_operator() const override { return false; }
    bool accept_empty_scan_ranges() const override { return false; }
    const TupleDescriptor* tuple_descriptor(RuntimeState* state) const override;

private:
    StatusOr<std::vector<SlotDescriptor*>> output_slots(RuntimeState* state) const;
    const std::vector<TStarRocksRemoteScanOutput>& remote_outputs() const;

    ConnectorScanNode* _scan_node;
    TStarRocksScanNode _starrocks_scan_node;
};

class StarRocksDataSource final : public DataSource {
public:
    StarRocksDataSource(const StarRocksDataSourceProvider* provider, const TScanRange& scan_range);
    ~StarRocksDataSource() override;

    std::string name() const override;
    Status open(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk) override;

    // "Raw" for a remote scan = rows/bytes received over the wire, BEFORE the local residual
    // conjuncts run. This is the scanner's work/progress measure: it drives the scan-thread
    // time-slice yield (raw_rows_threshold) and the ScanRows profile counter, so it must
    // advance even when the residual predicates filter out entire chunks. The remote BE's own
    // storage-level raw rows are not shipped back by the fetch protocol.
    int64_t raw_rows_read() const override { return _raw_rows_read; }
    // Rows actually delivered to the query (after residual filtering); drives LIMIT short-circuit.
    int64_t num_rows_read() const override { return _num_rows_read; }
    int64_t num_bytes_read() const override { return _bytes_read; }
    int64_t cpu_time_spent() const override { return 0; }

private:
    const StarRocksDataSourceProvider* _provider;
    TStarRocksScanRange _scan_range;
    bool _has_scan_range = false;
    std::unique_ptr<RemoteScanClient> _client;
    int64_t _raw_rows_read = 0;
    int64_t _num_rows_read = 0;
    int64_t _bytes_read = 0;
};

} // namespace starrocks::connector
