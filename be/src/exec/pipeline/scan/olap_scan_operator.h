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

#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/scan/scan_operator.h"

namespace starrocks {

class ScanNode;
class Rowset;
using RowsetSharedPtr = std::shared_ptr<Rowset>;

namespace pipeline {

class OlapScanContext;
using OlapScanContextPtr = std::shared_ptr<OlapScanContext>;
class OlapScanContextFactory;
using OlapScanContextFactoryPtr = std::shared_ptr<OlapScanContextFactory>;

class OlapScanOperatorFactory final : public ScanOperatorFactory {
public:
    OlapScanOperatorFactory(int32_t id, ScanNode* scan_node, OlapScanContextFactoryPtr _ctx_factory);

    ~OlapScanOperatorFactory() override = default;

    Status do_prepare(RuntimeState* state) override;
    void do_close(RuntimeState* state) override;
    OperatorPtr do_create(int32_t dop, int32_t driver_sequence) override;

    TPartitionType::type partition_type() const override { return TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED; }
    const std::vector<ExprContext*>& partition_exprs() const override;

private:
    OlapScanContextFactoryPtr _ctx_factory;
};

class OlapScanOperator final : public ScanOperator {
public:
    OlapScanOperator(OperatorFactory* factory, int32_t id, int32_t driver_sequence, int32_t dop, ScanNode* scan_node,
                     OlapScanContextPtr ctx);

    ~OlapScanOperator() override;

    bool has_output() const override;
    bool is_finished() const override;

    Status do_prepare(RuntimeState* state) override;
    void do_close(RuntimeState* state) override;
    ChunkSourcePtr create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) override;

    int64_t get_scan_table_id() const override;

protected:
    void attach_chunk_source(int32_t source_index) override;
    void detach_chunk_source(int32_t source_index) override;
    bool has_shared_chunk_source() const override;
    ChunkPtr get_chunk_from_buffer() override;
    size_t num_buffered_chunks() const override;
    size_t buffer_size() const override;
    size_t buffer_capacity() const override;
    size_t default_buffer_capacity() const override;
    ChunkBufferTokenPtr pin_chunk(int num_chunks) override;
    bool is_buffer_full() const override;
    void set_buffer_finished() override;

private:
    OlapScanContextPtr _ctx;
};

} // namespace pipeline
} // namespace starrocks
