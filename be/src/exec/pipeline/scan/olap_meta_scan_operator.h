// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/scan/balanced_chunk_buffer.h"
#include "exec/pipeline/scan/olap_meta_scan_context.h"
#include "exec/pipeline/scan/scan_operator.h"
#include "exec/vectorized/olap_meta_scan_node.h"
#include "gen_cpp/Types_types.h"

namespace starrocks {
namespace pipeline {

class OlapMetaScanOperatorFactory final : public ScanOperatorFactory {
public:
    OlapMetaScanOperatorFactory(int32_t id, ScanNode* meta_scan_node, size_t dop,
                                std::shared_ptr<OlapMetaScanContextFactory> ctx_factory);

    ~OlapMetaScanOperatorFactory() override = default;

    bool with_morsels() const override { return true; }

    Status do_prepare(RuntimeState* state) override;
    void do_close(RuntimeState* state) override;
    OperatorPtr do_create(int32_t dop, int32_t driver_sequence) override;

private:
    std::shared_ptr<OlapMetaScanContextFactory> _ctx_factory;
};

class OlapMetaScanOperator final : public ScanOperator {
public:
    OlapMetaScanOperator(OperatorFactory* factory, int32_t id, int32_t driver_sequence, int32_t dop,
                         ScanNode* scan_node, OlapMetaScanContextPtr scan_ctx);

    ~OlapMetaScanOperator() override;

    bool has_output() const override;
    bool is_finished() const override;

    Status do_prepare(RuntimeState* state) override;
    void do_close(RuntimeState* state) override;
    ChunkSourcePtr create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) override;

private:
    void attach_chunk_source(int32_t source_index) override {}
    void detach_chunk_source(int32_t source_index) override {}
    bool has_shared_chunk_source() const override { return false; }
    ChunkPtr get_chunk_from_buffer() override;
    size_t num_buffered_chunks() const override;
    size_t buffer_size() const override;
    size_t buffer_capacity() const override;
    size_t default_buffer_capacity() const override;
    ChunkBufferTokenPtr pin_chunk(int num_chunks) override;
    bool is_buffer_full() const override;
    void set_buffer_finished() override;

    OlapMetaScanContextPtr _ctx;
};
} // namespace pipeline
} // namespace starrocks
