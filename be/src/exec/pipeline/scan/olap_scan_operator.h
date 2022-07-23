// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

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

class OlapScanOperatorFactory final : public ScanOperatorFactory {
public:
    OlapScanOperatorFactory(int32_t id, ScanNode* scan_node, ChunkBufferLimiterPtr buffer_limiter,
                            OlapScanContextPtr ctx);

    ~OlapScanOperatorFactory() override = default;

    Status do_prepare(RuntimeState* state) override;
    void do_close(RuntimeState* state) override;
    OperatorPtr do_create(int32_t dop, int32_t driver_sequence) override;

private:
    OlapScanContextPtr _ctx;
};

class OlapScanOperator final : public ScanOperator {
public:
<<<<<<< HEAD
    OlapScanOperator(OperatorFactory* factory, int32_t id, int32_t driver_sequence, ScanNode* scan_node,
                     ChunkBufferLimiter* buffer_limiter, OlapScanContextPtr ctx);
=======
    OlapScanOperator(OperatorFactory* factory, int32_t id, int32_t driver_sequence, int32_t dop, ScanNode* scan_node,
                     OlapScanContextPtr ctx);
>>>>>>> 509a3b786 ([Enhance] introduce unplug mechanism to improve scalability (#8979))

    ~OlapScanOperator() override;

    bool has_output() const override;
    bool is_finished() const override;

    Status do_prepare(RuntimeState* state) override;
    void do_close(RuntimeState* state) override;
    ChunkSourcePtr create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) override;

<<<<<<< HEAD
=======
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

>>>>>>> 509a3b786 ([Enhance] introduce unplug mechanism to improve scalability (#8979))
private:
    OlapScanContextPtr _ctx;
};

} // namespace pipeline
} // namespace starrocks
