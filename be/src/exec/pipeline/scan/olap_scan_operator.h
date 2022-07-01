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
    OlapScanOperatorFactory(int32_t id, ScanNode* scan_node, OlapScanContextPtr ctx);

    ~OlapScanOperatorFactory() override = default;

    Status do_prepare(RuntimeState* state) override;
    void do_close(RuntimeState* state) override;
    OperatorPtr do_create(int32_t dop, int32_t driver_sequence) override;

private:
    OlapScanContextPtr _ctx;
};

class OlapScanOperator final : public ScanOperator {
public:
    OlapScanOperator(OperatorFactory* factory, int32_t id, int32_t driver_sequence, ScanNode* scan_node,
                     std::atomic<int>& num_committed_scan_tasks, OlapScanContextPtr ctx);

    ~OlapScanOperator() override;

    bool has_output() const override;
    bool is_finished() const override;

    Status do_prepare(RuntimeState* state) override;
    void do_close(RuntimeState* state) override;
    ChunkSourcePtr create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) override;

    size_t max_scan_concurrency() const override;

protected:
    void attach_chunk_source(int32_t source_index) override;
    void detach_chunk_source(int32_t source_index) override;
    bool has_shared_chunk_source() const override;
    bool has_buffer_output() const override;
    bool has_available_buffer() const override;
    ChunkPtr get_chunk_from_buffer() override;

private:
    size_t _avg_max_scan_concurrency() const;

private:
    OlapScanContextPtr _ctx;

    const size_t _default_max_scan_concurrency;
    // These three fields are used to calculate the average of different `max_scan_concurrency`s for profile.
    mutable size_t _prev_max_scan_concurrency = 0;
    mutable size_t _sum_max_scan_concurrency = 0;
    mutable size_t _num_max_scan_concurrency = 0;
};

} // namespace pipeline
} // namespace starrocks
