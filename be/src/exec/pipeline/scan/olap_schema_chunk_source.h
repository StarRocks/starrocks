// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/vectorized_fwd.h"
#include "exec/pipeline/scan/balanced_chunk_buffer.h"
#include "exec/pipeline/scan/chunk_source.h"
#include "exec/pipeline/scan/olap_schema_scan_context.h"
#include "exec/pipeline/scan/scan_operator.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"

namespace starrocks {
namespace vectorized {
class SchemaScannerParam;
class SchemaScanner;
} // namespace vectorized

namespace pipeline {

class OlapSchemaChunkSource final : public ChunkSource {
public:
    OlapSchemaChunkSource(ScanOperator* op, RuntimeProfile* runtime_profile, MorselPtr&& morsel,
                          const OlapSchemaScanContextPtr& ctx);

    ~OlapSchemaChunkSource() override;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    Status _read_chunk(RuntimeState* state, ChunkPtr* chunk) override;

    const workgroup::WorkGroupScanSchedEntity* _scan_sched_entity(const workgroup::WorkGroup* wg) const override;

    const TupleDescriptor* _dest_tuple_desc;
    std::unique_ptr<vectorized::SchemaScanner> _schema_scanner;

    OlapSchemaScanContextPtr _ctx;

    RuntimeProfile::Counter* _filter_timer = nullptr;

    // Because schema_scanner returns column data based on column index.
    // So a mapping relationship between the slot list of schema_scanner and the tuple slot list is needed
    // (it can be understood as the relationship between input slots and output slots of schema_scan)
    std::vector<int> _index_map;

    ChunkAccumulator _accumulator;
};
} // namespace pipeline
} // namespace starrocks
