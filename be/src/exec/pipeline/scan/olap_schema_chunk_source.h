// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/vectorized_fwd.h"
#include "exec/pipeline/scan/balanced_chunk_buffer.h"
#include "exec/pipeline/scan/chunk_source.h"
#include "exec/pipeline/scan/olap_schema_scan_context.h"
#include "exec/pipeline/scan/scan_operator.h"
#include "runtime/runtime_state.h"

namespace starrocks {
namespace vectorized {
class SchemaScannerParam;
class SchemaScanner;
} // namespace vectorized

namespace pipeline {

class OlapSchemaChunkSource final : public ChunkSource {
public:
    OlapSchemaChunkSource(int32_t scan_operator_id, RuntimeProfile* runtime_profile, MorselPtr&& morsel,
                          OlapSchemaScanContextPtr ctx);

    ~OlapSchemaChunkSource() override;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    Status _read_chunk(RuntimeState* state, ChunkPtr* chunk) override;

    const workgroup::WorkGroupScanSchedEntity* _scan_sched_entity(const workgroup::WorkGroup* wg) const override;

    const TupleDescriptor* _dest_tuple_desc;
    std::unique_ptr<vectorized::SchemaScanner> _scanner;

    OlapSchemaScanContextPtr _ctx;

    RuntimeProfile::Counter* _filter_timer = nullptr;
};
} // namespace pipeline
} // namespace starrocks
