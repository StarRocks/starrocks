// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <orc/OrcFile.hh>
#include <stdint.h>
#include <memory>
#include <vector>

#include "env/env.h"
#include "exec/vectorized/file_scanner.h"
#include "exec/vectorized/orc_scanner_adapter.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "common/statusor.h"
#include "udf/udf_internal.h"

namespace orc {
struct ColumnVectorBatch;
}  // namespace orc
namespace starrocks {
class RuntimeProfile;
class RuntimeState;
class SlotDescriptor;
class TBrokerScanRange;
struct TypeDescriptor;
}  // namespace starrocks

namespace starrocks::vectorized {

class ORCScanner : public FileScanner {
public:
    using FillColumnFunction = void (*)(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                                        const TypeDescriptor& slot_desc);

    ORCScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
               starrocks::vectorized::ScannerCounter* counter);

    ~ORCScanner() override = default;

    Status open() override;

    StatusOr<ChunkPtr> get_next() override;

    void close() override;

private:
    Status _next_orc_batch(ChunkPtr* result);

    StatusOr<ChunkPtr> _next_orc_chunk();

    Status _open_next_orc_reader();

    ChunkPtr _create_src_chunk();

    ChunkPtr _transfer_chunk(ChunkPtr& src);

    ChunkPtr _materialize(ChunkPtr& src, ChunkPtr& cast);

private:
    const TBrokerScanRange& _scan_range;
    const uint64_t _max_chunk_size;
    int _next_range;
    int _error_counter;
    bool _status_eof;

    std::unique_ptr<OrcScannerAdapter> _orc_adapter;
    std::vector<SlotDescriptor*> _orc_slot_descriptors;
};

} // namespace starrocks::vectorized
