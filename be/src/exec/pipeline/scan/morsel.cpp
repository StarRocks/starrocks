// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/scan/morsel.h"

#include "storage/tablet_reader_params.h"

namespace starrocks {
namespace pipeline {

void PhysicalSplitScanMorsel::init_tablet_reader_params(vectorized::TabletReaderParams* params) {
    params->rowset_id = _rowset_id;
    params->segment_id = _segment_id;
    params->rowid_range = _rowid_range;
}

} // namespace pipeline
} // namespace starrocks