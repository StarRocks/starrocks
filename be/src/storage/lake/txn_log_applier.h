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

#include <memory>

#include "common/status.h"
#include "gutil/macros.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"

namespace starrocks {
class TxnLogPB;
class TabletMetadataPB;
} // namespace starrocks

namespace starrocks::lake {

class Tablet;

class TxnLogApplier {
public:
    virtual ~TxnLogApplier() = default;

    virtual Status init() { return Status::OK(); }

    virtual Status apply(const TxnLogPB& txn_log) = 0;

    virtual Status apply(const TxnLogVector& txn_logs) = 0;

    virtual Status finish() = 0;

    // Mark this publish iteration as carrying a "no-op apply" — i.e. a txn that
    // produces no rowset changes against the metadata. Used by:
    //   (1) compaction whose txnlog is missing + force_publish=true (legacy
    //       "empty compaction" path), and
    //   (2) admin-issued no-op publish (ADMIN SKIP COMMITTED TRANSACTION,
    //       TxnInfoPB.no_op_publish=true).
    // The flag drives downstream PK persistent-index handling in finish() to
    // still load the primary index even when no rowsets changed, so a later
    // real compaction does not skip due to a stale in-memory index.
    void observe_no_op_apply() { _has_no_op_apply = true; }

protected:
    bool _has_no_op_apply = false;
    bool _skip_write_tablet_metadata = false;
};

std::unique_ptr<TxnLogApplier> new_txn_log_applier(const Tablet& tablet, MutableTabletMetadataPtr metadata,
                                                   int64_t new_version, bool rebuild_pindex,
                                                   bool skip_write_tablet_metadata);

} // namespace starrocks::lake
