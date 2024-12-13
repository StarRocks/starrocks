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

<<<<<<< HEAD
namespace starrocks::lake {

class Tablet;
class TxnLogPB;
class TabletMetadataPB;
=======
namespace starrocks {
class TxnLogPB;
class TabletMetadataPB;
} // namespace starrocks

namespace starrocks::lake {

class Tablet;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

class TxnLogApplier {
public:
    virtual ~TxnLogApplier() = default;

    virtual Status init() { return Status::OK(); }

    virtual Status apply(const TxnLogPB& tnx_log) = 0;

    virtual Status finish() = 0;

<<<<<<< HEAD
    virtual std::shared_ptr<std::vector<std::string>> trash_files() = 0;
};

std::unique_ptr<TxnLogApplier> new_txn_log_applier(Tablet tablet, MutableTabletMetadataPtr metadata,
                                                   int64_t new_version);

} // namespace starrocks::lake
=======
    void observe_empty_compaction() { _has_empty_compaction = true; }

protected:
    bool _has_empty_compaction = false;
};

std::unique_ptr<TxnLogApplier> new_txn_log_applier(const Tablet& tablet, MutableTabletMetadataPtr metadata,
                                                   int64_t new_version);

} // namespace starrocks::lake
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
