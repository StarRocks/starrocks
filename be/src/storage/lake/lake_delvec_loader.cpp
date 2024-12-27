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

#include "storage/lake/lake_delvec_loader.h"

#include "common/logging.h"
#include "common/status.h"
#include "storage/lake/location_provider.h"

namespace starrocks::lake {

Status LakeDelvecLoader::load(const TabletSegmentId& tsid, int64_t version, DelVectorPtr* pdelvec) {
    if (_pk_builder != nullptr) {
        // 1. find in meta builder first
        auto found = _pk_builder->find_delvec(tsid, pdelvec);
        if (!found.ok()) {
            return found.status();
        }
        if (*found) {
            return Status::OK();
        }
    }
    return load_from_file(tsid, version, pdelvec);
}

Status LakeDelvecLoader::load_from_file(const TabletSegmentId& tsid, int64_t version, DelVectorPtr* pdelvec) {
    (*pdelvec).reset(new DelVector());
    // 2. find in delvec file
    ASSIGN_OR_RETURN(auto metadata,
                     _tablet_manager->get_tablet_metadata(tsid.tablet_id, version, _fill_cache, 0, _lake_io_opts.fs));
    RETURN_IF_ERROR(
            lake::get_del_vec(_tablet_manager, *metadata, tsid.segment_id, _fill_cache, _lake_io_opts, pdelvec->get()));
    return Status::OK();
}

} // namespace starrocks::lake
