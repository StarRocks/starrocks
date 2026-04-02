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

#include <future>
#include <vector>

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
    // 2. check preloaded delvecs (populated by batch_load)
    auto it = _preloaded_delvecs.find(tsid.segment_id);
    if (it != _preloaded_delvecs.end()) {
        *pdelvec = it->second;
        return Status::OK();
    }
    return load_from_file(tsid, version, pdelvec);
}

Status LakeDelvecLoader::load_from_meta(const TabletMetadataPtr& metadata, const DelvecPagePB& delvec_page,
                                        DelVectorPtr* pdelvec) {
    *pdelvec = std::make_shared<DelVector>();
    return lake::get_del_vec(_tablet_manager, *metadata, delvec_page, _fill_cache, _lake_io_opts, pdelvec->get());
}

Status LakeDelvecLoader::load_from_file(const TabletSegmentId& tsid, int64_t version, DelVectorPtr* pdelvec) {
    *pdelvec = std::make_shared<DelVector>();
    // 2. find in delvec file
    TabletMetadataPtr metadata;
    if (_lake_io_opts.location_provider) {
        const std::string filepath = _lake_io_opts.location_provider->tablet_metadata_location(tsid.tablet_id, version);
        ASSIGN_OR_RETURN(metadata, _tablet_manager->get_tablet_metadata(filepath, _fill_cache, 0, _lake_io_opts.fs));
    } else {
        ASSIGN_OR_RETURN(metadata, _tablet_manager->get_tablet_metadata(tsid.tablet_id, version, _fill_cache, 0,
                                                                        _lake_io_opts.fs));
    }

    RETURN_IF_ERROR(
            lake::get_del_vec(_tablet_manager, *metadata, tsid.segment_id, _fill_cache, _lake_io_opts, pdelvec->get()));
    return Status::OK();
}

Status LakeDelvecLoader::batch_load(int64_t tablet_id, int64_t version,
                                    const std::unordered_set<uint32_t>& segment_ids) {
    if (segment_ids.empty()) {
        return Status::OK();
    }

    // Load metadata once (will be cached by tablet_manager for subsequent calls)
    TabletMetadataPtr metadata;
    if (_lake_io_opts.location_provider) {
        const std::string filepath = _lake_io_opts.location_provider->tablet_metadata_location(tablet_id, version);
        ASSIGN_OR_RETURN(metadata, _tablet_manager->get_tablet_metadata(filepath, _fill_cache, 0, _lake_io_opts.fs));
    } else {
        ASSIGN_OR_RETURN(metadata,
                         _tablet_manager->get_tablet_metadata(tablet_id, version, _fill_cache, 0, _lake_io_opts.fs));
    }

    // Filter to segment IDs that have delvecs in metadata
    std::vector<uint32_t> ids_to_load;
    ids_to_load.reserve(segment_ids.size());
    for (uint32_t seg_id : segment_ids) {
        if (_pk_builder != nullptr) {
            // Skip segments whose delvecs are in the current publish's MetaFileBuilder
            DelVectorPtr tmp;
            auto found = _pk_builder->find_delvec({tablet_id, seg_id}, &tmp);
            if (found.ok() && *found) {
                _preloaded_delvecs[seg_id] = std::move(tmp);
                continue;
            }
        }
        if (metadata->delvec_meta().delvecs().count(seg_id) > 0) {
            ids_to_load.push_back(seg_id);
        } else {
            // No delvec for this segment — store an empty one
            _preloaded_delvecs[seg_id] = std::make_shared<DelVector>();
        }
    }

    if (ids_to_load.empty()) {
        return Status::OK();
    }

    // Launch concurrent loads using std::async. Each load does one small
    // range read (~2-3ms) from object storage. Running them concurrently
    // overlaps the HTTP round-trip latency.
    struct LoadResult {
        uint32_t segment_id;
        DelVectorPtr delvec;
        Status status;
    };

    std::vector<std::future<LoadResult>> futures;
    futures.reserve(ids_to_load.size());

    for (uint32_t seg_id : ids_to_load) {
        futures.push_back(std::async(std::launch::async, [this, &metadata, seg_id]() -> LoadResult {
            LoadResult result;
            result.segment_id = seg_id;
            result.delvec = std::make_shared<DelVector>();
            result.status = lake::get_del_vec(_tablet_manager, *metadata, seg_id, _fill_cache, _lake_io_opts,
                                              result.delvec.get());
            return result;
        }));
    }

    // Collect results
    for (auto& f : futures) {
        auto result = f.get();
        RETURN_IF_ERROR(result.status);
        _preloaded_delvecs[result.segment_id] = std::move(result.delvec);
    }

    VLOG(1) << "batch_load: loaded " << _preloaded_delvecs.size() << " delvecs for tablet " << tablet_id << " ("
            << ids_to_load.size() << " from file, " << (segment_ids.size() - ids_to_load.size())
            << " from builder/empty)";
    return Status::OK();
}

} // namespace starrocks::lake
