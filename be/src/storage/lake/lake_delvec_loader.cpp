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
    // 2. check preloaded delvecs (populated by preload_delvecs())
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

Status LakeDelvecLoader::preload_delvecs(int64_t tablet_id, int64_t version) {
    // Load tablet metadata for the given version (will be cached by tablet_manager).
    TabletMetadataPtr metadata;
    if (_lake_io_opts.location_provider) {
        const std::string filepath = _lake_io_opts.location_provider->tablet_metadata_location(tablet_id, version);
        ASSIGN_OR_RETURN(metadata, _tablet_manager->get_tablet_metadata(filepath, _fill_cache, 0, _lake_io_opts.fs));
    } else {
        ASSIGN_OR_RETURN(metadata,
                         _tablet_manager->get_tablet_metadata(tablet_id, version, _fill_cache, 0, _lake_io_opts.fs));
    }

    const auto& delvec_meta = metadata->delvec_meta();
    if (delvec_meta.delvecs().empty()) {
        return Status::OK();
    }

    // Group delvec pages by their file (keyed by version which maps to a file).
    std::map<int64_t, std::vector<std::pair<uint32_t, const DelvecPagePB*>>> file_version_to_pages;
    for (const auto& [segment_id, delvec_page] : delvec_meta.delvecs()) {
        file_version_to_pages[delvec_page.version()].emplace_back(segment_id, &delvec_page);
    }

    // For each unique delvec file, read it once and extract all needed pages.
    for (const auto& [file_version, pages] : file_version_to_pages) {
        auto ver_it = delvec_meta.version_to_file().find(file_version);
        if (ver_it == delvec_meta.version_to_file().end()) {
            LOG(WARNING) << "preload_delvecs: can't find delvec file for version " << file_version << ", tablet "
                         << tablet_id << ", skipping";
            continue;
        }
        const auto& file_meta = ver_it->second;
        const std::string delvec_name = file_meta.name();

        // Open and read the entire delvec file.
        RandomAccessFileOptions opts{.skip_fill_local_cache = !_lake_io_opts.fill_data_cache};
        std::unique_ptr<RandomAccessFile> rf;
        if (_lake_io_opts.fs && _lake_io_opts.location_provider) {
            ASSIGN_OR_RETURN(rf,
                             _lake_io_opts.fs->new_random_access_file(
                                     opts, _lake_io_opts.location_provider->delvec_location(tablet_id, delvec_name)));
        } else {
            ASSIGN_OR_RETURN(
                    rf, fs::new_random_access_file(opts, _tablet_manager->delvec_location(tablet_id, delvec_name)));
        }
        ASSIGN_OR_RETURN(auto file_content, rf->read_all());

        // Extract each delvec page from the in-memory buffer.
        for (const auto& [segment_id, page_ptr] : pages) {
            const auto& page = *page_ptr;
            if (page.offset() + page.size() > file_content.size()) {
                LOG(WARNING) << "preload_delvecs: page out of bounds for segment " << segment_id << ", tablet "
                             << tablet_id << ", skipping";
                continue;
            }
            auto delvec = std::make_shared<DelVector>();
            RETURN_IF_ERROR(delvec->load(page.version(), file_content.data() + page.offset(), page.size()));
            _preloaded_delvecs[segment_id] = std::move(delvec);
        }
    }
    VLOG(1) << "preload_delvecs: preloaded " << _preloaded_delvecs.size() << " delvecs for tablet " << tablet_id
            << " version " << version << " from " << file_version_to_pages.size() << " files";
    return Status::OK();
}

} // namespace starrocks::lake
