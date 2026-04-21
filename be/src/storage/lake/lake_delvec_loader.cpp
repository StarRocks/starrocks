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
#include "fmt/format.h"
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

    // Look up delvec page for this segment
    auto iter = metadata->delvec_meta().delvecs().find(tsid.segment_id);
    if (iter == metadata->delvec_meta().delvecs().end()) {
        return Status::OK(); // no delvec for this segment
    }
    const auto& delvec_page = iter->second;

    // Try to read from file cache (avoids per-page remote IO)
    ASSIGN_OR_RETURN(auto page_data, _read_delvec_page(metadata, delvec_page));
    RETURN_IF_ERROR((*pdelvec)->load(delvec_page.version(), page_data.data(), page_data.size()));
    return Status::OK();
}

StatusOr<std::string> LakeDelvecLoader::_read_delvec_page(const TabletMetadataPtr& metadata,
                                                          const DelvecPagePB& delvec_page) {
    // Look up which file this delvec page belongs to
    auto ver_it = metadata->delvec_meta().version_to_file().find(delvec_page.version());
    if (ver_it == metadata->delvec_meta().version_to_file().end()) {
        return Status::InternalError(
                fmt::format("Can't find delvec file for tablet {}, version {}", metadata->id(), delvec_page.version()));
    }
    const std::string& delvec_name = ver_it->second.name();

    // Check file-level cache
    auto cache_it = _delvec_file_cache.find(delvec_name);
    if (cache_it == _delvec_file_cache.end()) {
        // Cache miss: read the entire delvec file into memory
        RandomAccessFileOptions opts{.skip_fill_local_cache = !_lake_io_opts.fill_data_cache};
        std::unique_ptr<RandomAccessFile> rf;
        if (_lake_io_opts.fs && _lake_io_opts.location_provider) {
            ASSIGN_OR_RETURN(
                    rf, _lake_io_opts.fs->new_random_access_file(
                                opts, _lake_io_opts.location_provider->delvec_location(metadata->id(), delvec_name)));
        } else {
            ASSIGN_OR_RETURN(rf, fs::new_random_access_file(
                                         opts, _tablet_manager->delvec_location(metadata->id(), delvec_name)));
        }
        ASSIGN_OR_RETURN(auto content, rf->read_all());
        cache_it = _delvec_file_cache.emplace(delvec_name, std::move(content)).first;
    }

    // Extract the requested page from the cached file content
    const auto& file_content = cache_it->second;
    if (delvec_page.offset() + delvec_page.size() > file_content.size()) {
        return Status::Corruption(fmt::format(
                "Delvec page out of bounds: offset={}, size={}, file_size={}, tablet={}, file={}", delvec_page.offset(),
                delvec_page.size(), file_content.size(), metadata->id(), delvec_name));
    }
    return file_content.substr(delvec_page.offset(), delvec_page.size());
}

} // namespace starrocks::lake
