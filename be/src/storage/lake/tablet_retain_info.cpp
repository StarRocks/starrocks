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

#include "storage/lake/tablet_retain_info.h"

#include "common/status.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"

namespace starrocks::lake {

Status TabletRetainInfo::init(int64_t tablet_id, const std::unordered_set<int64_t>& retain_versions,
                              TabletManager* tablet_mgr) {
    _tablet_id = tablet_id;
    for (const auto& version : retain_versions) {
        auto res = tablet_mgr->get_tablet_metadata(tablet_id, version, false);
        if (!res.status().ok()) {
            return res.status();
        }
        auto metadata = std::move(res).value();
        for (const auto& rowset : metadata->rowsets()) {
            _rowset_ids.insert(rowset.id());
        }

        for (const auto& [_, dcg] : (*metadata).dcg_meta().dcgs()) {
            for (const auto& column_file : dcg.column_files()) {
                _files.insert(column_file);
            }
        }
        for (const auto& [_, file] : (*metadata).delvec_meta().version_to_file()) {
            _files.insert(file.name());
        }
        for (const auto& sstable : (*metadata).sstable_meta().sstables()) {
            _files.insert(sstable.filename());
        }

        _versions.insert(version);
    }
    return Status::OK();
}

bool TabletRetainInfo::contains_file(const std::string& file_name) const {
    return _files.find(file_name) != _files.end();
}

bool TabletRetainInfo::contains_version(int64_t version) const {
    return _versions.find(version) != _versions.end();
}

bool TabletRetainInfo::contains_rowset(uint32_t rowset_id) const {
    return _rowset_ids.find(rowset_id) != _rowset_ids.end();
}

} // namespace starrocks::lake
