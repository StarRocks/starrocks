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

#include <string>
#include <unordered_set>
#include <vector>

#include "storage/lake/tablet_metadata.h"

namespace starrocks {
class Status;
}

namespace starrocks::lake {

class TabletManager;

class TabletRetainInfo {
public:
    TabletRetainInfo() = default;
    ~TabletRetainInfo() = default;

    Status build_info(const std::vector<int64_t>& retain_versions, int64_t tablet_id, TabletManager* tablet_mgr) {
        for (const auto& version : retain_versions) {
            auto res = tablet_mgr->get_tablet_metadata(tablet_id, version, false);
            if (!res.status().ok()) {
                return res.status();
            }
            auto metadata = std::move(res).value();
            for (const auto& rowset : metadata->rowsets()) {
                _rowset_ids.insert(rowset.id());
                for (const auto& segment : rowset.segments()) {
                    _files.insert(segment);
                }
                for (const auto& del_file : rowset.del_files()) {
                    _files.insert(del_file.name());
                }
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

    bool filter_by_file_name(const std::string& file_name) const {
        return _files.find(file_name) != _files.end();
    }

    bool filter_by_version(int64_t version) const {
        return _versions.find(version) != _versions.end();
    }

    bool filter_by_rowset_id(uint32_t rowset_id) const {
        return _rowset_ids.find(rowset_id) != _rowset_ids.end();
    }

private:
    std::unordered_set<int64_t> _versions;
    std::unordered_set<std::string> _files;
    std::unordered_set<uint32_t> _rowset_ids;
};

} // namespace starrocks::lake
