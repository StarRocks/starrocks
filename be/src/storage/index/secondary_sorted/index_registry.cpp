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

#include "storage/index/secondary_sorted/index_registry.h"

#include <algorithm>
#include <cctype>

#include "common/config.h"
#include "common/logging.h"

namespace starrocks::secondary_sorted {

SecondaryIndexRegistry& SecondaryIndexRegistry::instance() {
    static SecondaryIndexRegistry inst;
    return inst;
}

std::vector<SecondaryIndexDef> SecondaryIndexRegistry::get_for_tablet(int64_t tablet_id) {
    return instance().lookup(tablet_id);
}

void SecondaryIndexRegistry::force_reload() {
    auto& self = instance();
    std::lock_guard<std::mutex> lock(self._mutex);
    self._last_seen_config.clear();
    self._by_tablet.clear();
}

std::vector<SecondaryIndexDef> SecondaryIndexRegistry::lookup(int64_t tablet_id) {
    std::lock_guard<std::mutex> lock(_mutex);
    maybe_reload();
    auto it = _by_tablet.find(tablet_id);
    if (it == _by_tablet.end()) return {};
    return it->second;
}

void SecondaryIndexRegistry::maybe_reload() {
    const std::string& current = config::secondary_index_defs;
    if (current == _last_seen_config) return;
    _last_seen_config = current;
    _by_tablet.clear();
    if (current.empty()) return;

    // Each entry: "tablet_id:index_name:col1,col2"
    for (auto entry : split(current, ';')) {
        entry = trim(entry);
        if (entry.empty()) continue;
        auto parts = split(entry, ':');
        if (parts.size() != 3) {
            LOG(WARNING) << "SecondaryIndexRegistry: skipping malformed entry: '" << entry << "'";
            continue;
        }
        const std::string tablet_id_str = trim(parts[0]);
        const std::string index_name = trim(parts[1]);
        const std::string cols_csv = trim(parts[2]);

        int64_t tablet_id = 0;
        try {
            tablet_id = std::stoll(tablet_id_str);
        } catch (const std::exception&) {
            LOG(WARNING) << "SecondaryIndexRegistry: invalid tablet_id '" << tablet_id_str << "' in entry '" << entry
                         << "'";
            continue;
        }

        SecondaryIndexDef def;
        def.index_name = index_name;
        for (auto col : split(cols_csv, ',')) {
            std::string trimmed = trim(col);
            if (!trimmed.empty()) {
                def.index_col_names.push_back(std::move(trimmed));
            }
        }
        if (def.index_col_names.empty() || def.index_name.empty()) {
            LOG(WARNING) << "SecondaryIndexRegistry: empty name or columns in entry '" << entry << "'";
            continue;
        }
        // Append rather than overwrite: multiple ';'-separated entries with
        // the same tablet_id register that many distinct indexes.
        _by_tablet[tablet_id].push_back(std::move(def));
    }
    size_t total_indexes = 0;
    for (auto& [_, defs] : _by_tablet) total_indexes += defs.size();
    LOG(INFO) << "SecondaryIndexRegistry: loaded " << total_indexes << " index defs across " << _by_tablet.size()
              << " tablets from config";
}

std::vector<std::string> SecondaryIndexRegistry::split(std::string_view s, char delim) {
    std::vector<std::string> out;
    size_t start = 0;
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == delim) {
            out.emplace_back(s.substr(start, i - start));
            start = i + 1;
        }
    }
    out.emplace_back(s.substr(start));
    return out;
}

std::string SecondaryIndexRegistry::trim(std::string_view s) {
    size_t b = 0;
    size_t e = s.size();
    while (b < e && std::isspace(static_cast<unsigned char>(s[b]))) ++b;
    while (e > b && std::isspace(static_cast<unsigned char>(s[e - 1]))) --e;
    return std::string(s.substr(b, e - b));
}

} // namespace starrocks::secondary_sorted
