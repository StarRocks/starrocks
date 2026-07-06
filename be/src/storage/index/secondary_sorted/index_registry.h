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

#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace starrocks::secondary_sorted {

// One secondary index definition: which columns make up the index.
struct SecondaryIndexDef {
    std::string index_name;
    std::vector<std::string> index_col_names;
};

// SecondaryIndexRegistry parses config::secondary_index_defs and answers
// per-tablet lookups. The config string format is:
//   "tablet_id:index_name:col1,col2;tablet_id:index_name:col"
// Each ';'-separated entry registers one index. Repeating the tablet_id
// prefix registers multiple indexes on the same tablet. Whitespace around
// delimiters is tolerated. A blank or invalid string makes the registry
// empty.
//
// The registry is a process-wide singleton, repopulated on demand from the
// current config snapshot. Keyed by tablet_id because Lake metadata commits
// are tablet-scoped and we want to keep the hook sites trivial.
class SecondaryIndexRegistry {
public:
    // Look up all index definitions registered for a given tablet. Returns
    // an empty vector if no entry is configured. Triggers a (cheap) reparse
    // if the config string changed since the last call.
    static std::vector<SecondaryIndexDef> get_for_tablet(int64_t tablet_id);

    // For tests / introspection.
    static void force_reload();

private:
    static SecondaryIndexRegistry& instance();

    void maybe_reload();
    std::vector<SecondaryIndexDef> lookup(int64_t tablet_id);

    static std::vector<std::string> split(std::string_view s, char delim);
    static std::string trim(std::string_view s);

    std::mutex _mutex;
    // Snapshot of the config string that produced _by_tablet. We reload when
    // it diverges from the live config.
    std::string _last_seen_config;
    std::unordered_map<int64_t, std::vector<SecondaryIndexDef>> _by_tablet;
};

} // namespace starrocks::secondary_sorted
