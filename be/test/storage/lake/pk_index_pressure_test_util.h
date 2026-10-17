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

#include <fmt/format.h>

#include <random>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "fs/fs.h"
#include "gen_cpp/lake_types.pb.h"
#include "gen_cpp/types.pb.h"
#include "storage/sstable/comparator.h"
#include "storage/sstable/format.h"
#include "storage/sstable/table_builder.h"

namespace starrocks::lake {

struct PressureSstableFixture {
    PersistentIndexSstablePB pb;
    std::vector<std::string> keys;
    size_t bulk_bytes = 0;
    size_t minimum_bytes = 0;
};

// An old-format compressed index whose decoded output fits the budget but whose encoded and
// decoded overlap does not. Keep real full separator keys to obtain a large index with few rows.
inline PressureSstableFixture make_large_index_fixture(const std::string& path, const std::string& filename) {
    class FullKeyComparator final : public sstable::Comparator {
    public:
        int Compare(const Slice& a, const Slice& b) const override { return a.compare(b); }
        const char* Name() const override { return "test.full-keys"; }
        void FindShortestSeparator(std::string*, const Slice&) const override {}
        void FindShortSuccessor(std::string*) const override {}
    } comparator;
    PressureSstableFixture fixture;
    ASSIGN_OR_ABORT(auto file, fs::new_writable_file(path));
    sstable::Options options;
    options.block_size = 64;
    options.comparator = &comparator;
    sstable::TableBuilder builder(options, file.get());
    std::mt19937 rng(290448);
    for (uint32_t i = 0; i < 8192; ++i) {
        std::string random(128, '\0');
        for (char& byte : random) byte = static_cast<char>(rng());
        fixture.keys.push_back(fmt::format("{:08x}", i) + random + random);
        IndexValuesWithVerPB value;
        auto* entry = value.add_values();
        entry->set_version(1);
        entry->set_rssid(0);
        entry->set_rowid(i);
        CHECK_OK(builder.Add(fixture.keys.back(), value.SerializeAsString()));
    }
    CHECK_OK(builder.Finish());
    fixture.pb.set_filename(filename);
    fixture.pb.set_filesize(builder.FileSize());
    fixture.pb.set_max_rss_rowid(uint64_t{100} << 32);
    CHECK_OK(file->close());
    ASSIGN_OR_ABORT(auto reader, fs::new_random_access_file(path));
    char bytes[sstable::Footer::kEncodedLength];
    CHECK_OK(reader->read_at_fully(fixture.pb.filesize() - sizeof(bytes), bytes, sizeof(bytes)));
    Slice input(bytes, sizeof(bytes));
    sstable::Footer footer;
    CHECK_OK(footer.DecodeFrom(&input));
    sstable::BlockReadMemory memory;
    CHECK_OK(sstable::ProbeBlockMemory(reader.get(), footer.index_handle(), &memory));
    fixture.bulk_bytes = memory.bulk_bytes() + 4096;
    fixture.minimum_bytes = memory.minimum_open_bytes() + 4096;
    return fixture;
}

} // namespace starrocks::lake
