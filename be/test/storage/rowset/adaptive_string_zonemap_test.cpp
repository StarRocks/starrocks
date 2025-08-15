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

#include "storage/rowset/zone_map_index.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "common/config.h"
#include "storage/tablet_schema_helper.h"
#include "util/slice.h"

namespace starrocks {

class AdaptiveStringZoneMapTest : public testing::Test {};

TEST_F(AdaptiveStringZoneMapTest, HighOverlapSkipsWriting) {
    // Set a lower min_pages to keep test small
    config::string_zonemap_min_pages_for_adaptive_check = 5;
    // Consider overlapping if more than half of consecutive pages intersect
    config::string_zonemap_overlap_threshold = 0.5;

    TabletColumn varchar_column = create_varchar_key(0);
    TypeInfoPtr type_info = get_type_info(varchar_column);

    auto writer = ZoneMapIndexWriter::create(type_info.get());

    // Create 6 pages with identical ranges (full overlap among consecutive pages)
    for (int p = 0; p < 6; ++p) {
        std::vector<Slice> values;
        // All values equal ensures min==max, but pages still overlap pairwise
        std::string v = "aaaaa";
        values.emplace_back(v.data(), v.size());
        values.emplace_back(v.data(), v.size());
        writer->add_values(values.data(), values.size());
        ASSERT_TRUE(writer->flush().ok());
    }

    // With high overlap, the heuristic should suggest not to write
    ASSERT_FALSE(writer->should_write_for_strings(config::string_zonemap_overlap_threshold,
                                                  config::string_zonemap_min_pages_for_adaptive_check));
}

TEST_F(AdaptiveStringZoneMapTest, LowOverlapWrites) {
    config::string_zonemap_min_pages_for_adaptive_check = 5;
    config::string_zonemap_overlap_threshold = 0.5;

    TabletColumn varchar_column = create_varchar_key(0);
    TypeInfoPtr type_info = get_type_info(varchar_column);

    auto writer = ZoneMapIndexWriter::create(type_info.get());

    // Create 6 pages with non-overlapping ranges: "a..." then "b..." etc.
    for (int p = 0; p < 6; ++p) {
        char c = static_cast<char>('a' + p);
        std::string minv = std::string(1, c) + "000";
        std::string maxv = std::string(1, c) + "zzz";
        std::vector<Slice> values;
        values.emplace_back(minv.data(), minv.size());
        values.emplace_back(maxv.data(), maxv.size());
        writer->add_values(values.data(), values.size());
        ASSERT_TRUE(writer->flush().ok());
    }

    // With low overlap, should write
    ASSERT_TRUE(writer->should_write_for_strings(config::string_zonemap_overlap_threshold,
                                                 config::string_zonemap_min_pages_for_adaptive_check));
}

} // namespace starrocks