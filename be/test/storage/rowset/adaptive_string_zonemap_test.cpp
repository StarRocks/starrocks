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
#include "fs/fs_memory.h"
#include "column/binary_column.h"
#include "testutil/assert.h"
#include "storage/rowset/column_writer.h"
#include "storage/tablet_schema.h"
#include "storage/tablet_schema_helper.h"
#include "util/slice.h"

namespace starrocks {

class AdaptiveStringZoneMapTest : public testing::Test {};

TEST_F(AdaptiveStringZoneMapTest, HighOverlapSkipsWriting) {
    // Set a lower min_pages to keep test small
    config::string_zonemap_min_pages_for_adaptive_check = 5;
    // Consider overlapping if more than half of consecutive pages intersect
    config::string_zonemap_overlap_threshold = 0.5;

    // Build a minimal ColumnWriter to exercise per-page sampling path
    TabletColumn varchar_column = create_varchar_key(0);
    ColumnWriterOptions opts;
    ColumnMetaPB meta;
    meta.set_column_id(0);
    meta.set_unique_id(0);
    meta.set_type(varchar_column.type());
    meta.set_length(varchar_column.length());
    meta.set_encoding(DEFAULT_ENCODING);
    meta.set_compression(NO_COMPRESSION);
    meta.set_is_nullable(false);
    opts.meta = &meta;
    opts.need_zone_map = true;
    TypeInfoPtr type_info = get_type_info(varchar_column);

    auto fs = std::make_shared<MemoryFileSystem>();
    ASSERT_TRUE(fs->create_dir("/tmp").ok());
    ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file("/tmp/zm"))

    auto cw = std::make_unique<ScalarColumnWriter>(opts, type_info, wfile.get());
    ASSERT_TRUE(cw->init().ok());

    // Append 6 pages with identical min/max, then finalize each page
    for (int p = 0; p < 6; ++p) {
        BinaryColumn col;
        std::string v = "aaaaa";
        col.append(Slice(v));
        col.append(Slice(v));
        ASSERT_TRUE(cw->append(col).ok());
        ASSERT_TRUE(cw->finish_current_page().ok());
    }

    // When highly overlapping, the zonemap writer should have been disabled; write_zone_map is a no-op
    ASSERT_TRUE(cw->write_zone_map().ok());
}

TEST_F(AdaptiveStringZoneMapTest, LowOverlapWrites) {
    config::string_zonemap_min_pages_for_adaptive_check = 5;
    config::string_zonemap_overlap_threshold = 0.5;

    TabletColumn varchar_column = create_varchar_key(0);
    ColumnWriterOptions opts;
    ColumnMetaPB meta;
    meta.set_column_id(0);
    meta.set_unique_id(0);
    meta.set_type(varchar_column.type());
    meta.set_length(varchar_column.length());
    meta.set_encoding(DEFAULT_ENCODING);
    meta.set_compression(NO_COMPRESSION);
    meta.set_is_nullable(false);
    opts.meta = &meta;
    opts.need_zone_map = true;
    TypeInfoPtr type_info = get_type_info(varchar_column);

    auto fs = std::make_shared<MemoryFileSystem>();
    ASSERT_TRUE(fs->create_dir("/tmp").ok());
    ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file("/tmp/zm2"))

    auto cw = std::make_unique<ScalarColumnWriter>(opts, type_info, wfile.get());
    ASSERT_TRUE(cw->init().ok());

    for (int p = 0; p < 6; ++p) {
        BinaryColumn col;
        char c = static_cast<char>('a' + p);
        std::string minv = std::string(1, c) + "000";
        std::string maxv = std::string(1, c) + "zzz";
        col.append(Slice(minv));
        col.append(Slice(maxv));
        ASSERT_TRUE(cw->append(col).ok());
        ASSERT_TRUE(cw->finish_current_page().ok());
    }

    // Should still write zonemap successfully
    ASSERT_TRUE(cw->write_zone_map().ok());
}

} // namespace starrocks