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

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "base/testutil/assert.h"
#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "gen_cpp/segment.pb.h"
#include "gen_cpp/types.pb.h"
#include "storage/lake/index_file_reader.h"
#include "storage/lake/index_file_writer.h"

namespace starrocks::lake {

// Memory-backed round-trip test: populate an IndexFileFooterPB via
// IndexFileWriter, read it back via IndexFileReader, confirm lookups
// resolve to the right ColumnIndexMetaPB instances. This isolates the
// .idx file format from FileSystem / SegmentWriter integration.
class IndexFileWriterReaderTest : public ::testing::Test {
protected:
    static ColumnIndexMetaPB make_bitmap_meta(int32_t offset_hint) {
        // We don't need a fully valid BitmapIndexPB here — the round-trip
        // just verifies bytes survive encode/decode. Use bitmap_type as a
        // stable discriminator to differentiate entries.
        ColumnIndexMetaPB meta;
        meta.set_type(BITMAP_INDEX);
        auto* b = meta.mutable_bitmap_index();
        b->set_bitmap_type(BitmapIndexPB::ROARING_BITMAP);
        b->set_has_null(offset_hint % 2 == 0);
        return meta;
    }
};

TEST_F(IndexFileWriterReaderTest, RoundTripMultipleEntries) {
    MemoryFileSystem fs;
    auto* fsp = &fs;
    ASSERT_OK(fsp->create_dir_recursive("/idx"));
    const std::string path = "/idx/test.idx";

    // Writer: three entries spanning two (col_uid, index_type) pairs to
    // verify the reader's linear find covers all of them.
    {
        ASSIGN_OR_ABORT(auto wfile, fsp->new_writable_file(path));
        IndexFileWriter w(std::move(wfile));
        w.append_column_index(5, BITMAP, make_bitmap_meta(0));
        w.append_column_index(7, BITMAP, make_bitmap_meta(1));
        w.append_column_index(7, NGRAMBF, make_bitmap_meta(2));
        ASSERT_OK(w.finalize());
        EXPECT_GT(w.file_size(), 0U);
        EXPECT_EQ(3, w.num_entries());
    }

    // Reader: round-trip the footer and resolve each (col, type).
    {
        ASSIGN_OR_ABORT(auto rfile, fsp->new_random_access_file(path));
        IndexFileReader r;
        ASSERT_OK(r.init(rfile.get()));
        EXPECT_EQ(3, r.num_entries());

        const auto* m = r.find(5, BITMAP);
        ASSERT_NE(nullptr, m);
        EXPECT_EQ(BITMAP_INDEX, m->type());
        EXPECT_TRUE(m->bitmap_index().has_null());

        m = r.find(7, BITMAP);
        ASSERT_NE(nullptr, m);
        EXPECT_FALSE(m->bitmap_index().has_null());

        m = r.find(7, NGRAMBF);
        ASSERT_NE(nullptr, m);
        EXPECT_TRUE(m->bitmap_index().has_null());

        // Missing entries should return nullptr rather than crash.
        EXPECT_EQ(nullptr, r.find(99, BITMAP));
        EXPECT_EQ(nullptr, r.find(5, NGRAMBF));
    }
}

TEST_F(IndexFileWriterReaderTest, BadMagicIsCorruption) {
    MemoryFileSystem fs;
    auto* fsp = &fs;
    ASSERT_OK(fsp->create_dir_recursive("/idx"));
    const std::string path = "/idx/bad.idx";
    // Write 8 bytes of junk that happen to be the wrong magic.
    {
        ASSIGN_OR_ABORT(auto wfile, fsp->new_writable_file(path));
        ASSERT_OK(wfile->append(Slice("garbageXX")));
        ASSERT_OK(wfile->close());
    }
    ASSIGN_OR_ABORT(auto rfile, fsp->new_random_access_file(path));
    IndexFileReader r;
    auto st = r.init(rfile.get());
    EXPECT_TRUE(st.is_corruption()) << st;
}

TEST_F(IndexFileWriterReaderTest, TooSmallIsCorruption) {
    MemoryFileSystem fs;
    auto* fsp = &fs;
    ASSERT_OK(fsp->create_dir_recursive("/idx"));
    const std::string path = "/idx/tiny.idx";
    {
        ASSIGN_OR_ABORT(auto wfile, fsp->new_writable_file(path));
        ASSERT_OK(wfile->append(Slice("x")));
        ASSERT_OK(wfile->close());
    }
    ASSIGN_OR_ABORT(auto rfile, fsp->new_random_access_file(path));
    IndexFileReader r;
    auto st = r.init(rfile.get());
    EXPECT_TRUE(st.is_corruption()) << st;
}

TEST_F(IndexFileWriterReaderTest, FinalizeTwiceFails) {
    MemoryFileSystem fs;
    auto* fsp = &fs;
    ASSERT_OK(fsp->create_dir_recursive("/idx"));
    const std::string path = "/idx/double.idx";
    ASSIGN_OR_ABORT(auto wfile, fsp->new_writable_file(path));
    IndexFileWriter w(std::move(wfile));
    w.append_column_index(1, BITMAP, make_bitmap_meta(0));
    ASSERT_OK(w.finalize());
    auto st = w.finalize();
    EXPECT_FALSE(st.ok());
}

} // namespace starrocks::lake
