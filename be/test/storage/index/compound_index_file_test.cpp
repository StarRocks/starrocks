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

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <vector>

#include "fs/fs.h"
#include "storage/index/compound_index_common.h"
#include "storage/index/compound_index_file_reader.h"
#include "storage/index/compound_index_file_writer.h"
#include "storage/index/index_descriptor.h"

namespace starrocks {

namespace {

std::string make_tempdir(const std::string& prefix) {
    const char* base = std::getenv("TEST_TMPDIR");
    if (base == nullptr || *base == '\0') {
        base = "/tmp";
    }
    std::string tmpl = std::string(base) + "/" + prefix + "_XXXXXX";
    std::vector<char> buf(tmpl.begin(), tmpl.end());
    buf.push_back('\0');
    char* dir = ::mkdtemp(buf.data());
    if (dir == nullptr) {
        return {};
    }
    return std::string(dir);
}

struct TempDirGuard {
    std::string path;
    ~TempDirGuard() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
};

// Write `content` to `dir/name` and return absolute path.
std::string write_temp_file(const std::string& dir, const std::string& name, const std::string& content) {
    std::string full = dir + "/" + name;
    std::ofstream ofs(full, std::ios::binary);
    ofs.write(content.data(), content.size());
    ofs.close();
    return full;
}

std::string read_slice(const std::string& bin_path, uint64_t offset, uint64_t length) {
    std::ifstream ifs(bin_path, std::ios::binary);
    ifs.seekg(static_cast<std::streamoff>(offset));
    std::string out(length, '\0');
    ifs.read(out.data(), static_cast<std::streamsize>(length));
    return out;
}

// Helpers for building/packing a fixture .idx from in-memory content vectors.
struct FixtureFile {
    std::string name;
    std::string content;
};

std::string pack_fixture(const std::string& tmp,
                         const std::vector<std::pair<int64_t, std::vector<FixtureFile>>>& indices) {
    std::vector<CompoundIndexEntry> entries;
    entries.reserve(indices.size());
    for (const auto& [idx_id, files] : indices) {
        CompoundIndexEntry e;
        e.kind = CompoundIndexKind::INVERTED_TANTIVY;
        e.index_id = idx_id;
        e.suffix = std::to_string(idx_id);
        for (const auto& f : files) {
            CompoundFileRef ref;
            ref.name = f.name;
            ref.local_path = write_temp_file(tmp, std::to_string(idx_id) + "_" + f.name, f.content);
            e.files.push_back(std::move(ref));
        }
        entries.push_back(std::move(e));
    }
    std::string bin_path = tmp + "/out.idx";
    auto fs = *FileSystem::CreateSharedFromString("posix://");
    auto wf = *fs->new_writable_file(bin_path);
    auto st = CompoundIndexFileWriter::pack(entries, wf.get());
    EXPECT_TRUE(st.ok()) << st.message();
    EXPECT_TRUE(wf->close().ok());
    return bin_path;
}

} // namespace

TEST(CompoundIndexFile, SingleEntryRoundTrip) {
    TempDirGuard tg{make_tempdir("compound_single")};
    ASSERT_FALSE(tg.path.empty());

    std::string bin_path =
            pack_fixture(tg.path, {
                                          {1001, {{"meta.json", "{\"v\":1}"}, {"data.idx", std::string(1024, 'A')}}},
                                  });

    auto reader_or = CompoundIndexFileReader::open(bin_path);
    ASSERT_TRUE(reader_or.ok()) << reader_or.status().message();
    auto reader = std::move(reader_or).value();

    ASSERT_EQ(reader->layouts().size(), 1u);
    auto layout_or = reader->find_index(CompoundIndexKind::INVERTED_TANTIVY, 1001);
    ASSERT_TRUE(layout_or.ok());
    auto layout = std::move(layout_or).value();

    EXPECT_EQ(layout.kind, CompoundIndexKind::INVERTED_TANTIVY);
    EXPECT_EQ(layout.index_id, 1001);
    EXPECT_EQ(layout.suffix, "1001");
    ASSERT_EQ(layout.files.size(), 2u);

    // File entries appear in the order they were packed.
    EXPECT_EQ(layout.files[0].name, "meta.json");
    EXPECT_EQ(layout.files[0].length, 7u);
    EXPECT_EQ(layout.files[1].name, "data.idx");
    EXPECT_EQ(layout.files[1].length, 1024u);

    // Read each subfile back through its (offset, length) and verify byte
    // identity with what we packed.
    EXPECT_EQ(read_slice(bin_path, layout.files[0].offset, layout.files[0].length), "{\"v\":1}");
    EXPECT_EQ(read_slice(bin_path, layout.files[1].offset, layout.files[1].length), std::string(1024, 'A'));
}

TEST(CompoundIndexFile, MultipleIndicesPreserveLayout) {
    TempDirGuard tg{make_tempdir("compound_multi")};
    std::string bin_path =
            pack_fixture(tg.path, {
                                          {1001, {{"a.bin", "alpha-content"}, {"b.bin", "beta-content"}}},
                                          {1002, {{"c.bin", "gamma-content"}}},
                                          {1003, {{"d.bin", std::string(4096, 'X')}, {"e.bin", "epsilon"}}},
                                  });

    auto reader = std::move(*CompoundIndexFileReader::open(bin_path));
    EXPECT_EQ(reader->layouts().size(), 3u);

    auto l1 = std::move(*reader->find_index(CompoundIndexKind::INVERTED_TANTIVY, 1001));
    auto l2 = std::move(*reader->find_index(CompoundIndexKind::INVERTED_TANTIVY, 1002));
    auto l3 = std::move(*reader->find_index(CompoundIndexKind::INVERTED_TANTIVY, 1003));

    EXPECT_EQ(l1.files.size(), 2u);
    EXPECT_EQ(l2.files.size(), 1u);
    EXPECT_EQ(l3.files.size(), 2u);

    // Offsets must be monotonically increasing across the data region with
    // no overlap (offset_{i+1} == offset_i + length_i, considering all
    // subfiles concatenated in pack order).
    std::vector<std::pair<uint64_t, uint64_t>> all_segments;
    for (const auto& l : {l1, l2, l3}) {
        for (const auto& f : l.files) {
            all_segments.emplace_back(f.offset, f.length);
        }
    }
    std::sort(all_segments.begin(), all_segments.end());
    for (size_t i = 1; i < all_segments.size(); ++i) {
        EXPECT_EQ(all_segments[i].first, all_segments[i - 1].first + all_segments[i - 1].second)
                << "gap or overlap between subfiles";
    }

    // Spot-check the largest subfile's bytes round-trip.
    EXPECT_EQ(read_slice(bin_path, l3.files[0].offset, l3.files[0].length), std::string(4096, 'X'));
}

TEST(CompoundIndexFile, FindIndexNotFound) {
    TempDirGuard tg{make_tempdir("compound_notfound")};
    std::string bin_path = pack_fixture(tg.path, {{42, {{"only.bin", "x"}}}});
    auto reader = std::move(*CompoundIndexFileReader::open(bin_path));

    auto missing = reader->find_index(CompoundIndexKind::INVERTED_TANTIVY, 9999);
    EXPECT_TRUE(missing.status().is_not_found()) << missing.status().message();

    // Wrong kind for an existing index_id is also a miss.
    auto wrong_kind = reader->find_index(CompoundIndexKind::UNKNOWN, 42);
    EXPECT_TRUE(wrong_kind.status().is_not_found()) << wrong_kind.status().message();
}

TEST(CompoundIndexFile, CorruptedMagicRejected) {
    TempDirGuard tg{make_tempdir("compound_corrupt")};
    std::string bin_path = pack_fixture(tg.path, {{1, {{"f", "data"}}}});

    // Stomp the first 4 bytes (the magic) with garbage.
    {
        std::fstream f(bin_path, std::ios::in | std::ios::out | std::ios::binary);
        const char garbage[4] = {'X', 'X', 'X', 'X'};
        f.write(garbage, 4);
    }
    auto r = CompoundIndexFileReader::open(bin_path);
    ASSERT_FALSE(r.ok());
    EXPECT_TRUE(r.status().is_corruption()) << r.status().message();
    EXPECT_NE(r.status().message().find("bad magic"), std::string::npos);
}

TEST(CompoundIndexFile, EmptyEntriesRejected) {
    TempDirGuard tg{make_tempdir("compound_empty")};
    std::string out_path = tg.path + "/out.idx";
    auto fs = *FileSystem::CreateSharedFromString("posix://");
    auto wf = *fs->new_writable_file(out_path);

    std::vector<CompoundIndexEntry> entries;
    auto st = CompoundIndexFileWriter::pack(entries, wf.get());
    EXPECT_TRUE(st.is_invalid_argument()) << st.message();
}

TEST(CompoundIndexFile, KindFieldPreservedAcrossWriteRead) {
    TempDirGuard tg{make_tempdir("compound_kind")};

    // Pack an INVERTED entry and verify the on-disk kind survives parsing.
    std::vector<CompoundIndexEntry> entries(1);
    entries[0].kind = CompoundIndexKind::INVERTED_TANTIVY;
    entries[0].index_id = 7;
    entries[0].suffix = "u";
    CompoundFileRef ref{"f", write_temp_file(tg.path, "f", "p")};
    entries[0].files.push_back(ref);

    std::string bin_path = tg.path + "/out.idx";
    auto fs = *FileSystem::CreateSharedFromString("posix://");
    auto wf = *fs->new_writable_file(bin_path);
    ASSERT_TRUE(CompoundIndexFileWriter::pack(entries, wf.get()).ok());
    ASSERT_TRUE(wf->close().ok());

    auto reader = std::move(*CompoundIndexFileReader::open(bin_path));
    auto layout = std::move(*reader->find_index(CompoundIndexKind::INVERTED_TANTIVY, 7));
    EXPECT_EQ(layout.kind, CompoundIndexKind::INVERTED_TANTIVY);
}

// Exercise the header-probe grow loop: produce a header that is larger than the
// initial 64 KiB probe so the parser must signal truncation and the reader must
// double the buffer. Each index entry occupies (kind+reserved=4) + index_id(8)
// + suffix(prefix32 + bytes) + num_files(4) + per-file (prefix32 + name + 16) =
// > ~80 bytes; packing ~1500 entries with short names pushes total header past
// 64 KiB but stays well under 16 MiB cap.
TEST(CompoundIndexFile, HeaderLargerThanInitialProbeProbe_GrowsAndSucceeds) {
    TempDirGuard tg{make_tempdir("compound_big_header")};
    ASSERT_FALSE(tg.path.empty());

    std::vector<std::pair<int64_t, std::vector<FixtureFile>>> indices;
    indices.reserve(1500);
    for (int i = 0; i < 1500; ++i) {
        // 1-byte file content keeps the .idx tiny while header rows accumulate.
        indices.emplace_back(i + 1, std::vector<FixtureFile>{{"f", "x"}});
    }
    std::string bin_path = pack_fixture(tg.path, indices);

    auto reader_or = CompoundIndexFileReader::open(bin_path);
    ASSERT_TRUE(reader_or.ok()) << reader_or.status().message();
    auto reader = std::move(reader_or).value();
    EXPECT_EQ(reader->layouts().size(), 1500u);

    auto first = reader->find_index(CompoundIndexKind::INVERTED_TANTIVY, 1);
    auto last = reader->find_index(CompoundIndexKind::INVERTED_TANTIVY, 1500);
    ASSERT_TRUE(first.ok());
    ASSERT_TRUE(last.ok());
    EXPECT_EQ(first.value().files.size(), 1u);
    EXPECT_EQ(last.value().files.size(), 1u);
}

// IndexDescriptor::compound_index_file_path_from_segment is the single source
// of truth for deriving the .idx companion path from a .dat segment file. Vacuum
// and the lake tablet writers all funnel through this helper; cover the
// edge cases that previously lived as inline `substr(0, size-4) + ".idx"`
// snippets at multiple call sites.
TEST(IndexDescriptorPathTest, CompoundIndexPathFromSegment_BasicAndEdges) {
    // Standard local segment file.
    EXPECT_EQ(IndexDescriptor::compound_index_file_path_from_segment("seg_0.dat"), "seg_0.idx");

    // Remote URI with rowset prefix and dots in directory components.
    EXPECT_EQ(IndexDescriptor::compound_index_file_path_from_segment("s3://bucket/path.with.dots/seg_42_3.dat"),
              "s3://bucket/path.with.dots/seg_42_3.idx");

    // No extension: helper falls back to appending ".idx" so callers don't crash
    // on ill-formed input. (vacuum guards via is_segment(); writers via the
    // .dat suffix, so this shape isn't expected in production but the helper
    // must remain total.)
    EXPECT_EQ(IndexDescriptor::compound_index_file_path_from_segment("noextension"), "noextension.idx");

    // Already an .idx: rfind('.') strips the trailing extension and reapplies it.
    EXPECT_EQ(IndexDescriptor::compound_index_file_path_from_segment("seg.idx"), "seg.idx");
}

// IndexDescriptor::lake_compound_index_build_dir owns the lake-mode build path
// layout. Two shapes matter: the production form keyed on tablet_id+txn_id, and
// the legacy fallback for callers that haven't filled them in. SegmentWriter
// must stay agnostic to the format string, so cover the helper directly.
TEST(IndexDescriptorPathTest, LakeCompoundIndexBuildDir_TxnAndFallback) {
    // tablet_id != 0 && txn_id != 0 → per-transaction isolation. The instance_key
    // is appended in lowercase hex so two concurrently-alive SegmentWriter
    // instances for the same (tablet, txn, seg, idx) never share a temp dir.
    EXPECT_EQ(IndexDescriptor::lake_compound_index_build_dir("/tmp/tantivy_tmp", 100, 200, 3, 4, 0xCAFE),
              "/tmp/tantivy_tmp/100_200_3_4_cafe.ivt");

    // tablet_id == 0 → fallback uses instance_key, segment, index_id (decimal,
    // legacy shape unchanged).
    EXPECT_EQ(IndexDescriptor::lake_compound_index_build_dir("/tmp/tantivy_tmp", 0, 200, 3, 4, 0xCAFE),
              "/tmp/tantivy_tmp/51966_3_4.ivt");

    // Different (tablet, txn) yield different paths so retries can't collide.
    EXPECT_NE(IndexDescriptor::lake_compound_index_build_dir("/tmp/tantivy_tmp", 100, 200, 3, 4, 0),
              IndexDescriptor::lake_compound_index_build_dir("/tmp/tantivy_tmp", 100, 201, 3, 4, 0));
}

// Same (tablet, txn, seg, idx) but different SegmentWriter instances (different
// instance_key) MUST resolve to different paths so the two writers' init /
// commit / destructor cleanup never trample each other. This is the load-spill
// finish path that exposed `tantivy: Index already exists` and `.tmpXXX: No
// such file or directory` before the fix.
TEST(IndexDescriptorPathTest, LakeBuildDirIsolatesByInstanceKey) {
    const std::string root = "/tmp/tantivy_tmp";
    const int64_t tablet = 100;
    const int64_t txn = 200;
    const int seg = 0;
    const int64_t idx = 50000;

    auto a = IndexDescriptor::lake_compound_index_build_dir(root, tablet, txn, seg, idx, 0xAAAA);
    auto b = IndexDescriptor::lake_compound_index_build_dir(root, tablet, txn, seg, idx, 0xBBBB);

    EXPECT_NE(a, b);

    // Lowercase hex contract: callers (e.g., diagnostics greps) can rely on the
    // `_<hex>.ivt` suffix.
    EXPECT_TRUE(a.ends_with("_aaaa.ivt")) << a;
    EXPECT_TRUE(b.ends_with("_bbbb.ivt")) << b;

    // Boundary values must not produce malformed output.
    auto zero_key = IndexDescriptor::lake_compound_index_build_dir(root, tablet, txn, seg, idx, 0);
    EXPECT_TRUE(zero_key.ends_with("_0.ivt")) << zero_key;

    auto max_key = IndexDescriptor::lake_compound_index_build_dir(root, tablet, txn, seg, idx, UINTPTR_MAX);
    // UINTPTR_MAX in lowercase hex is all 'f's; just check the suffix shape and
    // that it doesn't equal the zero variant.
    EXPECT_NE(max_key, zero_key);
    EXPECT_TRUE(max_key.ends_with(".ivt")) << max_key;
}

} // namespace starrocks
