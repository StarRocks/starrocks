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

// Models the local-cache corruption behind the tablet-40561106 duplicate-PK
// incident. Lake tablet metadata is stored with ProtobufFile, which has NO
// whole-file checksum: load() only does ParseFromString and reports Corruption
// iff that fails (see protobuf_file.cpp ProtobufFile::load). So a node-local
// cache copy whose raw bytes get damaged -- as starcache can do on repeated
// writes -- may still parse as a perfectly valid protobuf while silently
// missing a delete-vector page.
//
// This test damages bytes of a real serialized metadata file (blind in-place
// corruption + truncation) and shows there exist corruptions that:
//   * are NOT rejected by load() (parse OK, not Corruption), AND
//   * keep the rowset list intact, BUT
//   * drop rowset 2263372's delvec page from delvec_meta.
// That is exactly the metadata the index rebuild loaded -> get_del_vec(2263372)
// returns empty -> row 211 is re-inserted -> duplicate key.

#include <gtest/gtest.h>

#include <fstream>

#include "base/testutil/assert.h"
#include "fs/fs_util.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/protobuf_file.h"

namespace starrocks {

namespace {
constexpr uint32_t kRowsetNew = 2263371;
constexpr uint32_t kRowsetCompact = 2263372; // the delvec page that went missing
constexpr int64_t kVersion = 2323783;

DelvecPagePB make_page(int64_t version, uint64_t offset, uint64_t size, uint32_t crc) {
    DelvecPagePB p;
    p.set_version(version);
    p.set_offset(offset);
    p.set_size(size);
    p.set_crc32c(crc);
    return p;
}

TabletMetadataPB make_metadata() {
    TabletMetadataPB full;
    full.set_id(40561106);
    full.set_version(kVersion);
    full.add_rowsets()->set_id(kRowsetNew);
    full.add_rowsets()->set_id(kRowsetCompact);
    auto* dm = full.mutable_delvec_meta();
    (*dm->mutable_delvecs())[kRowsetNew] = make_page(kVersion, 0, 64, 0x11111111);
    (*dm->mutable_delvecs())[kRowsetCompact] = make_page(kVersion, 64, 109, 0x5c0f5a90);
    return full;
}

// How a damaged buffer looks after the exact parse load() performs.
struct Outcome {
    bool parses = false;       // ParseFromString succeeded == load() returns OK
    int rowsets = 0;           // rowset list size
    bool has_compact_page = false; // delvec_meta still carries rowset 2263372's page
    int delvecs = 0;
};
Outcome classify(const std::string& bytes) {
    TabletMetadataPB m;
    if (!m.ParseFromString(bytes)) return {};
    Outcome o;
    o.parses = true;
    o.rowsets = m.rowsets_size();
    o.delvecs = m.delvec_meta().delvecs().size();
    o.has_compact_page = m.delvec_meta().delvecs().contains(kRowsetCompact);
    return o;
}

// The dangerous class: load() would accept it (parses), the rowset list is
// fully intact, yet rowset 2263372's delvec page is gone -- the silent
// valid-but-incomplete metadata the rebuild consumed.
bool is_dangerous(const Outcome& o) {
    return o.parses && o.rowsets == 2 && !o.has_compact_page;
}
} // namespace

class LakeMetadataCorruptionTest : public ::testing::Test {
public:
    void SetUp() override {
        _dir = "./lake_metadata_corruption_test";
        (void)fs::remove_all(_dir);
        ASSERT_OK(fs::create_directories(_dir));
    }
    void TearDown() override { (void)fs::remove_all(_dir); }

protected:
    // Write raw (possibly corrupted) bytes to a file.
    void write_raw(const std::string& path, const std::string& bytes) {
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        ASSERT_TRUE(out.good());
    }
    std::string _dir;
};

// Blind in-place single-byte corruption: flip bytes at each position and check
// what load() would do. We expect a non-trivial number of positions where the
// file still parses cleanly but rowset 2263372's delvec page is silently lost.
TEST_F(LakeMetadataCorruptionTest, in_place_byte_corruption_silently_drops_delvec_page) {
    const TabletMetadataPB full = make_metadata();
    std::string clean;
    ASSERT_TRUE(full.SerializeToString(&clean));
    ASSERT_TRUE(is_dangerous(classify(clean)) == false); // sanity: clean copy is fine
    ASSERT_TRUE(classify(clean).has_compact_page);

    int parse_fail = 0;     // protobuf itself rejected it (would still NOT be a checksum, but rejected)
    int parse_ok_same = 0;  // parsed, delvec page still present
    int dangerous = 0;      // parsed, rowsets intact, delvec page lost  <-- the incident shape
    std::vector<size_t> dangerous_offsets;

    // Try a few mutations per byte so we exercise tag/length/value damage.
    const std::vector<uint8_t> mutations = {0x00, 0xFF, 0x80, 0x01};
    for (size_t i = 0; i < clean.size(); ++i) {
        const uint8_t orig = static_cast<uint8_t>(clean[i]);
        for (uint8_t mv : mutations) {
            if (mv == orig) continue;
            std::string damaged = clean;
            damaged[i] = static_cast<char>(mv);
            Outcome o = classify(damaged);
            if (!o.parses) {
                ++parse_fail;
            } else if (is_dangerous(o)) {
                ++dangerous;
                if (dangerous_offsets.size() < 8) dangerous_offsets.push_back(i);
            } else {
                ++parse_ok_same;
            }
        }
    }

    std::cerr << "[corruption sweep] file_size=" << clean.size() << " parse_fail=" << parse_fail
              << " parse_ok_other=" << parse_ok_same << " dangerous(parse_ok+rowsets_intact+page_lost)="
              << dangerous << std::endl;

    // The point: such silent-loss corruptions DO exist on this format.
    ASSERT_GT(dangerous, 0) << "expected at least one byte corruption that parses cleanly yet loses "
                               "rowset 2263372's delvec page";

    // Drive ONE of them through the real load() path end to end, to prove load()
    // returns OK (not Corruption) on the damaged file and yields the incomplete meta.
    const size_t off = dangerous_offsets.front();
    std::string damaged = clean;
    damaged[off] ^= 0xFF; // a concrete flip; exact value doesn't matter, the position does
    Outcome chosen = classify(damaged);
    if (!is_dangerous(chosen)) {
        // pick a mutation at this offset that is dangerous
        for (uint8_t mv : mutations) {
            std::string d = clean;
            d[off] = static_cast<char>(mv);
            if (is_dangerous(classify(d))) {
                damaged = d;
                break;
            }
        }
    }

    const std::string bad_path = _dir + "/corrupted.meta";
    write_raw(bad_path, damaged);

    TabletMetadataPB loaded;
    auto st = ProtobufFile(bad_path).load(&loaded);
    ASSERT_TRUE(st.ok()) << "ProtobufFile has no whole-file checksum -> damaged-but-parseable metadata "
                            "must load as OK; got: "
                         << st;
    EXPECT_EQ(2, loaded.rowsets_size()) << "rowset list should survive the corruption";
    EXPECT_FALSE(loaded.delvec_meta().delvecs().contains(kRowsetCompact))
            << "rowset 2263372's delvec page should be silently gone";
    std::cerr << "[end-to-end] corrupted byte at offset " << off
              << " -> load() OK, rowsets=" << loaded.rowsets_size()
              << ", delvecs=" << loaded.delvec_meta().delvecs().size() << std::endl;
}

// The most common starcache failure shape: a short/partial copy (truncation).
// Truncating at the delvec_meta field boundary leaves a valid protobuf with the
// rowset list intact and NO delvec_meta at all -> get_del_vec returns empty.
TEST_F(LakeMetadataCorruptionTest, truncation_before_delvec_meta_parses_with_no_delvec) {
    const TabletMetadataPB full = make_metadata();
    std::string clean;
    ASSERT_TRUE(full.SerializeToString(&clean));

    // Locate the delvec_meta (field 7) region: its payload appears verbatim in
    // the serialized message; the field tag (0x3A) precedes its length varint.
    const std::string dm = full.delvec_meta().SerializeAsString();
    const auto dm_pos = clean.find(dm);
    ASSERT_NE(dm_pos, std::string::npos);
    // Walk back over the length varint (bytes with high bit set) to the 0x3A tag.
    size_t cut = dm_pos;
    while (cut > 0 && (static_cast<uint8_t>(clean[cut - 1]) & 0x80)) --cut; // varint continuation bytes
    if (cut > 0) --cut;                                                     // first/low varint byte
    ASSERT_GT(cut, 0u);
    ASSERT_EQ(static_cast<uint8_t>(clean[cut - 1]), 0x3A) << "expected delvec_meta (field 7) tag before length";
    const size_t tag_pos = cut - 1;

    const std::string truncated = clean.substr(0, tag_pos); // drop delvec_meta and everything after

    const std::string path = _dir + "/truncated.meta";
    write_raw(path, truncated);

    TabletMetadataPB loaded;
    auto st = ProtobufFile(path).load(&loaded);
    ASSERT_TRUE(st.ok()) << "truncation at a field boundary still parses; got: " << st;
    EXPECT_EQ(2, loaded.rowsets_size());
    EXPECT_EQ(kVersion, loaded.version());
    EXPECT_EQ(0, loaded.delvec_meta().delvecs().size()) << "delvec_meta should be entirely absent";
}

} // namespace starrocks
