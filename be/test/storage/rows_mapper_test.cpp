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

#include "storage/rows_mapper.h"

#include "common/config_primary_key_fwd.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "storage/data_dir.h"
#include "storage/lake/filenames.h"
#include "storage/storage_engine.h"
#include "testutil/assert.h"
#include "util/defer_op.h"

namespace starrocks {

class RowsMapperTest : public testing::Test {
public:
    RowsMapperTest() {}

protected:
    constexpr static const char* kTestDirectory = "./test_rows_mapper/";

    void SetUp() override { ASSERT_OK(fs::create_directories(kTestDirectory)); }

    void TearDown() override { (void)fs::remove_all(kTestDirectory); }

    DataDir* get_stores() {
        TCreateTabletReq request;
        return StorageEngine::instance()->get_stores_for_create_tablet(request.storage_medium)[0];
    }

    // generate id between [start, end)
    void generate_rssid_rowids(std::vector<uint64_t>* rssid_rowids, uint64_t start, size_t end, uint64_t rssid) {
        for (uint64_t i = start; i < end; i++) {
            rssid_rowids->push_back((rssid << 32) | i);
        }
    }
};

TEST_F(RowsMapperTest, test_write_read) {
    const std::string filename = std::string(kTestDirectory) + "test_write_read.crm";
    RowsMapperBuilder builder(filename);
    std::vector<uint64_t> rssid_rowids;
    ASSERT_OK(builder.append(rssid_rowids));
    ASSERT_FALSE(fs::path_exist(filename));
    generate_rssid_rowids(&rssid_rowids, 0, 1000, 11);
    ASSERT_OK(builder.append(rssid_rowids));
    rssid_rowids.clear();
    generate_rssid_rowids(&rssid_rowids, 1000, 3000, 11);
    ASSERT_OK(builder.append(rssid_rowids));
    ASSERT_OK(builder.finalize());

    // read from file
    RowsMapperIterator iterator;
    FileInfo file_info{.path = filename};
    ASSERT_OK(iterator.open(file_info));
    for (uint32_t i = 0; i < 3000; i += 100) {
        std::vector<uint64_t> rows_mapper;
        ASSERT_OK(iterator.next_values(100, &rows_mapper));
        ASSERT_TRUE(rows_mapper.size() == 100);
        for (uint32_t j = 0; j < rows_mapper.size(); j++) {
            ASSERT_TRUE((rows_mapper[j] >> 32) == 11);
            ASSERT_TRUE((rows_mapper[j] & 0xFFFFFFFF) == i + j);
        }
    }
    ASSERT_OK(iterator.status());
    // should eof
    std::vector<uint64_t> rows_mapper;
    ASSERT_TRUE(iterator.next_values(1, &rows_mapper).is_end_of_file());
}

TEST_F(RowsMapperTest, test_write_read_multi_segment) {
    const std::string filename = std::string(kTestDirectory) + "test_write_read_multi_segment.crm";
    RowsMapperBuilder builder(filename);
    std::vector<uint64_t> rssid_rowids;
    // rssid = 11
    generate_rssid_rowids(&rssid_rowids, 0, 1000, 11);
    ASSERT_OK(builder.append(rssid_rowids));
    rssid_rowids.clear();
    // rssid = 43
    generate_rssid_rowids(&rssid_rowids, 1000, 3000, 43);
    ASSERT_OK(builder.append(rssid_rowids));
    ASSERT_OK(builder.finalize());

    // read from file
    RowsMapperIterator iterator;
    FileInfo file_info{.path = filename};
    ASSERT_OK(iterator.open(file_info));
    for (uint32_t i = 0; i < 3000; i += 100) {
        std::vector<uint64_t> rows_mapper;
        ASSERT_OK(iterator.next_values(100, &rows_mapper));
        ASSERT_TRUE(rows_mapper.size() == 100);
        for (uint32_t j = 0; j < rows_mapper.size(); j++) {
            if (i + j < 1000) {
                ASSERT_TRUE((rows_mapper[j] >> 32) == 11);
                ASSERT_TRUE((rows_mapper[j] & 0xFFFFFFFF) == i + j);
            } else {
                ASSERT_TRUE((rows_mapper[j] >> 32) == 43);
                ASSERT_TRUE((rows_mapper[j] & 0xFFFFFFFF) == i + j);
            }
        }
    }
    ASSERT_OK(iterator.status());
    // should eof
    std::vector<uint64_t> rows_mapper;
    ASSERT_TRUE(iterator.next_values(1, &rows_mapper).is_end_of_file());
}

TEST_F(RowsMapperTest, test_file_info) {
    const std::string filename = std::string(kTestDirectory) + "test_file_info.crm";
    RowsMapperBuilder builder(filename);
    std::vector<uint64_t> rssid_rowids;
    generate_rssid_rowids(&rssid_rowids, 0, 1000, 11);
    ASSERT_OK(builder.append(rssid_rowids));
    ASSERT_OK(builder.finalize());

    // Get file info from builder
    FileInfo file_info = builder.file_info();
    ASSERT_FALSE(file_info.path.empty());
    ASSERT_TRUE(file_info.size.has_value());
    ASSERT_EQ(file_info.size.value(), 1000 * 8 + 12); // 1000 rows * 8 bytes + 8 bytes(row count) + 4 bytes(checksum)

    // Verify file name extraction
    ASSERT_EQ(file_info.path, "test_file_info.crm");
}

TEST_F(RowsMapperTest, test_open_with_size_in_fileinfo) {
    const std::string filename = std::string(kTestDirectory) + "test_open_with_size.crm";
    RowsMapperBuilder builder(filename);
    std::vector<uint64_t> rssid_rowids;
    generate_rssid_rowids(&rssid_rowids, 0, 500, 11);
    ASSERT_OK(builder.append(rssid_rowids));
    ASSERT_OK(builder.finalize());

    // Get file size
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(filename));
    ASSIGN_OR_ABORT(auto rfile, fs->new_random_access_file(filename));
    ASSIGN_OR_ABORT(int64_t file_size, rfile->get_size());
    rfile.reset();

    // Open with FileInfo that has size
    RowsMapperIterator iterator;
    FileInfo file_info{.path = filename, .size = file_size};
    ASSERT_OK(iterator.open(file_info));

    // Verify reading works correctly
    for (uint32_t i = 0; i < 500; i += 100) {
        std::vector<uint64_t> rows_mapper;
        ASSERT_OK(iterator.next_values(100, &rows_mapper));
        ASSERT_EQ(rows_mapper.size(), 100);
        for (uint32_t j = 0; j < rows_mapper.size(); j++) {
            ASSERT_EQ((rows_mapper[j] >> 32), 11);
            ASSERT_EQ((rows_mapper[j] & 0xFFFFFFFF), i + j);
        }
    }
    ASSERT_OK(iterator.status());
}

TEST_F(RowsMapperTest, test_open_without_size_in_fileinfo) {
    const std::string filename = std::string(kTestDirectory) + "test_open_without_size.crm";
    RowsMapperBuilder builder(filename);
    std::vector<uint64_t> rssid_rowids;
    generate_rssid_rowids(&rssid_rowids, 0, 500, 22);
    ASSERT_OK(builder.append(rssid_rowids));
    ASSERT_OK(builder.finalize());

    // Open with FileInfo without size (should query file size internally)
    RowsMapperIterator iterator;
    FileInfo file_info{.path = filename};
    ASSERT_OK(iterator.open(file_info));

    // Verify reading works correctly
    for (uint32_t i = 0; i < 500; i += 100) {
        std::vector<uint64_t> rows_mapper;
        ASSERT_OK(iterator.next_values(100, &rows_mapper));
        ASSERT_EQ(rows_mapper.size(), 100);
        for (uint32_t j = 0; j < rows_mapper.size(); j++) {
            ASSERT_EQ((rows_mapper[j] >> 32), 22);
            ASSERT_EQ((rows_mapper[j] & 0xFFFFFFFF), i + j);
        }
    }
    ASSERT_OK(iterator.status());
}

TEST_F(RowsMapperTest, test_lcrm_file_not_deleted_on_iterator_destruction) {
    // Test that lcrm files (lake compaction rows mapper) are NOT deleted when iterator is destroyed
    const std::string lcrm_filename = std::string(kTestDirectory) + "test_file.lcrm";

    // Create a lcrm file
    RowsMapperBuilder builder(lcrm_filename);
    std::vector<uint64_t> rssid_rowids;
    generate_rssid_rowids(&rssid_rowids, 0, 100, 11);
    ASSERT_OK(builder.append(rssid_rowids));
    ASSERT_OK(builder.finalize());

    // Verify file exists
    ASSERT_TRUE(fs::path_exist(lcrm_filename));

    {
        // Open and close iterator - lcrm file should NOT be deleted
        RowsMapperIterator iterator;
        FileInfo file_info{.path = lcrm_filename};
        ASSERT_OK(iterator.open(file_info));
        std::vector<uint64_t> rows_mapper;
        ASSERT_OK(iterator.next_values(100, &rows_mapper));
        ASSERT_OK(iterator.status());
        // Iterator destructor runs here
    }

    // File should still exist after iterator destruction
    ASSERT_TRUE(fs::path_exist(lcrm_filename));

    // Clean up
    ASSERT_OK(fs::remove(lcrm_filename));
}

TEST_F(RowsMapperTest, test_crm_file_deleted_on_iterator_destruction) {
    // Test that regular crm files (non-lcrm) ARE deleted when iterator is destroyed
    const std::string crm_filename = std::string(kTestDirectory) + "test_file.crm";

    // Create a regular crm file
    RowsMapperBuilder builder(crm_filename);
    std::vector<uint64_t> rssid_rowids;
    generate_rssid_rowids(&rssid_rowids, 0, 100, 11);
    ASSERT_OK(builder.append(rssid_rowids));
    ASSERT_OK(builder.finalize());

    // Verify file exists
    ASSERT_TRUE(fs::path_exist(crm_filename));

    {
        // Open and close iterator - regular crm file should be deleted
        RowsMapperIterator iterator;
        FileInfo file_info{.path = crm_filename};
        ASSERT_OK(iterator.open(file_info));
        std::vector<uint64_t> rows_mapper;
        ASSERT_OK(iterator.next_values(100, &rows_mapper));
        ASSERT_OK(iterator.status());
        // Iterator destructor runs here
    }

    // File should be deleted after iterator destruction
    ASSERT_FALSE(fs::path_exist(crm_filename));
}

// ---------------------------------------------------------------------------
// Pipelined per-segment / sub-chunk mode tests
// ---------------------------------------------------------------------------

namespace {

// Build a .crm file with the given per-segment row counts. Returns the FileInfo
// (with size) so callers can open it without an extra get_size() round-trip.
FileInfo build_rows_mapper_file(const std::string& path, const std::vector<size_t>& segment_row_counts,
                                uint64_t rssid_base) {
    RowsMapperBuilder builder(path);
    uint64_t row_id = 0;
    for (size_t seg = 0; seg < segment_row_counts.size(); ++seg) {
        std::vector<uint64_t> rssid_rowids;
        const uint64_t rssid = rssid_base + seg;
        for (size_t i = 0; i < segment_row_counts[seg]; ++i) {
            rssid_rowids.push_back((rssid << 32) | row_id);
            ++row_id;
        }
        CHECK_OK(builder.append(rssid_rowids));
    }
    CHECK_OK(builder.finalize());
    FileInfo info = builder.file_info();
    // file_info() returns a basename — switch to the full path the caller used so
    // the iterator can actually open it.
    info.path = path;
    return info;
}

void verify_segment(const std::vector<uint64_t>& values, size_t expected_count, uint64_t expected_rssid,
                    uint64_t expected_rowid_start) {
    ASSERT_EQ(values.size(), expected_count);
    for (size_t i = 0; i < values.size(); ++i) {
        ASSERT_EQ(values[i] >> 32, expected_rssid);
        ASSERT_EQ(values[i] & 0xFFFFFFFFULL, expected_rowid_start + i);
    }
}

} // namespace

TEST_F(RowsMapperTest, test_pipelined_single_segment) {
    const std::string filename = std::string(kTestDirectory) + "pipelined_single.crm";
    FileInfo info = build_rows_mapper_file(filename, {1000}, /*rssid_base=*/11);

    RowsMapperIterator iterator;
    ASSERT_OK(iterator.open(info));
    ASSERT_OK(iterator.prepare_segments({1000}));

    std::vector<uint64_t> values;
    ASSERT_OK(iterator.next_values(1000, &values));
    verify_segment(values, 1000, /*rssid=*/11, /*rowid_start=*/0);
    ASSERT_OK(iterator.status());
}

TEST_F(RowsMapperTest, test_pipelined_multi_segment) {
    const std::string filename = std::string(kTestDirectory) + "pipelined_multi.crm";
    const std::vector<size_t> seg_rows = {300, 700, 200, 800};
    FileInfo info = build_rows_mapper_file(filename, seg_rows, /*rssid_base=*/100);

    RowsMapperIterator iterator;
    ASSERT_OK(iterator.open(info));
    ASSERT_OK(iterator.prepare_segments(seg_rows));

    uint64_t expected_rowid_start = 0;
    for (size_t seg = 0; seg < seg_rows.size(); ++seg) {
        std::vector<uint64_t> values;
        ASSERT_OK(iterator.next_values(seg_rows[seg], &values));
        verify_segment(values, seg_rows[seg], /*rssid=*/100 + seg, expected_rowid_start);
        expected_rowid_start += seg_rows[seg];
    }
    ASSERT_OK(iterator.status());

    // Past-end consume should EOF.
    std::vector<uint64_t> trailing;
    ASSERT_TRUE(iterator.next_values(1, &trailing).is_end_of_file());
}

TEST_F(RowsMapperTest, test_pipelined_sub_chunking) {
    // Force sub-chunking by shrinking the sub-chunk byte budget so a single
    // segment is sliced into multiple sub-chunks. Each row is 8 bytes, so
    // sub_chunk_bytes=16 → 2 rows per sub-chunk → 500 sub-chunks for a
    // 1000-row segment.
    const int64_t saved_sub_chunk = config::lake_rows_mapper_sub_chunk_bytes;
    const int32_t saved_parallelism = config::lake_rows_mapper_read_parallelism;
    config::lake_rows_mapper_sub_chunk_bytes = 16;
    config::lake_rows_mapper_read_parallelism = 4;
    DeferOp restore([&]() {
        config::lake_rows_mapper_sub_chunk_bytes = saved_sub_chunk;
        config::lake_rows_mapper_read_parallelism = saved_parallelism;
    });

    const std::string filename = std::string(kTestDirectory) + "pipelined_sub_chunking.crm";
    // Two segments of unequal size, neither a multiple of the sub-chunk row
    // count, to exercise the trailing-partial-sub-chunk path.
    const std::vector<size_t> seg_rows = {1001, 503};
    FileInfo info = build_rows_mapper_file(filename, seg_rows, /*rssid_base=*/7);

    RowsMapperIterator iterator;
    ASSERT_OK(iterator.open(info));
    ASSERT_OK(iterator.prepare_segments(seg_rows));

    std::vector<uint64_t> values;
    ASSERT_OK(iterator.next_values(1001, &values));
    verify_segment(values, 1001, /*rssid=*/7, /*rowid_start=*/0);

    ASSERT_OK(iterator.next_values(503, &values));
    verify_segment(values, 503, /*rssid=*/8, /*rowid_start=*/1001);

    ASSERT_OK(iterator.status());
}

TEST_F(RowsMapperTest, test_pipelined_sum_mismatch_corruption) {
    const std::string filename = std::string(kTestDirectory) + "pipelined_sum_mismatch.crm";
    FileInfo info = build_rows_mapper_file(filename, {500}, /*rssid_base=*/11);

    RowsMapperIterator iterator;
    ASSERT_OK(iterator.open(info));
    // Declared sum (200+200=400) != file row count (500).
    auto st = iterator.prepare_segments({200, 200});
    ASSERT_TRUE(st.is_corruption()) << st;
}

TEST_F(RowsMapperTest, test_pipelined_after_next_values_fails) {
    const std::string filename = std::string(kTestDirectory) + "pipelined_after_next.crm";
    FileInfo info = build_rows_mapper_file(filename, {300}, /*rssid_base=*/11);

    RowsMapperIterator iterator;
    ASSERT_OK(iterator.open(info));

    // Consume one row in sequential mode before calling prepare_segments.
    std::vector<uint64_t> values;
    ASSERT_OK(iterator.next_values(1, &values));

    auto st = iterator.prepare_segments({300});
    ASSERT_TRUE(st.is_internal_error()) << st;
}

TEST_F(RowsMapperTest, test_pipelined_called_twice_fails) {
    const std::string filename = std::string(kTestDirectory) + "pipelined_twice.crm";
    FileInfo info = build_rows_mapper_file(filename, {300}, /*rssid_base=*/11);

    RowsMapperIterator iterator;
    ASSERT_OK(iterator.open(info));
    ASSERT_OK(iterator.prepare_segments({300}));

    auto st = iterator.prepare_segments({300});
    ASSERT_TRUE(st.is_internal_error()) << st;
}

TEST_F(RowsMapperTest, test_pipelined_wrong_fetch_size) {
    const std::string filename = std::string(kTestDirectory) + "pipelined_wrong_fetch.crm";
    FileInfo info = build_rows_mapper_file(filename, {100, 200}, /*rssid_base=*/5);

    RowsMapperIterator iterator;
    ASSERT_OK(iterator.open(info));
    ASSERT_OK(iterator.prepare_segments({100, 200}));

    // First segment is 100 rows; fetching 50 must fail.
    std::vector<uint64_t> values;
    auto st = iterator.next_values(50, &values);
    ASSERT_TRUE(st.is_internal_error()) << st;
}

TEST_F(RowsMapperTest, test_pipelined_empty_segment_in_middle) {
    // Output rowsets can contain zero-row segments between non-empty ones.
    // prepare_segments must accept this layout and next_values(0) must advance
    // the segment cursor so the next non-empty segment doesn't trip the
    // consume-mismatch guard.
    const std::string filename = std::string(kTestDirectory) + "pipelined_empty_middle.crm";
    const std::vector<size_t> seg_rows = {300, 0, 200};
    FileInfo info = build_rows_mapper_file(filename, seg_rows, /*rssid_base=*/20);

    RowsMapperIterator iterator;
    ASSERT_OK(iterator.open(info));
    ASSERT_OK(iterator.prepare_segments(seg_rows));

    std::vector<uint64_t> values;
    ASSERT_OK(iterator.next_values(300, &values));
    verify_segment(values, 300, /*rssid=*/20, /*rowid_start=*/0);

    // Empty segment — pure cursor advance.
    values.clear();
    ASSERT_OK(iterator.next_values(0, &values));
    ASSERT_TRUE(values.empty());

    ASSERT_OK(iterator.next_values(200, &values));
    verify_segment(values, 200, /*rssid=*/22, /*rowid_start=*/300);

    ASSERT_OK(iterator.status());
}

TEST_F(RowsMapperTest, test_pipelined_empty_segments_at_boundaries) {
    // Empty segments at the head and tail of the sequence.
    const std::string filename = std::string(kTestDirectory) + "pipelined_empty_boundaries.crm";
    const std::vector<size_t> seg_rows = {0, 0, 250, 0};
    FileInfo info = build_rows_mapper_file(filename, seg_rows, /*rssid_base=*/30);

    RowsMapperIterator iterator;
    ASSERT_OK(iterator.open(info));
    ASSERT_OK(iterator.prepare_segments(seg_rows));

    std::vector<uint64_t> values;
    ASSERT_OK(iterator.next_values(0, &values));
    ASSERT_OK(iterator.next_values(0, &values));
    ASSERT_OK(iterator.next_values(250, &values));
    verify_segment(values, 250, /*rssid=*/32, /*rowid_start=*/0);
    ASSERT_OK(iterator.next_values(0, &values));

    ASSERT_OK(iterator.status());
}

TEST_F(RowsMapperTest, test_pipelined_wrong_zero_fetch_on_nonempty_segment) {
    // next_values(0) on a segment that was declared with rows must fail —
    // otherwise we'd silently skip that segment's data.
    const std::string filename = std::string(kTestDirectory) + "pipelined_wrong_zero.crm";
    FileInfo info = build_rows_mapper_file(filename, {100}, /*rssid_base=*/40);

    RowsMapperIterator iterator;
    ASSERT_OK(iterator.open(info));
    ASSERT_OK(iterator.prepare_segments({100}));

    std::vector<uint64_t> values;
    auto st = iterator.next_values(0, &values);
    ASSERT_TRUE(st.is_internal_error()) << st;
}

TEST_F(RowsMapperTest, test_prepare_segments_parallelism_one_falls_back_to_sequential) {
    // lake_rows_mapper_read_parallelism=1 is documented as "disables pipelining".
    // After prepare_segments, the iterator should still answer next_values, but
    // through the sequential read_at_fully path (no per-sub-chunk RAFs, no
    // declared-size check on each call).
    const int32_t saved_parallelism = config::lake_rows_mapper_read_parallelism;
    config::lake_rows_mapper_read_parallelism = 1;
    DeferOp restore([&]() { config::lake_rows_mapper_read_parallelism = saved_parallelism; });

    const std::string filename = std::string(kTestDirectory) + "fallback_k1.crm";
    const std::vector<size_t> seg_rows = {300, 200};
    FileInfo info = build_rows_mapper_file(filename, seg_rows, /*rssid_base=*/50);

    RowsMapperIterator iterator;
    ASSERT_OK(iterator.open(info));
    ASSERT_OK(iterator.prepare_segments(seg_rows));

    // Sequential mode does not enforce declared sizes, so an arbitrary chunk
    // size must read fine — proves we did not stay in pipelined mode.
    std::vector<uint64_t> values;
    ASSERT_OK(iterator.next_values(100, &values));
    ASSERT_EQ(values.size(), 100u);
    for (uint32_t i = 0; i < 100; ++i) {
        ASSERT_EQ(values[i] >> 32, 50u);
        ASSERT_EQ(values[i] & 0xFFFFFFFFULL, i);
    }

    ASSERT_OK(iterator.next_values(400, &values));
    ASSERT_EQ(values.size(), 400u);
    ASSERT_OK(iterator.status());
}

TEST_F(RowsMapperTest, test_pipelined_destructor_drains_without_consume) {
    // Force tiny sub-chunks so there are several in-flight chunks pending when
    // the iterator goes out of scope without any next_values call. The
    // destructor must drain them all without crashing.
    const int64_t saved_sub_chunk = config::lake_rows_mapper_sub_chunk_bytes;
    const int32_t saved_parallelism = config::lake_rows_mapper_read_parallelism;
    config::lake_rows_mapper_sub_chunk_bytes = 16;
    config::lake_rows_mapper_read_parallelism = 8;
    DeferOp restore([&]() {
        config::lake_rows_mapper_sub_chunk_bytes = saved_sub_chunk;
        config::lake_rows_mapper_read_parallelism = saved_parallelism;
    });

    const std::string filename = std::string(kTestDirectory) + "pipelined_drain_dtor.crm";
    FileInfo info = build_rows_mapper_file(filename, {64}, /*rssid_base=*/11);

    {
        RowsMapperIterator iterator;
        ASSERT_OK(iterator.open(info));
        ASSERT_OK(iterator.prepare_segments({64}));
        // Leave with in-flight chunks; destructor must drain cleanly.
    }
}

TEST_F(RowsMapperTest, test_crm_file_gc) {
    DataDir* dir = get_stores();
    {
        // generate several crm files.
        ASSERT_OK(fs::new_writable_file(dir->get_tmp_path() + "/aaa.crm"));
        ASSERT_OK(fs::new_writable_file(dir->get_tmp_path() + "/bbb.crm"));
        ASSERT_OK(fs::new_writable_file(dir->get_tmp_path() + "/ccc.crm"));
        // collect files
        dir->perform_tmp_path_scan();
        dir->perform_tmp_path_scan();
        ASSERT_TRUE(dir->get_all_crm_files_cnt() == 3);
        // try to gc
        dir->perform_crm_gc(config::unused_crm_file_threshold_second);
        ASSERT_TRUE(dir->get_all_crm_files_cnt() == 0);
        // try to gc again
        dir->perform_tmp_path_scan();
        ASSERT_TRUE(dir->get_all_crm_files_cnt() == 3);
        dir->perform_crm_gc(0);
        ASSERT_TRUE(dir->get_all_crm_files_cnt() == 0);
        dir->perform_tmp_path_scan();
        // make sure file have been clean.
        ASSERT_TRUE(dir->get_all_crm_files_cnt() == 0);
    }
    {
        ASSERT_OK(fs::new_writable_file(dir->get_tmp_path() + "/aaa.crm"));
        // collect files
        dir->perform_tmp_path_scan();
        // delete this file
        ASSERT_OK(fs::remove(dir->get_tmp_path() + "/aaa.crm"));
        // try to gc
        dir->perform_crm_gc(config::unused_crm_file_threshold_second);
    }
    {
        ASSERT_OK(fs::remove(dir->get_tmp_path()));
        // collect files
        dir->perform_tmp_path_scan();
    }
}

} // namespace starrocks