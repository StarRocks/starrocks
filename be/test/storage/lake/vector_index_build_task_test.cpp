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

#include "storage/lake/vector_index_build_task.h"

#include <brpc/controller.h>
#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "column/array_column.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "fs/fs_util.h"
#include "gutil/casts.h"
#include "runtime/exec_env.h"
#include "service/service_be/lake_service.h"
#include "storage/chunk_helper.h"
#include "storage/lake/filenames.h"
#include "storage/lake/join_path.h"
#include "storage/lake/test_util.h"
#include "storage/rowset/segment_writer.h"

namespace starrocks::lake {

class VectorIndexBuildTaskTest : public TestBase {
public:
    VectorIndexBuildTaskTest() : TestBase("vector_index_build_task_test_" + std::to_string(GetCurrentTimeMicros())) {
        clear_and_init_test_dir();
    }

protected:
    static constexpr int64_t kTabletId = 10001;
    static constexpr int64_t kIndexId = 100;
    static constexpr int32_t kVectorColUniqueId = 2;
    uint32_t _next_rowset_id = 1;

    TabletSchemaPB create_schema_pb() {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(DUP_KEYS);
        schema_pb.set_num_short_key_columns(1);

        // key column: int32
        auto c0 = schema_pb.add_column();
        c0->set_unique_id(1);
        c0->set_name("pk");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);

        // value column: array<float> (vector column)
        auto c1 = schema_pb.add_column();
        c1->set_unique_id(kVectorColUniqueId);
        c1->set_name("vector");
        c1->set_type("ARRAY");
        c1->set_is_key(false);
        // Vector index requires the ARRAY column itself to be non-nullable;
        // ArrayColumnWriter DCHECKs this on the sync write path and the
        // tenann builder down_casts to ArrayColumn (not NullableColumn).
        c1->set_is_nullable(false);

        auto* child = c1->add_children_columns();
        child->set_unique_id(3);
        child->set_name("element");
        child->set_type("FLOAT");
        // Element must be nullable: tenann reads per-element null bits to
        // skip null vectors during build.
        child->set_is_nullable(true);

        // Add vector index
        auto* idx = schema_pb.add_table_indices();
        idx->set_index_id(kIndexId);
        idx->set_index_name("vec_idx");
        idx->set_index_type(IndexType::VECTOR);
        idx->add_col_unique_id(kVectorColUniqueId);
        // Properties are stored as a nested JSON string in index_properties
        std::string props_json = R"({
            "common_properties": {
                "index_type": "hnsw",
                "dim": "3",
                "is_vector_normed": "false",
                "metric_type": "l2_distance",
                "index_build_mode": "async"
            },
            "index_properties": {
                "efconstruction": "40",
                "m": "16"
            }
        })";
        idx->set_index_properties(props_json);

        return schema_pb;
    }

    // Write a segment file with pk + array<float> columns, return segment filename
    StatusOr<std::string> write_segment(const TabletSchemaCSPtr& tablet_schema, int64_t txn_id, int num_rows) {
        auto seg_name = gen_segment_filename(txn_id);
        auto seg_path = _tablet_mgr->segment_location(kTabletId, seg_name);

        SegmentWriterOptions opts;
        opts.is_compaction = false;
        // defer_vector_index_build = true: simulate async mode (no .vi generation)
        opts.defer_vector_index_build = true;

        // Fill vector_index_file_paths so metadata is populated
        std::string vi_name = gen_vector_index_filename(seg_name, kIndexId);
        std::string vi_path = _tablet_mgr->segment_location(kTabletId, vi_name);
        opts.vector_index_file_paths[kIndexId] = vi_path;

        ASSIGN_OR_RETURN(auto wfile, fs::new_writable_file(seg_path));
        auto writer = std::make_unique<SegmentWriter>(std::move(wfile), 0, tablet_schema, opts);
        RETURN_IF_ERROR(writer->init());

        // Build chunk with schema attached (required by SegmentWriter)
        auto schema = ChunkHelper::convert_schema(tablet_schema);
        auto chunk = ChunkHelper::new_chunk(schema, num_rows);

        for (int i = 0; i < num_rows; ++i) {
            // pk column
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Datum(static_cast<int32_t>(i)));
            // array<float> column: build a DatumArray with 3 floats
            DatumArray arr;
            arr.emplace_back(static_cast<float>(i) + 0.1f);
            arr.emplace_back(static_cast<float>(i) + 0.2f);
            arr.emplace_back(static_cast<float>(i) + 0.3f);
            chunk->get_column_raw_ptr_by_index(1)->append_datum(Datum(arr));
        }

        RETURN_IF_ERROR(writer->append_chunk(*chunk));

        uint64_t seg_size = 0, idx_size = 0, footer_pos = 0;
        RETURN_IF_ERROR(writer->finalize(&seg_size, &idx_size, &footer_pos));
        return seg_name;
    }

    // Create tablet metadata with rowsets referencing written segments
    void create_metadata(const TabletSchemaPB& schema_pb, int64_t version,
                         const std::vector<std::pair<int64_t, std::string>>& rowset_infos, // {version, seg_name}
                         int64_t built_version = 0) {
        auto metadata = std::make_shared<TabletMetadataPB>();
        metadata->set_id(kTabletId);
        metadata->set_version(version);
        *metadata->mutable_schema() = schema_pb;

        for (const auto& [rv, seg_name] : rowset_infos) {
            auto* rowset = metadata->add_rowsets();
            rowset->set_id(_next_rowset_id++);
            rowset->add_segments(seg_name);
            rowset->set_num_rows(10);
            rowset->set_data_size(1024);
            rowset->set_overlapped(false);
            rowset->set_version(rv);

            // Populate vector_index_ids in segment_metas
            auto* seg_meta = rowset->add_segment_metas();
            seg_meta->add_vector_index_ids(kIndexId);
        }

        if (built_version > 0) {
            metadata->set_vector_index_built_version(built_version);
        }

        CHECK_OK(_tablet_mgr->put_tablet_metadata(metadata));
    }
};

// Test basic build: one rowset with one segment, no built_version set
TEST_F(VectorIndexBuildTaskTest, test_basic_build) {
    auto schema_pb = create_schema_pb();
    auto tablet_schema = TabletSchema::create(schema_pb);

    ASSIGN_OR_ABORT(auto seg_name, write_segment(tablet_schema, 1001, 10));

    // Create metadata at version 2 with one rowset at version 2
    create_metadata(schema_pb, 2, {{2, seg_name}});

    // Verify .vi does NOT exist before build
    std::string vi_name = gen_vector_index_filename(seg_name, kIndexId);
    std::string vi_path = _tablet_mgr->segment_location(kTabletId, vi_name);
    ASSERT_FALSE(fs::path_exist(vi_path));

    // Execute build
    BuildVectorIndexRequest request;
    request.set_tablet_id(kTabletId);
    request.set_version(2);
    BuildVectorIndexResponse response;

    VectorIndexBuildTask task(_tablet_mgr.get());
    ASSERT_OK(task.execute(request, &response));
    ASSERT_EQ(response.new_built_version(), 2);

    // Verify .vi file now exists
    ASSERT_TRUE(fs::path_exist(vi_path));
}

// Test that already-built rowsets are skipped based on built_version watermark
TEST_F(VectorIndexBuildTaskTest, test_skip_built_rowsets) {
    auto schema_pb = create_schema_pb();
    auto tablet_schema = TabletSchema::create(schema_pb);

    ASSIGN_OR_ABORT(auto seg1, write_segment(tablet_schema, 1001, 10));
    ASSIGN_OR_ABORT(auto seg2, write_segment(tablet_schema, 1002, 10));

    // built_version=2 means rowset at version 2 is already built
    create_metadata(schema_pb, 3, {{2, seg1}, {3, seg2}}, /*built_version=*/2);

    BuildVectorIndexRequest request;
    request.set_tablet_id(kTabletId);
    request.set_version(3);
    BuildVectorIndexResponse response;

    VectorIndexBuildTask task(_tablet_mgr.get());
    ASSERT_OK(task.execute(request, &response));
    ASSERT_EQ(response.new_built_version(), 3);

    // Only seg2's .vi should be built, seg1 was skipped
    std::string vi1 = _tablet_mgr->segment_location(kTabletId, gen_vector_index_filename(seg1, kIndexId));
    std::string vi2 = _tablet_mgr->segment_location(kTabletId, gen_vector_index_filename(seg2, kIndexId));
    ASSERT_FALSE(fs::path_exist(vi1));
    ASSERT_TRUE(fs::path_exist(vi2));
}

// Test batch limiting: only max_rowsets_per_batch rowsets are processed
TEST_F(VectorIndexBuildTaskTest, test_batch_limit) {
    auto schema_pb = create_schema_pb();
    auto tablet_schema = TabletSchema::create(schema_pb);

    ASSIGN_OR_ABORT(auto seg1, write_segment(tablet_schema, 1001, 10));
    ASSIGN_OR_ABORT(auto seg2, write_segment(tablet_schema, 1002, 10));
    ASSIGN_OR_ABORT(auto seg3, write_segment(tablet_schema, 1003, 10));

    create_metadata(schema_pb, 4, {{2, seg1}, {3, seg2}, {4, seg3}});

    BuildVectorIndexRequest request;
    request.set_tablet_id(kTabletId);
    request.set_version(4);
    request.set_max_rowsets_per_batch(2); // Only process 2 of 3 rowsets
    BuildVectorIndexResponse response;

    VectorIndexBuildTask task(_tablet_mgr.get());
    ASSERT_OK(task.execute(request, &response));

    // Should have processed rowsets at version 2 and 3 (sorted by version, first 2)
    ASSERT_EQ(response.new_built_version(), 3);

    std::string vi1 = _tablet_mgr->segment_location(kTabletId, gen_vector_index_filename(seg1, kIndexId));
    std::string vi2 = _tablet_mgr->segment_location(kTabletId, gen_vector_index_filename(seg2, kIndexId));
    std::string vi3 = _tablet_mgr->segment_location(kTabletId, gen_vector_index_filename(seg3, kIndexId));
    ASSERT_TRUE(fs::path_exist(vi1));
    ASSERT_TRUE(fs::path_exist(vi2));
    ASSERT_FALSE(fs::path_exist(vi3)); // Not processed yet
}

// Test that FE-provided built_version in request skips already-built rowsets
TEST_F(VectorIndexBuildTaskTest, test_request_built_version) {
    auto schema_pb = create_schema_pb();
    auto tablet_schema = TabletSchema::create(schema_pb);

    ASSIGN_OR_ABORT(auto seg1, write_segment(tablet_schema, 1001, 10));
    ASSIGN_OR_ABORT(auto seg2, write_segment(tablet_schema, 1002, 10));

    // Metadata has built_version=0, but FE knows built_version=2 from previous build
    create_metadata(schema_pb, 3, {{2, seg1}, {3, seg2}}, /*built_version=*/0);

    BuildVectorIndexRequest request;
    request.set_tablet_id(kTabletId);
    request.set_version(3);
    request.set_built_version(2); // FE passes its in-memory built_version
    BuildVectorIndexResponse response;

    VectorIndexBuildTask task(_tablet_mgr.get());
    ASSERT_OK(task.execute(request, &response));
    ASSERT_EQ(response.new_built_version(), 3);

    // Only seg2 should be built; seg1 skipped via request built_version
    std::string vi1 = _tablet_mgr->segment_location(kTabletId, gen_vector_index_filename(seg1, kIndexId));
    std::string vi2 = _tablet_mgr->segment_location(kTabletId, gen_vector_index_filename(seg2, kIndexId));
    ASSERT_FALSE(fs::path_exist(vi1));
    ASSERT_TRUE(fs::path_exist(vi2));
}

// Test that existing .vi files are skipped on partial retry
TEST_F(VectorIndexBuildTaskTest, test_skip_existing_vi_files) {
    auto schema_pb = create_schema_pb();
    auto tablet_schema = TabletSchema::create(schema_pb);

    ASSIGN_OR_ABORT(auto seg_name, write_segment(tablet_schema, 1001, 10));

    create_metadata(schema_pb, 2, {{2, seg_name}});

    // Pre-create .vi file to simulate a previous partial build success
    std::string vi_name = gen_vector_index_filename(seg_name, kIndexId);
    std::string vi_path = _tablet_mgr->segment_location(kTabletId, vi_name);
    ASSIGN_OR_ABORT(auto wf, fs::new_writable_file(vi_path));
    ASSERT_OK(wf->append("dummy"));
    ASSERT_OK(wf->close());

    BuildVectorIndexRequest request;
    request.set_tablet_id(kTabletId);
    request.set_version(2);
    BuildVectorIndexResponse response;

    VectorIndexBuildTask task(_tablet_mgr.get());
    ASSERT_OK(task.execute(request, &response));
    ASSERT_EQ(response.new_built_version(), 2);

    // File should still contain "dummy" (skipped, not overwritten)
    ASSIGN_OR_ABORT(auto rf, fs::new_random_access_file(vi_path));
    ASSIGN_OR_ABORT(auto content, rf->read_all());
    ASSERT_EQ(content, "dummy");
}

// Test with no rowsets needing build (all below watermark)
TEST_F(VectorIndexBuildTaskTest, test_nothing_to_build) {
    auto schema_pb = create_schema_pb();
    auto tablet_schema = TabletSchema::create(schema_pb);

    ASSIGN_OR_ABORT(auto seg_name, write_segment(tablet_schema, 1001, 10));

    // built_version=5, rowset version=2 -> nothing to build
    create_metadata(schema_pb, 5, {{2, seg_name}}, /*built_version=*/5);

    BuildVectorIndexRequest request;
    request.set_tablet_id(kTabletId);
    request.set_version(5);
    BuildVectorIndexResponse response;

    VectorIndexBuildTask task(_tablet_mgr.get());
    ASSERT_OK(task.execute(request, &response));
    ASSERT_EQ(response.new_built_version(), 5);
}

// Test with rowsets that have no vector_index_ids in segment_metas (should be skipped)
TEST_F(VectorIndexBuildTaskTest, test_skip_rowsets_without_vector_indexes) {
    auto schema_pb = create_schema_pb();
    auto tablet_schema = TabletSchema::create(schema_pb);

    ASSIGN_OR_ABORT(auto seg_name, write_segment(tablet_schema, 1001, 10));

    // Create metadata with a rowset that has NO vector_index_ids in segment_metas
    auto metadata = std::make_shared<TabletMetadataPB>();
    metadata->set_id(kTabletId);
    metadata->set_version(2);
    *metadata->mutable_schema() = schema_pb;

    auto* rowset = metadata->add_rowsets();
    rowset->set_id(_next_rowset_id++);
    rowset->add_segments(seg_name);
    rowset->set_num_rows(10);
    rowset->set_data_size(1024);
    rowset->set_version(2);
    // Note: NOT setting vector_index_ids in segment_metas

    CHECK_OK(_tablet_mgr->put_tablet_metadata(metadata));

    BuildVectorIndexRequest request;
    request.set_tablet_id(kTabletId);
    request.set_version(2);
    BuildVectorIndexResponse response;

    VectorIndexBuildTask task(_tablet_mgr.get());
    ASSERT_OK(task.execute(request, &response));
    // No vi rowsets to build, watermark advances through the no-vi rowset
    ASSERT_EQ(response.new_built_version(), 2);
}

// Reusing the same task instance across two prepare()/execute() calls must reset
// _work_items / _rowset_versions / _built_version / _target_version, otherwise the
// second run would see stale state from the first.
TEST_F(VectorIndexBuildTaskTest, test_prepare_reuse_resets_state) {
    auto schema_pb = create_schema_pb();
    auto tablet_schema = TabletSchema::create(schema_pb);

    // First metadata: target version 2, one rowset at v=2.
    ASSIGN_OR_ABORT(auto seg1, write_segment(tablet_schema, 1001, 10));
    create_metadata(schema_pb, 2, {{2, seg1}});

    BuildVectorIndexRequest req1;
    req1.set_tablet_id(kTabletId);
    req1.set_version(2);
    BuildVectorIndexResponse resp1;

    VectorIndexBuildTask task(_tablet_mgr.get());
    ASSERT_OK(task.execute(req1, &resp1));
    ASSERT_EQ(2, resp1.new_built_version());
    EXPECT_EQ(1u, task.work_count());

    // Second metadata: target version 3, two rowsets at v=2 and v=3.
    // built_version supplied via request = 2, so only v=3 should produce work items.
    ASSIGN_OR_ABORT(auto seg2, write_segment(tablet_schema, 1002, 10));
    create_metadata(schema_pb, 3, {{2, seg1}, {3, seg2}}, /*built_version=*/2);

    BuildVectorIndexRequest req2;
    req2.set_tablet_id(kTabletId);
    req2.set_version(3);
    BuildVectorIndexResponse resp2;

    ASSERT_OK(task.execute(req2, &resp2));
    ASSERT_EQ(3, resp2.new_built_version());
    // Only 1 fresh work item from v=3; if state hadn't been reset, work_count would
    // accumulate the v=2 entry from the first run too.
    EXPECT_EQ(1u, task.work_count());
}

// Drives the end-to-end LakeServiceImpl::build_vector_index RPC: schema with vector
// index → segment written in defer mode → metadata records vector_index_ids → call the
// RPC and verify the .vi file lands on disk with the watermark advanced. This exercises
// the parallel `latch.wait()` / `thread_pool->submit` path in lake_service.cpp that the
// task-level tests cannot reach.
TEST_F(VectorIndexBuildTaskTest, test_lake_service_build_vector_index_full_path) {
    auto schema_pb = create_schema_pb();
    auto tablet_schema = TabletSchema::create(schema_pb);
    ASSIGN_OR_ABORT(auto seg_name, write_segment(tablet_schema, 1001, 10));
    create_metadata(schema_pb, 2, {{2, seg_name}});

    LakeServiceImpl service(ExecEnv::GetInstance(), _tablet_mgr.get());

    BuildVectorIndexRequest request;
    request.set_tablet_id(kTabletId);
    request.set_version(2);
    BuildVectorIndexResponse response;
    brpc::Controller cntl;
    service.build_vector_index(&cntl, &request, &response, nullptr);

    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_TRUE(response.has_status());
    EXPECT_EQ(0, response.status().status_code());
    ASSERT_TRUE(response.has_new_built_version());
    EXPECT_EQ(2, response.new_built_version());

    std::string vi_path = _tablet_mgr->segment_location(kTabletId, gen_vector_index_filename(seg_name, kIndexId));
    EXPECT_TRUE(fs::path_exist(vi_path)) << "deferred .vi should have been built by the RPC";
}

// Same RPC end-to-end path but with one good rowset (v=2) and one whose segment is
// broken (v=3). Watermark must advance to v=2 and pause at v=3, exercising the
// parallel worker error reporting + compute_built_version partial-progress path.
TEST_F(VectorIndexBuildTaskTest, test_lake_service_build_vector_index_partial_failure) {
    auto schema_pb = create_schema_pb();
    auto tablet_schema = TabletSchema::create(schema_pb);

    ASSIGN_OR_ABORT(auto seg_ok, write_segment(tablet_schema, 1001, 10));
    auto seg_bad = gen_segment_filename(2002); // never actually written

    auto metadata = std::make_shared<TabletMetadataPB>();
    metadata->set_id(kTabletId);
    metadata->set_version(3);
    *metadata->mutable_schema() = schema_pb;
    {
        auto* rs = metadata->add_rowsets();
        rs->set_id(_next_rowset_id++);
        rs->add_segments(seg_ok);
        rs->set_num_rows(10);
        rs->set_data_size(1024);
        rs->set_version(2);
        rs->add_segment_metas()->add_vector_index_ids(kIndexId);
    }
    {
        auto* rs = metadata->add_rowsets();
        rs->set_id(_next_rowset_id++);
        rs->add_segments(seg_bad);
        rs->set_num_rows(10);
        rs->set_data_size(1024);
        rs->set_version(3);
        rs->add_segment_metas()->add_vector_index_ids(kIndexId);
    }
    CHECK_OK(_tablet_mgr->put_tablet_metadata(metadata));

    LakeServiceImpl service(ExecEnv::GetInstance(), _tablet_mgr.get());
    BuildVectorIndexRequest request;
    request.set_tablet_id(kTabletId);
    request.set_version(3);
    BuildVectorIndexResponse response;
    brpc::Controller cntl;
    service.build_vector_index(&cntl, &request, &response, nullptr);

    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    EXPECT_EQ(0, response.status().status_code());
    ASSERT_TRUE(response.has_new_built_version());
    EXPECT_EQ(2, response.new_built_version());

    EXPECT_TRUE(fs::path_exist(_tablet_mgr->segment_location(kTabletId, gen_vector_index_filename(seg_ok, kIndexId))));
    EXPECT_FALSE(
            fs::path_exist(_tablet_mgr->segment_location(kTabletId, gen_vector_index_filename(seg_bad, kIndexId))));
}

// Build with one healthy rowset (v=2) and one whose segment file is broken (v=3).
// Watermark must advance through v=2 (fully built) and stop at v=3 (failed segment),
// exercising compute_built_version's failed_versions branch.
TEST_F(VectorIndexBuildTaskTest, test_partial_failure_watermark_stops_at_failed_version) {
    auto schema_pb = create_schema_pb();
    auto tablet_schema = TabletSchema::create(schema_pb);

    ASSIGN_OR_ABORT(auto seg_ok, write_segment(tablet_schema, 1001, 10));

    // Manufacture a non-existent segment name for v=3 so build_segment fails to open
    // it. We still populate vector_index_ids for that segment so prepare() schedules
    // a build attempt and compute_built_version can see the failed version.
    auto seg_bad = gen_segment_filename(2002);

    auto metadata = std::make_shared<TabletMetadataPB>();
    metadata->set_id(kTabletId);
    metadata->set_version(3);
    *metadata->mutable_schema() = schema_pb;
    {
        auto* rs = metadata->add_rowsets();
        rs->set_id(_next_rowset_id++);
        rs->add_segments(seg_ok);
        rs->set_num_rows(10);
        rs->set_data_size(1024);
        rs->set_version(2);
        rs->add_segment_metas()->add_vector_index_ids(kIndexId);
    }
    {
        auto* rs = metadata->add_rowsets();
        rs->set_id(_next_rowset_id++);
        rs->add_segments(seg_bad);
        rs->set_num_rows(10);
        rs->set_data_size(1024);
        rs->set_version(3);
        rs->add_segment_metas()->add_vector_index_ids(kIndexId);
    }
    CHECK_OK(_tablet_mgr->put_tablet_metadata(metadata));

    BuildVectorIndexRequest request;
    request.set_tablet_id(kTabletId);
    request.set_version(3);
    BuildVectorIndexResponse response;
    VectorIndexBuildTask task(_tablet_mgr.get());
    // execute() returns OK from the orchestration layer regardless of per-segment
    // failure; progress is conveyed only via new_built_version.
    ASSERT_OK(task.execute(request, &response));
    ASSERT_TRUE(response.has_new_built_version());
    EXPECT_EQ(2, response.new_built_version()) << "watermark should advance through v=2 and pause at v=3";

    // v=2's .vi was actually built; v=3's was not.
    EXPECT_TRUE(fs::path_exist(_tablet_mgr->segment_location(kTabletId, gen_vector_index_filename(seg_ok, kIndexId))));
    EXPECT_FALSE(
            fs::path_exist(_tablet_mgr->segment_location(kTabletId, gen_vector_index_filename(seg_bad, kIndexId))));
}

// Persist segment_size and segment_encryption_metas on the rowset and verify they
// flow through prepare() into the FileInfo handed to build_segment. This exercises
// the optional fields in vector_index_build_task.cpp::prepare() that were previously
// uncovered.
TEST_F(VectorIndexBuildTaskTest, test_prepare_carries_segment_size_and_encryption) {
    auto schema_pb = create_schema_pb();
    auto tablet_schema = TabletSchema::create(schema_pb);
    ASSIGN_OR_ABORT(auto seg_name, write_segment(tablet_schema, 1001, 10));

    auto metadata = std::make_shared<TabletMetadataPB>();
    metadata->set_id(kTabletId);
    metadata->set_version(2);
    *metadata->mutable_schema() = schema_pb;
    auto* rowset = metadata->add_rowsets();
    rowset->set_id(_next_rowset_id++);
    rowset->add_segments(seg_name);
    rowset->add_segment_size(2048);           // exercises has_segment_size branch
    rowset->add_segment_encryption_metas(""); // exercises has_encryption_meta branch (empty meta is fine)
    rowset->set_num_rows(10);
    rowset->set_version(2);
    auto* seg_meta = rowset->add_segment_metas();
    seg_meta->add_vector_index_ids(kIndexId);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(metadata));

    BuildVectorIndexRequest request;
    request.set_tablet_id(kTabletId);
    request.set_version(2);
    BuildVectorIndexResponse response;
    VectorIndexBuildTask task(_tablet_mgr.get());
    ASSERT_OK(task.execute(request, &response));
    ASSERT_EQ(response.new_built_version(), 2);

    std::string vi_path = _tablet_mgr->segment_location(kTabletId, gen_vector_index_filename(seg_name, kIndexId));
    EXPECT_TRUE(fs::path_exist(vi_path));
}

} // namespace starrocks::lake
