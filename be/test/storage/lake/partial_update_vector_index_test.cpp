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

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "base/testutil/sync_point.h"
#include "column/array_column.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "common/config_vector_index_fwd.h"
#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/filenames.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/test_util.h"
#include "storage/rowset/segment.h"
#include "storage/tablet_schema.h"

namespace starrocks::lake {

// Publish-level tests for vector index preservation across the shared-data partial-update
// segment rewrite (RowsetUpdateState::rewrite_segment): the rewritten (dest) segment must end
// up with the right SegmentMetadataPB::vector_index_ids, footer vector_index_storage_type and,
// for sync indexes, a .vi artifact at the segment-name-keyed location-provider path. Covers
// both rewrite branches (SegmentRewriter::rewrite_partial_update and rewrite_auto_increment_lake)
// in sync and async index_build_mode, above and below the deferred-build threshold.
class LakePartialUpdateVectorIndexTest : public TestBase {
public:
    LakePartialUpdateVectorIndexTest() : TestBase(kTestDirectory) {}

    void SetUp() override { clear_and_init_test_dir(); }

    void TearDown() override {
        EXPECT_TRUE(_update_mgr->TEST_check_primary_index_cache_ref(_tablet_metadata->id(), 1));
        remove_test_dir_or_die();
    }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_partial_update_vector_index";
    constexpr static int kChunkSize = 12;
    constexpr static int64_t kIndexId = 77;
    constexpr static int32_t kKeyUid = 1;
    constexpr static int32_t kValueUid = 2;
    constexpr static int32_t kVecUid = 3;
    constexpr static int32_t kExtraUid = 5;

    // PK schema: c0 INT key | c1 BIGINT (optionally auto-increment) | c2 ARRAY<FLOAT> with a
    // vector index | optionally c3 BIGINT (so a partial update can cover c0..c2 and a later
    // schema drop of c3 leaves no unmodified columns). index_build_mode: sync by default, async
    // with the given threshold when |async_threshold| > 0.
    void create_tablet(bool auto_increment, uint32_t async_threshold, bool with_extra_column = false) {
        _tablet_metadata = std::make_shared<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);
        _tablet_metadata->set_next_rowset_id(1);

        auto schema = _tablet_metadata->mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(PRIMARY_KEYS);
        schema->set_num_rows_per_row_block(65535);
        auto c0 = schema->add_column();
        c0->set_unique_id(kKeyUid);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);

        auto c1 = schema->add_column();
        c1->set_unique_id(kValueUid);
        c1->set_name("c1");
        c1->set_type("BIGINT");
        c1->set_is_key(false);
        c1->set_is_nullable(false);
        c1->set_is_auto_increment(auto_increment);
        c1->set_aggregation("REPLACE");

        // Vector index requires a non-nullable outer array column with a nullable element
        // (DCHECK in ArrayColumnWriter::append / VectorIndexWriter).
        auto c2 = schema->add_column();
        c2->set_unique_id(kVecUid);
        c2->set_name("c2");
        c2->set_type("ARRAY");
        c2->set_is_key(false);
        c2->set_is_nullable(false);
        c2->set_aggregation("REPLACE");
        auto* child = c2->add_children_columns();
        child->set_unique_id(4);
        child->set_name("element");
        child->set_type("FLOAT");
        child->set_is_nullable(true);

        if (with_extra_column) {
            auto c3 = schema->add_column();
            c3->set_unique_id(kExtraUid);
            c3->set_name("c3");
            c3->set_type("BIGINT");
            c3->set_is_key(false);
            c3->set_is_nullable(false);
            c3->set_aggregation("REPLACE");
        }

        auto* idx = schema->add_table_indices();
        idx->set_index_id(kIndexId);
        idx->set_index_name("vec_idx");
        idx->set_index_type(IndexType::VECTOR);
        idx->add_col_unique_id(kVecUid);
        std::string mode_props;
        if (async_threshold > 0) {
            mode_props = R"(,
                "index_build_mode": "async",
                "index_build_threshold": ")" +
                         std::to_string(async_threshold) + R"(")";
        }
        std::string props_json = R"({
            "common_properties": {
                "index_type": "hnsw",
                "dim": "3",
                "is_vector_normed": "false",
                "metric_type": "l2_distance")" +
                                 mode_props + R"(
            },
            "index_properties": {
                "efconstruction": "40",
                "m": "16"
            }
        })";
        idx->set_index_properties(props_json);

        _tablet_schema = TabletSchema::create(*schema);
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
        CHECK_OK(_tablet_mgr->create_schema_file(_tablet_metadata->id(), _tablet_metadata->schema()));
    }

    static ColumnPtr build_vector_column(int num_rows, float bias) {
        auto element = FixedLengthColumn<float>::create();
        auto nulls = NullColumn::create();
        auto offsets = UInt32Column::create();
        offsets->append(0);
        for (int i = 0; i < num_rows; ++i) {
            for (int d = 0; d < 3; ++d) {
                element->append(static_cast<float>(i) + bias + static_cast<float>(d) / 10.0f);
                nulls->append(0);
            }
            offsets->append((i + 1) * 3);
        }
        auto nullable = NullableColumn::create(std::move(element), std::move(nulls));
        return ArrayColumn::create(std::move(nullable), std::move(offsets));
    }

    Chunk full_chunk() {
        std::vector<int> v0(kChunkSize);
        std::vector<int64_t> v1(kChunkSize);
        std::iota(v0.begin(), v0.end(), 0);
        std::iota(v1.begin(), v1.end(), 1);
        auto c0 = Int32Column::create();
        auto c1 = Int64Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int64_t));
        Columns columns{std::move(c0), std::move(c1), build_vector_column(kChunkSize, /*bias=*/0.1f)};
        Chunk::SlotHashMap slot_map{{0, 0}, {1, 1}, {2, 2}};
        if (_tablet_schema->num_columns() == 4) {
            auto c3 = Int64Column::create();
            std::vector<int64_t> v3(kChunkSize, 7);
            c3->append_numbers(v3.data(), v3.size() * sizeof(int64_t));
            columns.emplace_back(std::move(c3));
            slot_map[3] = 3;
        }
        return Chunk(std::move(columns), slot_map);
    }

    int64_t write_full(int64_t version) {
        auto chunk = full_chunk();
        auto indexes = std::vector<uint32_t>(kChunkSize);
        std::iota(indexes.begin(), indexes.end(), 0);
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(_tablet_metadata->id())
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(kPartitionId)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        CHECK_OK(delta_writer->open());
        CHECK_OK(delta_writer->write(chunk, indexes.data(), indexes.size()));
        CHECK_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        CHECK_OK(publish_single_version(_tablet_metadata->id(), version + 1, txn_id).status());
        return version + 1;
    }

    // Row-mode partial update write only (no publish); returns the txn id so a test can mutate
    // state (e.g. drop a column from the tablet metadata) before publishing.
    int64_t partial_update_txn(std::vector<SlotDescriptor*>* slots, Chunk& chunk, bool miss_auto_increment) {
        auto indexes = std::vector<uint32_t>(kChunkSize);
        std::iota(indexes.begin(), indexes.end(), 0);
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(_tablet_metadata->id())
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(kPartitionId)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(slots)
                                                   .set_partial_update_mode(PartialUpdateMode::ROW_MODE)
                                                   .set_miss_auto_increment_column(miss_auto_increment)
                                                   .set_table_id(next_id())
                                                   .build());
        CHECK_OK(delta_writer->open());
        CHECK_OK(delta_writer->write(chunk, indexes.data(), indexes.size()));
        CHECK_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        return txn_id;
    }

    // Row-mode partial update through the given slots; triggers rewrite_segment at publish.
    int64_t partial_update(int64_t version, std::vector<SlotDescriptor*>* slots, Chunk& chunk,
                           bool miss_auto_increment) {
        auto txn_id = partial_update_txn(slots, chunk, miss_auto_increment);
        CHECK_OK(publish_single_version(_tablet_metadata->id(), version + 1, txn_id).status());
        return version + 1;
    }

    SegmentMetadataPB rewritten_segment_meta(int64_t version) {
        auto metadata = _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version).value();
        CHECK_GT(metadata->rowsets_size(), 0);
        const auto& rowset = metadata->rowsets(metadata->rowsets_size() - 1);
        CHECK_GT(rowset.segment_metas_size(), 0);
        return rowset.segment_metas(0);
    }

    std::string vi_location(const std::string& segment_name) {
        return _tablet_mgr->segment_location(_tablet_metadata->id(),
                                             gen_vector_index_filename(segment_name, _tablet_metadata->id(), kIndexId));
    }

    SegmentFooterPB read_footer(const std::string& segment_name) {
        auto path = _tablet_mgr->segment_location(_tablet_metadata->id(), segment_name);
        ASSIGN_OR_ABORT(auto rf, fs::new_random_access_file(path));
        SegmentFooterPB footer;
        CHECK_OK(Segment::parse_segment_footer(rf.get(), &footer, nullptr, nullptr).status());
        return footer;
    }

    // Read back the whole table and verify the vector column carries the expected per-row bias.
    void check_vector_data(int64_t version, float bias) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        ASSERT_OK(reader->prepare());
        ASSERT_OK(reader->open(TabletReaderParams()));
        auto chunk = ChunkFactory::new_chunk(*_schema, 128);
        int64_t rows = 0;
        while (true) {
            auto st = reader->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            ASSERT_OK(st);
            for (int i = 0; i < chunk->num_rows(); ++i) {
                int key = chunk->columns()[0]->get(i).get_int32();
                auto arr = chunk->columns()[2]->get(i).get_array();
                ASSERT_EQ(arr.size(), 3);
                ASSERT_FLOAT_EQ(arr[0].get_float(), static_cast<float>(key) + bias);
            }
            rows += chunk->num_rows();
            chunk->reset();
        }
        ASSERT_EQ(rows, kChunkSize);
    }

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    constexpr static int64_t kPartitionId = 7561;
};

// Baseline for the main fix: a sync vector index must be rebuilt inline by the partial-update
// rewrite — dest segment records the index id and the .vi lands at the segment-name-keyed path.
TEST_F(LakePartialUpdateVectorIndexTest, test_sync_rewrite_builds_vi_for_unmodified_vector_column) {
    ConfigResetGuard<int32_t> threshold_guard(&config::config_vector_index_default_build_threshold, 1);
    create_tablet(/*auto_increment=*/false, /*async_threshold=*/0);

    auto version = write_full(1);

    // Partial update of (c0, c1): the vector column c2 is unmodified and re-materialized.
    std::vector<SlotDescriptor> slots;
    slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
    slots.emplace_back(1, "c1", TypeDescriptor{LogicalType::TYPE_BIGINT});
    std::vector<SlotDescriptor*> slot_ptrs{&slots[0], &slots[1]};
    std::vector<int> v0(kChunkSize);
    std::vector<int64_t> v1(kChunkSize);
    std::iota(v0.begin(), v0.end(), 0);
    std::iota(v1.begin(), v1.end(), 100);
    auto c0 = Int32Column::create();
    auto c1 = Int64Column::create();
    c0->append_numbers(v0.data(), v0.size() * sizeof(int));
    c1->append_numbers(v1.data(), v1.size() * sizeof(int64_t));
    Chunk chunk({std::move(c0), std::move(c1)}, Chunk::SlotHashMap{{0, 0}, {1, 1}});
    version = partial_update(version, &slot_ptrs, chunk, /*miss_auto_increment=*/false);

    auto seg_meta = rewritten_segment_meta(version);
    ASSERT_EQ(seg_meta.vector_index_ids_size(), 1);
    ASSERT_EQ(seg_meta.vector_index_ids(0), kIndexId);
    ASSERT_OK(fs::path_exist(vi_location(seg_meta.filename())) ? Status::OK()
                                                               : Status::NotFound(vi_location(seg_meta.filename())));
    ASSERT_EQ(read_footer(seg_meta.filename()).vector_index_storage_type(), VECTOR_INDEX_STORAGE_STANDALONE);
    check_vector_data(version, /*bias=*/0.1f);
}

// Async index above the deferred-build threshold: the rewritten segment must record the index id
// (the FE-scheduled build task keys off it) and mark the footer STANDALONE.
TEST_F(LakePartialUpdateVectorIndexTest, test_async_rewrite_above_threshold_records_ids) {
    create_tablet(/*auto_increment=*/false, /*async_threshold=*/1);

    auto version = write_full(1);

    std::vector<SlotDescriptor> slots;
    slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
    slots.emplace_back(1, "c1", TypeDescriptor{LogicalType::TYPE_BIGINT});
    std::vector<SlotDescriptor*> slot_ptrs{&slots[0], &slots[1]};
    std::vector<int> v0(kChunkSize);
    std::vector<int64_t> v1(kChunkSize);
    std::iota(v0.begin(), v0.end(), 0);
    std::iota(v1.begin(), v1.end(), 100);
    auto c0 = Int32Column::create();
    auto c1 = Int64Column::create();
    c0->append_numbers(v0.data(), v0.size() * sizeof(int));
    c1->append_numbers(v1.data(), v1.size() * sizeof(int64_t));
    Chunk chunk({std::move(c0), std::move(c1)}, Chunk::SlotHashMap{{0, 0}, {1, 1}});
    version = partial_update(version, &slot_ptrs, chunk, /*miss_auto_increment=*/false);

    auto seg_meta = rewritten_segment_meta(version);
    ASSERT_EQ(seg_meta.vector_index_ids_size(), 1);
    ASSERT_EQ(seg_meta.vector_index_ids(0), kIndexId);
    ASSERT_EQ(read_footer(seg_meta.filename()).vector_index_storage_type(), VECTOR_INDEX_STORAGE_STANDALONE);
}

// Async index below the deferred-build threshold: no build task will run for the rewritten
// segment, so it must record no ids AND mark the footer NONE — a STANDALONE footer would send
// every query looking for a .vi that will never exist.
TEST_F(LakePartialUpdateVectorIndexTest, test_async_rewrite_below_threshold_marks_footer_none) {
    create_tablet(/*auto_increment=*/false, /*async_threshold=*/1000);

    auto version = write_full(1);

    std::vector<SlotDescriptor> slots;
    slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
    slots.emplace_back(1, "c1", TypeDescriptor{LogicalType::TYPE_BIGINT});
    std::vector<SlotDescriptor*> slot_ptrs{&slots[0], &slots[1]};
    std::vector<int> v0(kChunkSize);
    std::vector<int64_t> v1(kChunkSize);
    std::iota(v0.begin(), v0.end(), 0);
    std::iota(v1.begin(), v1.end(), 100);
    auto c0 = Int32Column::create();
    auto c1 = Int64Column::create();
    c0->append_numbers(v0.data(), v0.size() * sizeof(int));
    c1->append_numbers(v1.data(), v1.size() * sizeof(int64_t));
    Chunk chunk({std::move(c0), std::move(c1)}, Chunk::SlotHashMap{{0, 0}, {1, 1}});
    version = partial_update(version, &slot_ptrs, chunk, /*miss_auto_increment=*/false);

    auto seg_meta = rewritten_segment_meta(version);
    ASSERT_EQ(seg_meta.vector_index_ids_size(), 0);
    ASSERT_EQ(read_footer(seg_meta.filename()).vector_index_storage_type(), VECTOR_INDEX_STORAGE_NONE);
}

// The update set includes the vector column itself (sync mode): the raw-copied vector data in the
// dest segment is identical to the src partial segment's, so its .vi must be carried over to the
// dest segment name and the id recorded — otherwise the index is silently lost.
TEST_F(LakePartialUpdateVectorIndexTest, test_sync_rewrite_carries_vi_for_updated_vector_column) {
    ConfigResetGuard<int32_t> threshold_guard(&config::config_vector_index_default_build_threshold, 1);
    create_tablet(/*auto_increment=*/false, /*async_threshold=*/0);

    auto version = write_full(1);

    // Partial update of (c0, c2): the vector column is load-provided; c1 is re-materialized.
    std::vector<SlotDescriptor> slots;
    slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
    TypeDescriptor array_type(LogicalType::TYPE_ARRAY);
    array_type.children.emplace_back(LogicalType::TYPE_FLOAT);
    slots.emplace_back(1, "c2", array_type);
    std::vector<SlotDescriptor*> slot_ptrs{&slots[0], &slots[1]};
    std::vector<int> v0(kChunkSize);
    std::iota(v0.begin(), v0.end(), 0);
    auto c0 = Int32Column::create();
    c0->append_numbers(v0.data(), v0.size() * sizeof(int));
    Chunk chunk({std::move(c0), build_vector_column(kChunkSize, /*bias=*/0.5f)}, Chunk::SlotHashMap{{0, 0}, {1, 1}});
    version = partial_update(version, &slot_ptrs, chunk, /*miss_auto_increment=*/false);

    auto seg_meta = rewritten_segment_meta(version);
    ASSERT_EQ(seg_meta.vector_index_ids_size(), 1);
    ASSERT_EQ(seg_meta.vector_index_ids(0), kIndexId);
    ASSERT_OK(fs::path_exist(vi_location(seg_meta.filename())) ? Status::OK()
                                                               : Status::NotFound(vi_location(seg_meta.filename())));
    // The vector column is raw-copied (no column writer), so the footer flag may stay unset; what
    // matters for reads is that it is not an explicit NONE, which would skip the carried .vi.
    auto footer = read_footer(seg_meta.filename());
    ASSERT_FALSE(footer.has_vector_index_storage_type() &&
                 footer.vector_index_storage_type() == VECTOR_INDEX_STORAGE_NONE);
    check_vector_data(version, /*bias=*/0.5f);
}

// Auto-increment rewrite branch (rewrite_auto_increment_lake), async mode: the dest segment is a
// full rewrite re-materializing the vector column, so the id must survive publish — the publish
// path previously cleared it, dropping the FE-scheduled build for the rewritten data.
TEST_F(LakePartialUpdateVectorIndexTest, test_auto_increment_rewrite_keeps_async_vector_index) {
    create_tablet(/*auto_increment=*/true, /*async_threshold=*/1);

    auto version = write_full(1);

    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("StorageEngine::get_next_increment_id_interval.1", [](void* arg) {
        auto& meta = *(std::shared_ptr<AutoIncrementMeta>*)(arg);
        meta->min = 1;
        meta->max = kChunkSize * 2;
    });

    // Partial update of (c0, c1) with c1 = 0 meaning "fill from the auto-increment allocator";
    // the vector column c2 is unmodified and re-materialized by rewrite_auto_increment_lake.
    std::vector<SlotDescriptor> slots;
    slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
    slots.emplace_back(1, "c1", TypeDescriptor{LogicalType::TYPE_BIGINT});
    std::vector<SlotDescriptor*> slot_ptrs{&slots[0], &slots[1]};
    std::vector<int> v0(kChunkSize);
    std::vector<int64_t> v1(kChunkSize, 0);
    std::iota(v0.begin(), v0.end(), 0);
    auto c0 = Int32Column::create();
    auto c1 = Int64Column::create();
    c0->append_numbers(v0.data(), v0.size() * sizeof(int));
    c1->append_numbers(v1.data(), v1.size() * sizeof(int64_t));
    Chunk chunk({std::move(c0), std::move(c1)}, Chunk::SlotHashMap{{0, 0}, {1, 1}});
    version = partial_update(version, &slot_ptrs, chunk, /*miss_auto_increment=*/true);

    SyncPoint::GetInstance()->ClearAllCallBacks();
    SyncPoint::GetInstance()->DisableProcessing();

    auto seg_meta = rewritten_segment_meta(version);
    ASSERT_EQ(seg_meta.vector_index_ids_size(), 1);
    ASSERT_EQ(seg_meta.vector_index_ids(0), kIndexId);
    ASSERT_EQ(read_footer(seg_meta.filename()).vector_index_storage_type(), VECTOR_INDEX_STORAGE_STANDALONE);
    check_vector_data(version, /*bias=*/0.1f);
}

// Auto-increment rewrite branch, sync mode: the .vi must be built inline at the dest
// segment-name-keyed path and the id recorded, mirroring the rewrite_partial_update branch.
TEST_F(LakePartialUpdateVectorIndexTest, test_auto_increment_rewrite_builds_sync_vector_index) {
    ConfigResetGuard<int32_t> threshold_guard(&config::config_vector_index_default_build_threshold, 1);
    create_tablet(/*auto_increment=*/true, /*async_threshold=*/0);

    auto version = write_full(1);

    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("StorageEngine::get_next_increment_id_interval.1", [](void* arg) {
        auto& meta = *(std::shared_ptr<AutoIncrementMeta>*)(arg);
        meta->min = 1;
        meta->max = kChunkSize * 2;
    });

    std::vector<SlotDescriptor> slots;
    slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
    slots.emplace_back(1, "c1", TypeDescriptor{LogicalType::TYPE_BIGINT});
    std::vector<SlotDescriptor*> slot_ptrs{&slots[0], &slots[1]};
    std::vector<int> v0(kChunkSize);
    std::vector<int64_t> v1(kChunkSize, 0);
    std::iota(v0.begin(), v0.end(), 0);
    auto c0 = Int32Column::create();
    auto c1 = Int64Column::create();
    c0->append_numbers(v0.data(), v0.size() * sizeof(int));
    c1->append_numbers(v1.data(), v1.size() * sizeof(int64_t));
    Chunk chunk({std::move(c0), std::move(c1)}, Chunk::SlotHashMap{{0, 0}, {1, 1}});
    version = partial_update(version, &slot_ptrs, chunk, /*miss_auto_increment=*/true);

    SyncPoint::GetInstance()->ClearAllCallBacks();
    SyncPoint::GetInstance()->DisableProcessing();

    auto seg_meta = rewritten_segment_meta(version);
    ASSERT_EQ(seg_meta.vector_index_ids_size(), 1);
    ASSERT_EQ(seg_meta.vector_index_ids(0), kIndexId);
    ASSERT_OK(fs::path_exist(vi_location(seg_meta.filename())) ? Status::OK()
                                                               : Status::NotFound(vi_location(seg_meta.filename())));
    ASSERT_EQ(read_footer(seg_meta.filename()).vector_index_storage_type(), VECTOR_INDEX_STORAGE_STANDALONE);
    check_vector_data(version, /*bias=*/0.1f);
}

// Copy-only rewrite (async): when a schema change drops the only column the partial update left
// unmodified between the write and its publish, rewrite_partial_update degenerates to a raw file
// copy and records no ids itself. The src partial segment's scheduled async ids must be carried
// onto the dest segment instead of being wiped by the metadata refresh — the FE build task and
// vacuum key off them.
TEST_F(LakePartialUpdateVectorIndexTest, test_async_copy_only_rewrite_keeps_scheduled_ids) {
    create_tablet(/*auto_increment=*/false, /*async_threshold=*/1, /*with_extra_column=*/true);

    auto version = write_full(1);

    // Partial update covering c0..c2 (c3 stays unmodified), including the vector column, so the
    // src partial segment records the async index id at write time (rows >= threshold).
    std::vector<SlotDescriptor> slots;
    slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
    slots.emplace_back(1, "c1", TypeDescriptor{LogicalType::TYPE_BIGINT});
    TypeDescriptor array_type(LogicalType::TYPE_ARRAY);
    array_type.children.emplace_back(LogicalType::TYPE_FLOAT);
    slots.emplace_back(2, "c2", array_type);
    std::vector<SlotDescriptor*> slot_ptrs{&slots[0], &slots[1], &slots[2]};
    std::vector<int> v0(kChunkSize);
    std::vector<int64_t> v1(kChunkSize, 200);
    std::iota(v0.begin(), v0.end(), 0);
    auto c0 = Int32Column::create();
    auto c1 = Int64Column::create();
    c0->append_numbers(v0.data(), v0.size() * sizeof(int));
    c1->append_numbers(v1.data(), v1.size() * sizeof(int64_t));
    Chunk chunk({std::move(c0), std::move(c1), build_vector_column(kChunkSize, /*bias=*/0.5f)},
                Chunk::SlotHashMap{{0, 0}, {1, 1}, {2, 2}});
    auto txn_id = partial_update_txn(&slot_ptrs, chunk, /*miss_auto_increment=*/false);

    // Fast schema evolution drops c3 before the publish: the publish-time schema now has no
    // column outside the update set, so unmodified_column_ids is empty and the rewrite takes the
    // copy-only fast path.
    {
        auto base = _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version).value();
        auto mutated = std::make_shared<TabletMetadata>(*base);
        auto* schema = mutated->mutable_schema();
        ASSERT_EQ(schema->column(3).unique_id(), kExtraUid);
        schema->mutable_column()->RemoveLast();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*mutated));
    }
    CHECK_OK(publish_single_version(_tablet_metadata->id(), version + 1, txn_id).status());
    version = version + 1;

    auto seg_meta = rewritten_segment_meta(version);
    ASSERT_EQ(seg_meta.vector_index_ids_size(), 1);
    ASSERT_EQ(seg_meta.vector_index_ids(0), kIndexId);
}

} // namespace starrocks::lake
