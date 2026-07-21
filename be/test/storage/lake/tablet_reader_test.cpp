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

#include "storage/lake/tablet_reader.h"

#include <gtest/gtest.h>

#include <atomic>
#include <iomanip>
#include <set>
#include <utility>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "common/config_ingest_fwd.h"
#include "common/config_lake_fwd.h"
#include "common/logging.h"
#include "exec/pipeline/scan/morsel.h"
#include "gen_cpp/InternalService_types.h"
#include "storage/chunk_helper.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/query/split_scan_morsel.h"
#include "storage/rowset/rowid_range_option.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/segment_options.h"
#include "storage/seek_range.h"
#include "storage/tablet_schema.h"
#include "storage_primitive/rowid_types.h"
#include "test_util.h"

namespace starrocks::lake {

using namespace starrocks;

class LakeDuplicateTabletReaderTest : public TestBase {
public:
    LakeDuplicateTabletReaderTest() : TestBase(kTestDirectory) {
        _tablet_metadata = generate_simple_tablet_metadata(DUP_KEYS);
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override {
        remove_test_dir_ignore_error();
        Segment::toggle_batch_update_cache_mode(true);
    }

protected:
    constexpr static const char* const kTestDirectory = "test_duplicate_lake_tablet_reader";

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
};

TEST_F(LakeDuplicateTabletReaderTest, test_read_success) {
    std::vector<int> k0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};
    std::vector<int> v0{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 41, 44};

    std::vector<int> k1{30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41};
    std::vector<int> v1{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};

    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    auto c2 = Int32Column::create();
    auto c3 = Int32Column::create();
    c0->append_numbers(k0.data(), k0.size() * sizeof(int));
    c1->append_numbers(v0.data(), v0.size() * sizeof(int));
    c2->append_numbers(k1.data(), k1.size() * sizeof(int));
    c3->append_numbers(v1.data(), v1.size() * sizeof(int));

    Chunk chunk0({std::move(c0), std::move(c1)}, _schema);
    Chunk chunk1({std::move(c2), std::move(c3)}, _schema);

    const int segment_rows = chunk0.num_rows() + chunk1.num_rows();

    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);
    {
        int64_t txn_id = next_id();
        // write rowset 1 with 2 segments
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
        ASSERT_OK(writer->open());

        // write rowset data
        // segment #1
        ASSERT_OK(writer->write(chunk0));
        ASSERT_OK(writer->write(chunk1));
        ASSERT_OK(writer->finish());

        // segment #2
        ASSERT_OK(writer->write(chunk0));
        ASSERT_OK(writer->write(chunk1));
        ASSERT_OK(writer->finish());

        const auto& files = writer->segments();
        ASSERT_EQ(2, files.size());

        // add rowset metadata
        auto* rowset = _tablet_metadata->add_rowsets();
        rowset->set_overlapped(true);
        rowset->set_id(1);
        for (const auto& file : writer->segments()) {
            auto* segment_meta = rowset->add_segment_metas();
            segment_meta->set_filename(file.path);
            segment_meta->set_size(file.size.value());
        }

        writer->close();
    }

    // write tablet metadata
    _tablet_metadata->set_version(2);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    int invocation_counter = 0;
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("lake::TabletManager::update_segment_cache_size",
                                          [&](void* arg) { ++invocation_counter; });
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("lake::TabletManager::update_segment_cache_size");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    ASSERT_EQ(0, invocation_counter);

    // test reader
    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), _tablet_metadata, *_schema);
    ASSERT_OK(reader->prepare());
    TabletReaderParams params;
    ASSERT_OK(reader->open(params));

    auto read_chunk_ptr = ChunkFactory::new_chunk(*_schema, 1024);
    for (int j = 0; j < 2; ++j) {
        read_chunk_ptr->reset();
        ASSERT_OK(reader->get_next(read_chunk_ptr.get()));
        ASSERT_EQ(segment_rows, read_chunk_ptr->num_rows());
        for (int i = 0, sz = k0.size(); i < sz; i++) {
            EXPECT_EQ(k0[i], read_chunk_ptr->get(i)[0].get_int32());
            EXPECT_EQ(v0[i], read_chunk_ptr->get(i)[1].get_int32());
        }
        for (int i = 0, sz = k1.size(); i < sz; i++) {
            EXPECT_EQ(k1[i], read_chunk_ptr->get(k0.size() + i)[0].get_int32());
            EXPECT_EQ(v1[i], read_chunk_ptr->get(k0.size() + i)[1].get_int32());
        }
    }
    // With the fix: 2 segments, 2 calls per segment: 1 for _open, 1 for initializing column iterators
    // total = 2 segments × (1 segment open + 1 column init) = 4 calls
    // Without the fix: there would be 6 invocations: 2 segment opens and 2 column inits per segment.
    // total = 2 segments × (1 segment open + 2 column inits) = 6 calls
    ASSERT_EQ(4, invocation_counter);

    read_chunk_ptr->reset();
    ASSERT_TRUE(reader->get_next(read_chunk_ptr.get()).is_end_of_file());

    reader->close();
}

// Regression test for issue #75203: a transient object-storage failure while a physical-split scan
// lazily loads a lake rowset's segments used to leave PhysicalSplitMorselQueue's range iterator
// uninitialized; a successful retry inside the same loop then dereferenced a null _range in
// SparseRangeIterator::has_more() -> SIGSEGV @0x0.
//
// We reproduce the exact non-idempotency window with a SyncPoint that fails the FIRST segment-load
// attempt and succeeds afterward, then drive PhysicalSplitMorselQueue::try_get(). With the fix
// (Rowset::get_segments_checked() primed in _init_segment), the failure surfaces as its real,
// retryable Status instead of crashing, and repeated try_get() calls stay crash-free.
TEST_F(LakeDuplicateTabletReaderTest, test_issue_75203_physical_split_transient_segment_load) {
    // 1. Write one rowset with one segment that has rows.
    std::vector<int> k{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    std::vector<int> v{10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    c0->append_numbers(k.data(), k.size() * sizeof(int));
    c1->append_numbers(v.data(), v.size() * sizeof(int));
    Chunk chunk({std::move(c0), std::move(c1)}, _schema);

    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);
    {
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
        ASSERT_OK(writer->open());
        ASSERT_OK(writer->write(chunk));
        ASSERT_OK(writer->finish());
        auto* rowset_meta = _tablet_metadata->add_rowsets();
        rowset_meta->set_overlapped(false);
        rowset_meta->set_id(1);
        for (const auto& file : writer->segments()) {
            auto* seg = rowset_meta->add_segment_metas();
            seg->set_filename(file.path);
            seg->set_size(file.size.value());
        }
        writer->close();
    }
    _tablet_metadata->set_version(2);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    // 2. Enable parallel segment load so the injectable sync point fires.
    auto old_parallel = config::enable_load_segment_parallel;
    config::enable_load_segment_parallel = true;
    DeferOp restore_cfg([&]() { config::enable_load_segment_parallel = old_parallel; });

    // 3. Fail the FIRST segment-load attempt, succeed after: the #75203 non-idempotency window.
    std::atomic<int> load_calls{0};
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("Rowset::load_segments::parallel_load", [&](void* arg) {
        if (load_calls.fetch_add(1) == 0) {
            *reinterpret_cast<Status*>(arg) = Status::IOError("injected transient segment load failure (#75203)");
        }
    });
    DeferOp clear_sp([]() {
        SyncPoint::GetInstance()->ClearCallBack("Rowset::load_segments::parallel_load");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    // 4. Fresh lake rowsets (empty _segments, so the first checked load actually hits object storage).
    auto rowsets = Rowset::get_rowsets(_tablet_mgr.get(), _tablet_metadata);
    ASSERT_FALSE(rowsets.empty());

    // 5. Build a PhysicalSplitMorselQueue over the tablet + rowset (physical split; small
    //    splitted_scan_rows forces the split walk).
    TScanRange scan_range;
    TInternalScanRange internal_scan_range;
    internal_scan_range.tablet_id = _tablet_metadata->id();
    internal_scan_range.version = "2";
    internal_scan_range.partition_id = 1;
    scan_range.__set_internal_scan_range(internal_scan_range);

    pipeline::Morsels morsels;
    morsels.emplace_back(std::make_unique<pipeline::ScanMorsel>(1, scan_range));
    pipeline::PhysicalSplitMorselQueue queue(std::move(morsels), 1, 1);

    auto lake_tablet = std::make_shared<Tablet>(_tablet_mgr.get(), _tablet_metadata->id());
    std::vector<BaseTabletSharedPtr> tablets{lake_tablet};
    queue.set_tablets(tablets);

    std::vector<std::vector<BaseRowsetSharedPtr>> tablet_rowsets;
    tablet_rowsets.push_back({rowsets[0]});
    queue.set_tablet_rowsets(tablet_rowsets);
    queue.set_tablet_schema(_tablet_schema);

    // 6. Without the fix this crashes (@0x0) in has_more(); with the fix the transient failure
    //    surfaces as its real Status from try_get(), no crash.
    auto result = queue.try_get();
    ASSERT_FALSE(result.ok());
    EXPECT_NE(result.status().to_string().find("injected"), std::string::npos) << result.status().to_string();

    // The crash repetition ends: a second call also does not crash (queue is exhausted).
    auto again = queue.try_get();
    ASSERT_TRUE(again.ok());
    ASSERT_EQ(again.value(), nullptr);
}

class LakeAggregateTabletReaderTest : public TestBase {
public:
    LakeAggregateTabletReaderTest() : TestBase(kTestDirectory) {
        _tablet_metadata = generate_simple_tablet_metadata(AGG_KEYS);
        _tablet_metadata->mutable_schema()->mutable_column(1)->set_aggregation("SUM");
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

protected:
    constexpr static const char* const kTestDirectory = "test_aggregate_lake_tablet_reader";

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
};

TEST_F(LakeAggregateTabletReaderTest, test_read_success) {
    std::vector<int> k0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};
    std::vector<int> v0{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 41, 44};

    std::vector<int> k1{30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41};
    std::vector<int> v1{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};

    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    auto c2 = Int32Column::create();
    auto c3 = Int32Column::create();
    c0->append_numbers(k0.data(), k0.size() * sizeof(int));
    c1->append_numbers(v0.data(), v0.size() * sizeof(int));
    c2->append_numbers(k1.data(), k1.size() * sizeof(int));
    c3->append_numbers(v1.data(), v1.size() * sizeof(int));

    Chunk chunk0({std::move(c0), std::move(c1)}, _schema);
    Chunk chunk1({std::move(c2), std::move(c3)}, _schema);

    const int segment_rows = chunk0.num_rows() + chunk1.num_rows();

    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);

    {
        // write rowset 1 with 2 segments
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
        ASSERT_OK(writer->open());

        // write rowset data
        // segment #1
        ASSERT_OK(writer->write(chunk0));
        ASSERT_OK(writer->write(chunk1));
        ASSERT_OK(writer->finish());

        // segment #2
        ASSERT_OK(writer->write(chunk0));
        ASSERT_OK(writer->write(chunk1));
        ASSERT_OK(writer->finish());

        const auto& files = writer->segments();
        ASSERT_EQ(2, files.size());

        // add rowset metadata
        auto* rowset = _tablet_metadata->add_rowsets();
        rowset->set_overlapped(true);
        rowset->set_id(1);
        for (const auto& file : writer->segments()) {
            auto* segment_meta = rowset->add_segment_metas();
            segment_meta->set_filename(file.path);
            segment_meta->set_size(file.size.value());
        }

        writer->close();
    }

    {
        // write rowset 2 with 1 segment
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
        ASSERT_OK(writer->open());

        // write rowset data
        // segment #1
        ASSERT_OK(writer->write(chunk0));
        ASSERT_OK(writer->write(chunk1));
        ASSERT_OK(writer->finish());

        const auto& files = writer->segments();
        ASSERT_EQ(1, files.size());

        // add rowset metadata
        auto* rowset = _tablet_metadata->add_rowsets();
        rowset->set_overlapped(false);
        rowset->set_id(2);
        for (const auto& file : writer->segments()) {
            auto* segment_meta = rowset->add_segment_metas();
            segment_meta->set_filename(file.path);
            segment_meta->set_size(file.size.value());
        }

        writer->close();
    }

    // write tablet metadata
    _tablet_metadata->set_version(3);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    // test reader
    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), _tablet_metadata, *_schema);
    ASSERT_OK(reader->prepare());
    TabletReaderParams params;
    ASSERT_OK(reader->open(params));

    auto read_chunk_ptr = ChunkFactory::new_chunk(*_schema, 1024);
    ASSERT_OK(reader->get_next(read_chunk_ptr.get()));
    ASSERT_EQ(segment_rows, read_chunk_ptr->num_rows());
    for (int i = 0, sz = k0.size(); i < sz; i++) {
        EXPECT_EQ(k0[i], read_chunk_ptr->get(i)[0].get_int32());
        EXPECT_EQ(v0[i] * 3, read_chunk_ptr->get(i)[1].get_int32());
    }
    for (int i = 0, sz = k1.size(); i < sz; i++) {
        EXPECT_EQ(k1[i], read_chunk_ptr->get(k0.size() + i)[0].get_int32());
        EXPECT_EQ(v1[i] * 3, read_chunk_ptr->get(k0.size() + i)[1].get_int32());
    }

    read_chunk_ptr->reset();
    ASSERT_TRUE(reader->get_next(read_chunk_ptr.get()).is_end_of_file());

    reader->close();
}

class LakeDuplicateTabletReaderWithDeleteTest : public TestBase {
public:
    LakeDuplicateTabletReaderWithDeleteTest() : TestBase(kTestDirectory) {
        _tablet_metadata = generate_simple_tablet_metadata(DUP_KEYS);
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

protected:
    constexpr static const char* const kTestDirectory = "test_duplicate_lake_tablet_reader_with_delete";

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
};

TEST_F(LakeDuplicateTabletReaderWithDeleteTest, test_read_success) {
    std::vector<int> k0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};
    std::vector<int> v0{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 41, 44};

    std::vector<int> k1{30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41};
    std::vector<int> v1{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};

    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    auto c2 = Int32Column::create();
    auto c3 = Int32Column::create();
    c0->append_numbers(k0.data(), k0.size() * sizeof(int));
    c1->append_numbers(v0.data(), v0.size() * sizeof(int));
    c2->append_numbers(k1.data(), k1.size() * sizeof(int));
    c3->append_numbers(v1.data(), v1.size() * sizeof(int));

    Chunk chunk0({std::move(c0), std::move(c1)}, _schema);
    Chunk chunk1({std::move(c2), std::move(c3)}, _schema);

    const int segment_rows = chunk0.num_rows() + chunk1.num_rows();

    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);

    {
        // write rowset 1 with 2 segments
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
        ASSERT_OK(writer->open());

        // write rowset data
        // segment #1
        ASSERT_OK(writer->write(chunk0));
        ASSERT_OK(writer->write(chunk1));
        ASSERT_OK(writer->finish());

        // segment #2
        ASSERT_OK(writer->write(chunk0));
        ASSERT_OK(writer->write(chunk1));
        ASSERT_OK(writer->finish());

        const auto& files = writer->segments();
        ASSERT_EQ(2, files.size());

        // add rowset metadata
        auto* rowset = _tablet_metadata->add_rowsets();
        rowset->set_overlapped(true);
        rowset->set_id(1);
        for (const auto& file : writer->segments()) {
            auto* segment_meta = rowset->add_segment_metas();
            segment_meta->set_filename(file.path);
            segment_meta->set_size(file.size.value());
        }

        writer->close();
    }

    { // Add empty delete_predicate, won't affect anything
        auto* rowset = _tablet_metadata->add_rowsets();
        rowset->set_overlapped(false);
        rowset->set_num_rows(0);
        rowset->set_data_size(0);

        auto* empty_delete_predicate = rowset->mutable_delete_predicate();
        empty_delete_predicate->set_version(-1);
    }

    {
        auto* rowset = _tablet_metadata->add_rowsets();
        rowset->set_overlapped(false);
        rowset->set_num_rows(0);
        rowset->set_data_size(0);

        auto* delete_predicate = rowset->mutable_delete_predicate();
        delete_predicate->set_version(-1);
        // delete c0 (21, 22, 30)
        auto* binary_predicate = delete_predicate->add_binary_predicates();
        binary_predicate->set_column_name("c0");
        binary_predicate->set_op("<=");
        binary_predicate->set_value("30");
        auto* in_predicate = delete_predicate->add_in_predicates();
        in_predicate->set_column_name("c1");
        in_predicate->set_is_not_in(false);
        in_predicate->add_values("41");
        in_predicate->add_values("44");
        in_predicate->add_values("0");
        in_predicate->add_values("1");

        // This is to simulate the bug where a delete predicate references a non-existent column.
        auto* invalid_binary_predicate = delete_predicate->add_binary_predicates();
        invalid_binary_predicate->set_column_name("c0c"); // column name doesn't exist
        invalid_binary_predicate->set_op("=");
        invalid_binary_predicate->set_value("30");
    }

    // write tablet metadata
    _tablet_metadata->set_version(3);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    bool original_ignore_config_val = config::lake_tablet_ignore_invalid_delete_predicate;
    config::lake_tablet_ignore_invalid_delete_predicate = false;

    { // test reader open failed due to invalid delete_predicate
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), _tablet_metadata, *_schema);
        ASSERT_OK(reader->prepare());
        TabletReaderParams params;
        auto st = reader->open(params);
        EXPECT_FALSE(st.ok());
        EXPECT_TRUE(st.is_unknown()) << st;
        EXPECT_EQ("unknown column c0c", st.message());
    }

    config::lake_tablet_ignore_invalid_delete_predicate = true;

    // test reader
    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), _tablet_metadata, *_schema);
    ASSERT_OK(reader->prepare());
    TabletReaderParams params;
    ASSERT_OK(reader->open(params));

    auto read_chunk_ptr = ChunkFactory::new_chunk(*_schema, 1024);
    for (int j = 0; j < 2; ++j) {
        read_chunk_ptr->reset();
        ASSERT_OK(reader->get_next(read_chunk_ptr.get()));
        ASSERT_EQ(segment_rows - 3, read_chunk_ptr->num_rows());
        int chunk_index = 0;
        int sz0 = k0.size() - 2;
        for (int i = 0; i < sz0; i++) {
            EXPECT_EQ(k0[i], read_chunk_ptr->get(chunk_index)[0].get_int32());
            EXPECT_EQ(v0[i], read_chunk_ptr->get(chunk_index)[1].get_int32());
            ++chunk_index;
        }
        int sz1 = k1.size() - 1;
        for (int i = 0; i < sz1; i++) {
            EXPECT_EQ(k1[i + 1], read_chunk_ptr->get(chunk_index)[0].get_int32());
            EXPECT_EQ(v1[i + 1], read_chunk_ptr->get(chunk_index)[1].get_int32());
            ++chunk_index;
        }
    }

    read_chunk_ptr->reset();
    ASSERT_TRUE(reader->get_next(read_chunk_ptr.get()).is_end_of_file());

    reader->close();

    config::lake_tablet_ignore_invalid_delete_predicate = original_ignore_config_val;
}

class LakeDuplicateTabletReaderWithDeleteNotInOneValueTest : public TestBase {
public:
    LakeDuplicateTabletReaderWithDeleteNotInOneValueTest() : TestBase(kTestDirectory) {
        _tablet_metadata = generate_simple_tablet_metadata(DUP_KEYS);
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

protected:
    constexpr static const char* const kTestDirectory = "test_duplicate_lake_tablet_reader_with_delete_not_in_one";

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
};

TEST_F(LakeDuplicateTabletReaderWithDeleteNotInOneValueTest, test_read_success) {
    std::vector<int> k0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};
    std::vector<int> v0{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 41, 44};

    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    c0->append_numbers(k0.data(), k0.size() * sizeof(int));
    c1->append_numbers(v0.data(), v0.size() * sizeof(int));

    Chunk chunk0({std::move(c0), std::move(c1)}, _schema);

    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);
    {
        // write rowset 1 with 1 segments
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
        ASSERT_OK(writer->open());

        // write rowset data
        ASSERT_OK(writer->write(chunk0));
        ASSERT_OK(writer->finish());

        const auto& files = writer->segments();
        ASSERT_EQ(1, files.size());

        // add rowset metadata
        auto* rowset = _tablet_metadata->add_rowsets();
        rowset->set_overlapped(true);
        rowset->set_id(1);
        for (const auto& file : writer->segments()) {
            auto* segment_meta = rowset->add_segment_metas();
            segment_meta->set_filename(file.path);
            segment_meta->set_size(file.size.value());
        }

        writer->close();
    }

    {
        auto* rowset = _tablet_metadata->add_rowsets();
        rowset->set_overlapped(false);
        rowset->set_num_rows(0);
        rowset->set_data_size(0);

        auto* delete_predicate = rowset->mutable_delete_predicate();
        delete_predicate->set_version(-1);
        // delete c0 not in (10)
        auto* in_predicate = delete_predicate->add_in_predicates();
        in_predicate->set_column_name("c0");
        in_predicate->set_is_not_in(true);
        in_predicate->add_values("10");
    }

    // write tablet metadata
    _tablet_metadata->set_version(3);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    // test reader
    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), _tablet_metadata, *_schema);
    ASSERT_OK(reader->prepare());
    TabletReaderParams params;
    ASSERT_OK(reader->open(params));

    auto read_chunk_ptr = ChunkFactory::new_chunk(*_schema, 1024);
    ASSERT_OK(reader->get_next(read_chunk_ptr.get()));
    ASSERT_EQ(1, read_chunk_ptr->num_rows());
    EXPECT_EQ(10, read_chunk_ptr->get(0)[0].get_int32());
    EXPECT_EQ(20, read_chunk_ptr->get(0)[1].get_int32());

    read_chunk_ptr->reset();
    ASSERT_TRUE(reader->get_next(read_chunk_ptr.get()).is_end_of_file());

    reader->close();
}

class LakeTabletReaderSpit : public TestBase {
public:
    LakeTabletReaderSpit() : TestBase(kTestDirectory) {
        _tablet_metadata = generate_simple_tablet_metadata(DUP_KEYS);
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

    TabletReaderParams generate_tablet_reader_params(TScanRange* scan_range) {
        TabletReaderParams params;
        params.splitted_scan_rows = 4;
        params.scan_dop = 4;
        params.plan_node_id = 1;
        params.start_key = std::vector<OlapTuple>();
        params.end_key = std::vector<OlapTuple>();
        params.scan_range = scan_range;
        return params;
    }

protected:
    constexpr static const char* const kTestDirectory = "test_tablet_reader_split";

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    std::shared_ptr<TScanRange> _scan_range;
};

TEST_F(LakeTabletReaderSpit, test_reader_split) {
    std::vector<int> k0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};
    std::vector<int> v0{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 41, 44};

    std::vector<int> k1{30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41};
    std::vector<int> v1{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};

    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    auto c2 = Int32Column::create();
    auto c3 = Int32Column::create();
    c0->append_numbers(k0.data(), k0.size() * sizeof(int));
    c1->append_numbers(v0.data(), v0.size() * sizeof(int));
    c2->append_numbers(k1.data(), k1.size() * sizeof(int));
    c3->append_numbers(v1.data(), v1.size() * sizeof(int));

    Chunk chunk0({std::move(c0), std::move(c1)}, _schema);
    Chunk chunk1({std::move(c2), std::move(c3)}, _schema);

    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);

    {
        // write rowset 1 with 2 segments
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
        ASSERT_OK(writer->open());

        // write rowset data
        // segment #1
        ASSERT_OK(writer->write(chunk0));
        ASSERT_OK(writer->write(chunk1));
        ASSERT_OK(writer->finish());

        // segment #2
        ASSERT_OK(writer->write(chunk0));
        ASSERT_OK(writer->write(chunk1));
        ASSERT_OK(writer->finish());

        const auto& files = writer->segments();
        ASSERT_EQ(2, files.size());

        // add rowset metadata
        auto* rowset = _tablet_metadata->add_rowsets();
        rowset->set_overlapped(true);
        rowset->set_id(1);
        rowset->set_num_rows(2 * (chunk0.num_rows() + chunk1.num_rows()));
        for (const auto& file : writer->segments()) {
            auto* segment_meta = rowset->add_segment_metas();
            segment_meta->set_filename(file.path);
            segment_meta->set_size(file.size.value());
        }

        writer->close();
    }

    {
        // write rowset 2 with 1 segment
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
        ASSERT_OK(writer->open());

        // write rowset data
        // segment #1
        ASSERT_OK(writer->write(chunk0));
        ASSERT_OK(writer->write(chunk1));
        ASSERT_OK(writer->finish());

        const auto& files = writer->segments();
        ASSERT_EQ(1, files.size());

        // add rowset metadata
        auto* rowset = _tablet_metadata->add_rowsets();
        rowset->set_overlapped(false);
        rowset->set_id(2);
        rowset->set_num_rows(chunk0.num_rows() + chunk1.num_rows());
        for (const auto& file : writer->segments()) {
            auto* segment_meta = rowset->add_segment_metas();
            segment_meta->set_filename(file.path);
            segment_meta->set_size(file.size.value());
        }

        writer->close();
    }

    // write tablet metadata
    _tablet_metadata->set_version(3);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    {
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), _tablet_metadata, *_schema, true, true);

        // construct scan_range
        TInternalScanRange internal_scan_range;
        internal_scan_range.__set_tablet_id(_tablet_metadata->id());
        internal_scan_range.__set_version(std::to_string(_tablet_metadata->version()));
        TScanRange scan_range;
        scan_range.__set_internal_scan_range(internal_scan_range);
        auto params = generate_tablet_reader_params(&scan_range);

        ASSERT_OK(reader->prepare());
        ASSERT_OK(reader->open(params));

        std::vector<pipeline::ScanSplitContextPtr> split_tasks;
        reader->get_split_tasks(&split_tasks);
        ASSERT_GT(split_tasks.size(), 0);

        reader->close();
    }

    {
        // test read data
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), _tablet_metadata, *_schema, false, false);

        // construct scan_range
        TInternalScanRange internal_scan_range;
        internal_scan_range.__set_tablet_id(_tablet_metadata->id());
        internal_scan_range.__set_version(std::to_string(_tablet_metadata->version()));
        TScanRange scan_range;
        scan_range.__set_internal_scan_range(internal_scan_range);
        auto params = generate_tablet_reader_params(&scan_range);

        // construct rowid_range_option
        auto rowid_range_option = std::make_shared<RowidRangeOption>();
        Rowset rowset(_tablet_mgr.get(), _tablet_metadata, 1, 0 /* compaction_segment_limit */);
        auto segment = rowset.get_segments().back();
        auto sparse_range = std::make_shared<SparseRange<rowid_t>>(1, 21);
        rowid_range_option->add(&rowset, segment.get(), sparse_range, true);
        params.rowid_range_option = rowid_range_option;

        ASSERT_OK(reader->prepare());
        ASSERT_OK(reader->open(params));

        auto read_chunk_ptr = ChunkFactory::new_chunk(*_schema, 1024);
        read_chunk_ptr->reset();
        ASSERT_OK(reader->get_next(read_chunk_ptr.get()));
        ASSERT_EQ(20, read_chunk_ptr->num_rows());

        read_chunk_ptr->reset();
        ASSERT_TRUE(reader->get_next(read_chunk_ptr.get()).is_end_of_file());

        reader->close();
    }
}

class DISABLED_LakeLoadSegmentParallelTest : public TestBase {
public:
    DISABLED_LakeLoadSegmentParallelTest() : TestBase(kTestDirectory) {
        _tablet_metadata = std::make_shared<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | NO  |  NO  |
        auto schema = _tablet_metadata->mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(DUP_KEYS);
        schema->set_num_rows_per_row_block(65535);
        auto c0 = schema->add_column();
        {
            c0->set_unique_id(next_id());
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        auto c1 = schema->add_column();
        {
            c1->set_unique_id(next_id());
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(false);
            c1->set_is_nullable(false);
        }

        _tablet_schema = TabletSchema::create(*schema);
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_load_segment_parallel_test";

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
};

TEST_F(DISABLED_LakeLoadSegmentParallelTest, test_normal) {
    std::vector<int> k0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};
    std::vector<int> v0{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 41, 44};

    std::vector<int> k1{30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41};
    std::vector<int> v1{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};

    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    auto c2 = Int32Column::create();
    auto c3 = Int32Column::create();
    c0->append_numbers(k0.data(), k0.size() * sizeof(int));
    c1->append_numbers(v0.data(), v0.size() * sizeof(int));
    c2->append_numbers(k1.data(), k1.size() * sizeof(int));
    c3->append_numbers(v1.data(), v1.size() * sizeof(int));

    Chunk chunk0({std::move(c0), std::move(c1)}, _schema);
    Chunk chunk1({std::move(c2), std::move(c3)}, _schema);

    const int segment_rows = chunk0.num_rows() + chunk1.num_rows();

    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);
    {
        int64_t txn_id = next_id();
        // write rowset 1 with 2 segments
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
        ASSERT_OK(writer->open());

        // write rowset data
        // segment #1
        ASSERT_OK(writer->write(chunk0));
        ASSERT_OK(writer->write(chunk1));
        ASSERT_OK(writer->finish());

        // segment #2
        ASSERT_OK(writer->write(chunk0));
        ASSERT_OK(writer->write(chunk1));
        ASSERT_OK(writer->finish());

        const auto& files = writer->segments();
        ASSERT_EQ(2, files.size());

        // add rowset metadata
        auto* rowset = _tablet_metadata->add_rowsets();
        rowset->set_overlapped(true);
        rowset->set_id(1);
        for (const auto& file : writer->segments()) {
            auto* segment_meta = rowset->add_segment_metas();
            segment_meta->set_filename(file.path);
            segment_meta->set_size(file.size.value());
        }

        writer->close();
    }

    // write tablet metadata
    _tablet_metadata->set_version(2);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    // test reader
    config::enable_load_segment_parallel = true;
    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), _tablet_metadata, *_schema);
    ASSERT_OK(reader->prepare());
    TabletReaderParams params;
    ASSERT_OK(reader->open(params));

    auto read_chunk_ptr = ChunkFactory::new_chunk(*_schema, 1024);
    for (int j = 0; j < 2; ++j) {
        read_chunk_ptr->reset();
        ASSERT_OK(reader->get_next(read_chunk_ptr.get()));
        ASSERT_EQ(segment_rows, read_chunk_ptr->num_rows());
        for (int i = 0, sz = k0.size(); i < sz; i++) {
            EXPECT_EQ(k0[i], read_chunk_ptr->get(i)[0].get_int32());
            EXPECT_EQ(v0[i], read_chunk_ptr->get(i)[1].get_int32());
        }
        for (int i = 0, sz = k1.size(); i < sz; i++) {
            EXPECT_EQ(k1[i], read_chunk_ptr->get(k0.size() + i)[0].get_int32());
            EXPECT_EQ(v1[i], read_chunk_ptr->get(k0.size() + i)[1].get_int32());
        }
    }

    read_chunk_ptr->reset();
    ASSERT_TRUE(reader->get_next(read_chunk_ptr.get()).is_end_of_file());

    reader->close();
}

class LakeDuplicateTablet10kColumnReaderTest : public TestBase {
public:
    LakeDuplicateTablet10kColumnReaderTest() : TestBase(kTestDirectory) {
        _tablet_metadata = generate_simple_tablet_metadata(DUP_KEYS, kColumnSize);
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override {
        remove_test_dir_ignore_error();
        Segment::toggle_batch_update_cache_mode(true);
    }

    void test_10k_column_read_perf_body(bool enable_batch_update, size_t* time_costs);

    StatusOr<std::pair<size_t, size_t>> testOneRoundGetColumnInit(const Chunk& chunk, size_t num_rows,
                                                                  size_t num_columns);

protected:
    constexpr static const char* const kTestDirectory = "test_duplicate_lake_tablet_reader_10k";
    constexpr static const size_t kColumnSize = 10000;

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
};

static Chunk generate_chunk(SchemaPtr schema, size_t num_rows, size_t num_columns) {
    Columns columns;
    columns.reserve(num_columns);
    for (int i = 0; i < num_columns; ++i) {
        auto col = Int32Column::create();
        std::vector<int> col_v;
        col_v.reserve(num_rows);
        for (int j = 0; j < num_rows; ++j) {
            col_v.emplace_back(i * 1000 + j);
        }
        col->append_numbers(col_v.data(), col_v.size() * sizeof(int));
        columns.push_back(std::move(col));
    }
    return Chunk(std::move(columns), schema);
}

void LakeDuplicateTablet10kColumnReaderTest::test_10k_column_read_perf_body(bool enable_batch_update,
                                                                            size_t* time_costs) {
    Segment::toggle_batch_update_cache_mode(enable_batch_update);

    auto chunk = generate_chunk(_schema, 100, kColumnSize);
    const int segment_rows = chunk.num_rows();

    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);
    // clear historic rowsets
    _tablet_metadata->clear_rowsets();
    {
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
        ASSERT_OK(writer->open());
        ASSERT_OK(writer->write(chunk));
        ASSERT_OK(writer->finish());

        const auto& files = writer->segments();
        ASSERT_EQ(1, files.size());
        auto* rowset = _tablet_metadata->add_rowsets();
        rowset->set_overlapped(true);
        rowset->set_id(1);
        for (const auto& file : writer->segments()) {
            auto* segment_meta = rowset->add_segment_metas();
            segment_meta->set_filename(file.path);
            segment_meta->set_size(file.size.value());
        }
        writer->close();
    }

    // write tablet metadata
    _tablet_metadata->set_version(2);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    int invocation_counter = 0;
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("lake::TabletManager::update_segment_cache_size",
                                          [&](void* arg) { ++invocation_counter; });
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("lake::TabletManager::update_segment_cache_size");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    ASSERT_EQ(0, invocation_counter);

    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), _tablet_metadata, *_schema);
    ASSERT_OK(reader->prepare());
    TabletReaderParams params;
    ASSERT_OK(reader->open(params));

    auto read_chunk_ptr = ChunkFactory::new_chunk(*_schema, 1024);
    read_chunk_ptr->reset();
    ASSERT_OK(reader->get_next(read_chunk_ptr.get()));
    ASSERT_EQ(segment_rows, read_chunk_ptr->num_rows());
    LOG(INFO) << "[BatchUpdateMode=" << enable_batch_update
              << "] Segment init time cost: " << reader->stats().segment_init_ns << "ns";
    if (time_costs != nullptr) {
        *time_costs = reader->stats().segment_init_ns;
    }
    if (enable_batch_update) {
        ASSERT_EQ(2, invocation_counter); // 1 segment open + 1 batch update
    } else {
        ASSERT_EQ(1 + kColumnSize, invocation_counter);
    }

    read_chunk_ptr->reset();
    ASSERT_TRUE(reader->get_next(read_chunk_ptr.get()).is_end_of_file());
    reader->close();
}

// Perf comparison details (columns=10000): batch=false, time_costs=1334756936ns; batch=true, time_costs=32745137ns. Perf diff times: 40.8x
TEST_F(LakeDuplicateTablet10kColumnReaderTest, test_10k_column_read_perf) {
    size_t time_costs_false = 0;
    size_t time_costs_true = 0;
    test_10k_column_read_perf_body(false, &time_costs_false);
    test_10k_column_read_perf_body(true, &time_costs_true);
    LOG(WARNING) << "Perf comparison details (columns=" << kColumnSize
                 << "): batch=false, time_costs=" << time_costs_false
                 << "ns; batch=true, time_costs=" << time_costs_true << "ns. Perf diff times: " << std::setprecision(3)
                 << (double)time_costs_false / time_costs_true << "x";
}

StatusOr<std::pair<size_t, size_t>> LakeDuplicateTablet10kColumnReaderTest::testOneRoundGetColumnInit(
        const Chunk& chunk, size_t num_rows, size_t num_columns) {
    const int segment_rows = chunk.num_rows();
    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);
    // clear all historic rowsets
    _tablet_metadata->clear_rowsets();
    {
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
        RETURN_IF_ERROR(writer->open());
        RETURN_IF_ERROR(writer->write(chunk));
        RETURN_IF_ERROR(writer->finish());
        const auto& files = writer->segments();
        EXPECT_EQ(1, files.size());
        auto* rowset = _tablet_metadata->add_rowsets();
        rowset->set_overlapped(true);
        rowset->set_id(1);
        for (const auto& file : writer->segments()) {
            auto* segment_meta = rowset->add_segment_metas();
            segment_meta->set_filename(file.path);
            segment_meta->set_size(file.size.value());
        }
        writer->close();
    }

    _tablet_metadata->set_version(next_id());
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    int invocation_counter = 0;
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("lake::TabletManager::update_segment_cache_size",
                                          [&](void* arg) { ++invocation_counter; });
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("lake::TabletManager::update_segment_cache_size");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    EXPECT_EQ(0, invocation_counter);
    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), _tablet_metadata, *_schema);
    RETURN_IF_ERROR(reader->prepare());
    TabletReaderParams params;
    RETURN_IF_ERROR(reader->open(params));

    auto read_chunk_ptr = ChunkFactory::new_chunk(*_schema, 1024);
    read_chunk_ptr->reset();
    RETURN_IF_ERROR(reader->get_next(read_chunk_ptr.get()));
    EXPECT_EQ(segment_rows, read_chunk_ptr->num_rows());
    read_chunk_ptr->reset();
    EXPECT_TRUE(reader->get_next(read_chunk_ptr.get()).is_end_of_file());
    reader->close();
    return std::make_pair(reader->stats().segment_init_ns, invocation_counter);
}

// Perf comparison details(Columns=5000): batch=false, time_costs=268474431ns; batch=true, time_costs=13515903ns. Perf diff times: 19.9x
// This case takes time, disable it by default.
TEST_F(LakeDuplicateTablet10kColumnReaderTest, DISABLED_test_bench_5kcolumn_init) {
    constexpr size_t num_columns = 5000;
    constexpr size_t num_rows = 100;
    constexpr size_t round = 10;
    _tablet_metadata = generate_simple_tablet_metadata(DUP_KEYS, num_columns);
    _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
    _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    auto chunk = generate_chunk(_schema, num_rows, num_columns);

    size_t total_time_ns_false = 0;
    Segment::toggle_batch_update_cache_mode(false);
    {
        size_t total_invocations = 0;
        for (int i = 0; i < round; ++i) {
            auto result = testOneRoundGetColumnInit(chunk, num_rows, num_columns);
            ASSERT_OK(result.status());
            auto [time_cost, invocation_counter] = result.value();
            total_time_ns_false += time_cost;
            total_invocations += invocation_counter;
        }
        LOG(INFO) << "[BatchUpdateMode=false] Done running " << round
                  << " round, average time cost: " << total_time_ns_false / round
                  << "ns, average update cache invocation: " << total_invocations / round;
    }
    size_t total_time_ns_true = 0;
    Segment::toggle_batch_update_cache_mode(true);
    {
        size_t total_invocations = 0;
        for (int i = 0; i < round; ++i) {
            auto result = testOneRoundGetColumnInit(chunk, num_rows, num_columns);
            ASSERT_OK(result.status());
            auto [time_cost, invocation_counter] = result.value();
            total_time_ns_true += time_cost;
            total_invocations += invocation_counter;
        }
        LOG(INFO) << "[BatchUpdateMode=true] Done running " << round
                  << " round, average time cost: " << total_time_ns_true / round
                  << "ns, average update cache invocation: " << total_invocations / round;
    }
    LOG(WARNING) << "Perf comparison details(Columns=" << num_columns
                 << "): batch=false, time_costs=" << total_time_ns_false / round
                 << "ns; batch=true, time_costs=" << total_time_ns_true / round
                 << "ns. Perf diff times: " << std::setprecision(3) << (double)total_time_ns_false / total_time_ns_true
                 << "x";
}

// Test case for issue: PhysicalSplitMorselQueue crashes when tablet has no rowsets
// This test verifies that TabletReader::open correctly handles empty rowsets
// when need_split is true, preventing SIGSEGV in PhysicalSplitMorselQueue::_cur_rowset
TEST_F(LakeDuplicateTabletReaderTest, test_read_empty_tablet_with_split) {
    // Create a tablet metadata without any rowsets (empty tablet)
    // Do not add any rowsets to _tablet_metadata

    // write tablet metadata (version 2, but no rowsets)
    _tablet_metadata->set_version(2);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    // test reader with need_split = true
    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), _tablet_metadata, *_schema,
                                                 /*need_split=*/true, /*could_split_physically=*/true);
    ASSERT_OK(reader->prepare());

    TabletReaderParams params;
    params.splitted_scan_rows = 16384; // Set a non-zero value
    // This should not crash even with need_split=true and empty rowsets
    ASSERT_OK(reader->open(params));

    auto read_chunk_ptr = ChunkFactory::new_chunk(*_schema, 1024);
    read_chunk_ptr->reset();
    // Should return end of file immediately for empty tablet
    ASSERT_TRUE(reader->get_next(read_chunk_ptr.get()).is_end_of_file());

    reader->close();
}

// Verify that DeferOp in get_segment_iterators waits for all parallel tasks
// before returning on error, preventing use-after-free of captured references.
TEST_F(LakeDuplicateTabletReaderTest, test_parallel_read_error_waits_all_futures) {
    std::vector<int> k0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};
    std::vector<int> v0{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 41, 44};

    std::vector<int> k1{30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41};
    std::vector<int> v1{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};

    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    auto c2 = Int32Column::create();
    auto c3 = Int32Column::create();
    c0->append_numbers(k0.data(), k0.size() * sizeof(int));
    c1->append_numbers(v0.data(), v0.size() * sizeof(int));
    c2->append_numbers(k1.data(), k1.size() * sizeof(int));
    c3->append_numbers(v1.data(), v1.size() * sizeof(int));

    Chunk chunk0({std::move(c0), std::move(c1)}, _schema);
    Chunk chunk1({std::move(c2), std::move(c3)}, _schema);

    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);

    // Write rowset 1
    {
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
        ASSERT_OK(writer->open());
        ASSERT_OK(writer->write(chunk0));
        ASSERT_OK(writer->write(chunk1));
        ASSERT_OK(writer->finish());

        auto* rowset = _tablet_metadata->add_rowsets();
        rowset->set_overlapped(false);
        rowset->set_id(1);
        for (const auto& file : writer->segments()) {
            auto* segment_meta = rowset->add_segment_metas();
            segment_meta->set_filename(file.path);
            segment_meta->set_size(file.size.value());
        }
        writer->close();
    }

    // Write rowset 2
    {
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
        ASSERT_OK(writer->open());
        ASSERT_OK(writer->write(chunk0));
        ASSERT_OK(writer->write(chunk1));
        ASSERT_OK(writer->finish());

        auto* rowset = _tablet_metadata->add_rowsets();
        rowset->set_overlapped(false);
        rowset->set_id(2);
        for (const auto& file : writer->segments()) {
            auto* segment_meta = rowset->add_segment_metas();
            segment_meta->set_filename(file.path);
            segment_meta->set_size(file.size.value());
        }
        writer->close();
    }

    _tablet_metadata->set_version(3);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    ConfigResetGuard<bool> guard(&config::enable_load_segment_parallel, true);

    std::atomic<int> total_hits{0};

    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([] {
        SyncPoint::GetInstance()->ClearAllCallBacks();
        SyncPoint::GetInstance()->ClearTrace();
        SyncPoint::GetInstance()->DisableProcessing();
    });

    // First call injects error; subsequent calls succeed normally.
    // total_hits >= 2 proves DeferOp waited for all tasks before returning.
    SyncPoint::GetInstance()->SetCallBack("TabletReader::get_segment_iterators::parallel_read", [&](void* arg) {
        if (total_hits.fetch_add(1) == 0) {
            *static_cast<Status*>(arg) = Status::IOError("injected");
        }
    });

    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), _tablet_metadata, *_schema);
    ASSERT_OK(reader->prepare());
    TabletReaderParams params;
    auto st = reader->open(params);
    ASSERT_FALSE(st.ok());
    ASSERT_GE(total_hits.load(), 2);
}

// Regression: TabletReaderParams::has_predicate_above_iterator must reach SegmentReadOptions
// through the lake reader path (TabletReader::get_segment_iterators builds RowsetReadOptions,
// Rowset::read builds SegmentReadOptions). Before the fix, lake/tablet_reader.cpp dropped the
// flag when copying params into RowsetReadOptions, so the whole chain delivered the default
// (false) to SegmentIterator even when an above-iterator residual was present.
TEST_F(LakeDuplicateTabletReaderTest, test_propagate_has_predicate_above_iterator) {
    std::vector<int> k0{1, 2, 3, 4, 5};
    std::vector<int> v0{2, 4, 6, 8, 10};
    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    c0->append_numbers(k0.data(), k0.size() * sizeof(int));
    c1->append_numbers(v0.data(), v0.size() * sizeof(int));
    Chunk chunk0({std::move(c0), std::move(c1)}, _schema);

    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);
    {
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
        ASSERT_OK(writer->open());
        ASSERT_OK(writer->write(chunk0));
        ASSERT_OK(writer->finish());

        auto* rowset = _tablet_metadata->add_rowsets();
        rowset->set_overlapped(false);
        rowset->set_id(1);
        for (const auto& file : writer->segments()) {
            auto* segment_meta = rowset->add_segment_metas();
            segment_meta->set_filename(file.path);
            segment_meta->set_size(file.size.value());
        }
        writer->close();
    }
    _tablet_metadata->set_version(2);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    bool seen = false;
    bool propagated = false;
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("Rowset::read::seg_options", [&](void* arg) {
        auto* seg_options = static_cast<SegmentReadOptions*>(arg);
        seen = true;
        propagated = seg_options->has_predicate_above_iterator;
    });
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("Rowset::read::seg_options");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), _tablet_metadata, *_schema);
    ASSERT_OK(reader->prepare());
    TabletReaderParams params;
    params.has_predicate_above_iterator = true;
    ASSERT_OK(reader->open(params));

    auto read_chunk_ptr = ChunkFactory::new_chunk(*_schema, 1024);
    (void)reader->get_next(read_chunk_ptr.get());
    reader->close();

    ASSERT_TRUE(seen);
    EXPECT_TRUE(propagated);
}

// Drives the lake prepared-physical-split SEED path: with enable_prepared_physical_split_scan set,
// TabletReader::open runs build_prepared_tablet_read_state -> build_initial_coarse_split_tasks, and
// get_split_tasks returns INITIAL_COARSE seed tasks carrying the shared prepared read state -- rather
// than the force-split PhysicalSplitMorselQueue path taken when the flag is off.
TEST_F(LakeTabletReaderSpit, test_prepared_physical_split_seed) {
    std::vector<int> keys{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};
    std::vector<int> vals{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 42, 44};
    auto make_chunk = [&]() {
        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(keys.data(), keys.size() * sizeof(int));
        c1->append_numbers(vals.data(), vals.size() * sizeof(int));
        return Chunk({std::move(c0), std::move(c1)}, _schema);
    };

    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);
    {
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
        ASSERT_OK(writer->open());
        // Several segments so the tablet has well above splitted_scan_rows * lake_tablet_rows_splitted_ratio
        // rows and the split heuristic allows splitting.
        for (int i = 0; i < 4; i++) {
            auto chunk = make_chunk();
            ASSERT_OK(writer->write(chunk));
            ASSERT_OK(writer->finish());
        }
        auto* rowset = _tablet_metadata->add_rowsets();
        rowset->set_overlapped(true);
        rowset->set_id(1);
        rowset->set_num_rows(4 * keys.size());
        for (const auto& file : writer->segments()) {
            auto* segment_meta = rowset->add_segment_metas();
            segment_meta->set_filename(file.path);
            segment_meta->set_size(file.size.value());
        }
        writer->close();
    }
    _tablet_metadata->set_version(2);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), _tablet_metadata, *_schema, /*need_split=*/true,
                                                 /*could_split_physically=*/true);
    TInternalScanRange internal_scan_range;
    internal_scan_range.__set_tablet_id(_tablet_metadata->id());
    internal_scan_range.__set_version(std::to_string(_tablet_metadata->version()));
    TScanRange scan_range;
    scan_range.__set_internal_scan_range(internal_scan_range);
    auto params = generate_tablet_reader_params(&scan_range);
    params.enable_prepared_physical_split_scan = true; // take the prepared-split seed path

    ASSERT_OK(reader->prepare());
    ASSERT_OK(reader->open(params));

    // Profile counters: the seed builds prepared state for the single rowset. The per-segment prune
    // counters (lake_prepared_segments / scan_rows) are bumped later when a child refines, not on the
    // seed -- see the end-to-end test -- so they stay 0 here.
    EXPECT_EQ(1, reader->stats().lake_prepared_rowsets);
    EXPECT_EQ(0, reader->stats().lake_prepared_segments);

    std::vector<pipeline::ScanSplitContextPtr> split_tasks;
    reader->get_split_tasks(&split_tasks);
    ASSERT_GT(split_tasks.size(), 0);

    for (auto& task : split_tasks) {
        auto* ctx = dynamic_cast<pipeline::LakeSplitContext*>(task.get());
        ASSERT_NE(nullptr, ctx) << "prepared-split seed must produce LakeSplitContext tasks";
        EXPECT_EQ(pipeline::LakeSplitContext::RowidRangeSource::INITIAL_COARSE, ctx->rowid_range_source);
        EXPECT_NE(nullptr, ctx->prepared_tablet_read_state)
                << "seed task must carry the shared prepared tablet read state";
    }
    reader->close();
}

// subtract_sparse_ranges is the core range math behind the REFINED coverage set
// (pruned - already-allocated-coarse). Exercise each branch: empty / exact-cover / superset-cover /
// left-cut / right-cut / hole / disjoint / rhs-spanning-two-lhs / multiple-cuts.
TEST(SubtractSparseRangesTest, CoversAllBranches) {
    auto mk = [](std::initializer_list<std::pair<rowid_t, rowid_t>> parts) {
        SparseRange<> r;
        for (const auto& p : parts) {
            r.add(Range<>(p.first, p.second));
        }
        return r;
    };
    auto to_vec = [](const SparseRange<>& r) {
        std::vector<std::pair<rowid_t, rowid_t>> v;
        for (size_t i = 0; i < r.size(); ++i) {
            v.emplace_back(r[i].begin(), r[i].end());
        }
        return v;
    };
    using V = std::vector<std::pair<rowid_t, rowid_t>>;

    EXPECT_EQ((V{{0, 10}}), to_vec(subtract_sparse_ranges(mk({{0, 10}}), mk({}))));               // rhs empty
    EXPECT_TRUE(to_vec(subtract_sparse_ranges(mk({{0, 10}}), mk({{0, 10}}))).empty());            // exact cover
    EXPECT_TRUE(to_vec(subtract_sparse_ranges(mk({{2, 8}}), mk({{0, 10}}))).empty());             // superset cover
    EXPECT_EQ((V{{5, 10}}), to_vec(subtract_sparse_ranges(mk({{0, 10}}), mk({{0, 5}}))));         // left cut
    EXPECT_EQ((V{{0, 5}}), to_vec(subtract_sparse_ranges(mk({{0, 10}}), mk({{5, 10}}))));         // right cut
    EXPECT_EQ((V{{0, 3}, {7, 10}}), to_vec(subtract_sparse_ranges(mk({{0, 10}}), mk({{3, 7}})))); // hole in the middle
    EXPECT_EQ((V{{0, 10}}), to_vec(subtract_sparse_ranges(mk({{0, 10}}), mk({{20, 30}}))));       // disjoint
    EXPECT_EQ((V{{0, 2}, {18, 20}}),
              to_vec(subtract_sparse_ranges(mk({{0, 5}, {15, 20}}), mk({{2, 18}})))); // rhs spans 2 lhs ranges
    EXPECT_EQ((V{{0, 2}, {4, 6}, {8, 10}}),
              to_vec(subtract_sparse_ranges(mk({{0, 10}}), mk({{2, 4}, {6, 8}})))); // multiple cuts in one lhs
}

// PreparedSegmentReadState is the cross-child shared cache. Verify the publish / clear / disable
// truth table for both caches, and that the disabled flag is sticky: once a runtime-filter arrival
// disables the cache, a later re-publish must NOT silently re-enable it.
TEST(PreparedReadStateCacheTest, CacheStateMachine) {
    PreparedSegmentReadState s;
    EXPECT_FALSE(s.has_pruned_scan_range());
    EXPECT_FALSE(s.has_rowid_bounds_cache());

    // publish -> visible; clear -> hidden; re-publish after clear -> visible again.
    s.publish_pruned_scan_range(std::make_shared<SparseRange<>>(0, 10));
    EXPECT_TRUE(s.has_pruned_scan_range());
    s.clear_pruned_scan_range();
    EXPECT_FALSE(s.has_pruned_scan_range());
    s.publish_pruned_scan_range(std::make_shared<SparseRange<>>(0, 5));
    EXPECT_TRUE(s.has_pruned_scan_range());

    std::vector<std::optional<Range<rowid_t>>> bounds{Range<rowid_t>(0, 5)};
    s.publish_rowid_bounds_cache(std::move(bounds), Range<rowid_t>(0, 5));
    EXPECT_TRUE(s.has_rowid_bounds_cache());

    // disable_runtime_filter_dependent_cache disables BOTH caches and stays disabled after re-publish.
    s.disable_runtime_filter_dependent_cache();
    EXPECT_FALSE(s.has_pruned_scan_range());
    EXPECT_FALSE(s.has_rowid_bounds_cache());
    s.publish_pruned_scan_range(std::make_shared<SparseRange<>>(0, 3));
    EXPECT_FALSE(s.has_pruned_scan_range()) << "disabled cache must stay disabled after re-publish";
}

// Build a 1-rowset, 4-segment DUP_KEYS tablet (22 rows/segment) on |tablet|; returns rows/segment.
static int build_split_test_tablet(VersionedTablet& tablet, const std::shared_ptr<Schema>& schema,
                                   const std::shared_ptr<TabletMetadata>& metadata, const std::vector<int>& keys,
                                   const std::vector<int>& vals) {
    auto make_chunk = [&]() {
        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(keys.data(), keys.size() * sizeof(int));
        c1->append_numbers(vals.data(), vals.size() * sizeof(int));
        return Chunk({std::move(c0), std::move(c1)}, schema);
    };
    int64_t txn_id = next_id();
    auto writer_or = tablet.new_writer(kHorizontal, txn_id);
    CHECK(writer_or.ok());
    auto writer = std::move(writer_or.value());
    CHECK_OK(writer->open());
    for (int i = 0; i < 4; i++) {
        auto chunk = make_chunk();
        CHECK_OK(writer->write(chunk));
        CHECK_OK(writer->finish());
    }
    auto* rowset = metadata->add_rowsets();
    rowset->set_overlapped(true);
    rowset->set_id(1);
    rowset->set_num_rows(4 * keys.size());
    for (const auto& file : writer->segments()) {
        auto* segment_meta = rowset->add_segment_metas();
        segment_meta->set_filename(file.path);
        segment_meta->set_size(file.size.value());
    }
    writer->close();
    return static_cast<int>(keys.size());
}

// Gate OFF (default): the seed reader must NOT take the prepared-split path -- it falls back to the
// regular PhysicalSplitMorselQueue, so every split task is a REGULAR one with no prepared state.
TEST_F(LakeTabletReaderSpit, test_prepared_physical_split_gate_off) {
    std::vector<int> keys{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};
    std::vector<int> vals{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 42, 44};
    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);
    build_split_test_tablet(tablet, _schema, _tablet_metadata, keys, vals);
    _tablet_metadata->set_version(2);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), _tablet_metadata, *_schema,
                                                 /*need_split=*/true, /*could_split_physically=*/true);
    TInternalScanRange internal_scan_range;
    internal_scan_range.__set_tablet_id(_tablet_metadata->id());
    internal_scan_range.__set_version(std::to_string(_tablet_metadata->version()));
    TScanRange scan_range;
    scan_range.__set_internal_scan_range(internal_scan_range);
    auto params = generate_tablet_reader_params(&scan_range);
    // enable_prepared_physical_split_scan left at its default (false).

    ASSERT_OK(reader->prepare());
    ASSERT_OK(reader->open(params));

    std::vector<pipeline::ScanSplitContextPtr> split_tasks;
    reader->get_split_tasks(&split_tasks);
    ASSERT_GT(split_tasks.size(), 0);
    for (auto& task : split_tasks) {
        auto* ctx = dynamic_cast<pipeline::LakeSplitContext*>(task.get());
        ASSERT_NE(nullptr, ctx);
        EXPECT_EQ(pipeline::LakeSplitContext::RowidRangeSource::REGULAR, ctx->rowid_range_source);
        EXPECT_EQ(nullptr, ctx->prepared_tablet_read_state) << "gate off must not build prepared state";
        EXPECT_FALSE(ctx->is_prepared_physical_split());
    }
    // Gate off must leave every prepared-split profile counter untouched.
    EXPECT_EQ(0, reader->stats().lake_prepared_rowsets);
    EXPECT_EQ(0, reader->stats().lake_prepared_segments);
    reader->close();
}

// End-to-end correctness contract: the INITIAL_COARSE seed children (which read their coarse range
// and refine) plus the REFINED children they append must together reproduce EXACTLY the full-scan
// result -- no missing rows, no duplicates. This drives refine_initial_coarse_split_and_append_refined_tasks,
// subtract_sparse_ranges, get_segment_iterators / read_prepared_segment and the shared prepared state,
// mimicking how LakeDataSource (a follow-up PR) drives them.
TEST_F(LakeTabletReaderSpit, test_prepared_physical_split_end_to_end_equivalence) {
    std::vector<int> keys{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};
    std::vector<int> vals{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 42, 44};
    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);
    const int rows_per_segment = build_split_test_tablet(tablet, _schema, _tablet_metadata, keys, vals);
    _tablet_metadata->set_version(2);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    using Row = std::pair<int, int>;
    auto collect = [&](TabletReader* r, std::multiset<Row>* out) {
        auto chunk = ChunkFactory::new_chunk(*_schema, 1024);
        while (true) {
            chunk->reset();
            auto st = r->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            ASSERT_OK(st);
            for (size_t i = 0; i < chunk->num_rows(); ++i) {
                out->emplace(chunk->get(i)[0].get_int32(), chunk->get(i)[1].get_int32());
            }
        }
    };

    // Baseline: a plain, non-split full scan.
    std::multiset<Row> expected;
    {
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), _tablet_metadata, *_schema);
        ASSERT_OK(reader->prepare());
        TabletReaderParams params;
        ASSERT_OK(reader->open(params));
        collect(reader.get(), &expected);
        reader->close();
    }
    ASSERT_EQ(static_cast<size_t>(4 * rows_per_segment), expected.size());

    TInternalScanRange internal_scan_range;
    internal_scan_range.__set_tablet_id(_tablet_metadata->id());
    internal_scan_range.__set_version(std::to_string(_tablet_metadata->version()));
    TScanRange scan_range;
    scan_range.__set_internal_scan_range(internal_scan_range);

    // Seed: produces INITIAL_COARSE tasks carrying the shared prepared state.
    std::vector<pipeline::ScanSplitContextPtr> queue;
    {
        auto seed = std::make_shared<TabletReader>(_tablet_mgr.get(), _tablet_metadata, *_schema,
                                                   /*need_split=*/true, /*could_split_physically=*/true);
        auto params = generate_tablet_reader_params(&scan_range);
        params.enable_prepared_physical_split_scan = true;
        ASSERT_OK(seed->prepare());
        ASSERT_OK(seed->open(params));
        seed->get_split_tasks(&queue);
        seed->close();
    }
    ASSERT_GT(queue.size(), 0u);

    // Drive the children the way the connector does: an INITIAL_COARSE child reads its coarse range and
    // refines (appending REFINED tasks); a REFINED child just reads its range.
    std::multiset<Row> actual;
    int64_t refined_segments = 0;
    int64_t refined_scan_rows = 0;
    int guard = 0;
    for (size_t head = 0; head < queue.size(); ++head) {
        ASSERT_LT(guard++, 10000) << "runaway split loop";
        auto* ctx = dynamic_cast<pipeline::LakeSplitContext*>(queue[head].get());
        ASSERT_NE(nullptr, ctx);
        ASSERT_TRUE(ctx->is_prepared_physical_split());
        const bool is_initial = ctx->rowid_range_source == pipeline::LakeSplitContext::RowidRangeSource::INITIAL_COARSE;

        auto child = std::make_shared<TabletReader>(_tablet_mgr.get(), _tablet_metadata, *_schema,
                                                    /*need_split=*/false, /*could_split_physically=*/true);
        auto params = generate_tablet_reader_params(&scan_range);
        params.rowid_range_option = ctx->rowid_range;
        params.prepared_tablet_read_state = ctx->prepared_tablet_read_state;
        params.prepared_segment_read_state = ctx->prepared_segment_read_state;
        params.prepared_rowset_index = ctx->rowset_index;
        params.prepared_segment_index = ctx->segment_index;
        params.refine_initial_coarse_split_and_append_refined_tasks = is_initial;

        ASSERT_OK(child->prepare());
        ASSERT_OK(child->open(params));
        collect(child.get(), &actual);
        // Only the INITIAL_COARSE children refine; each bumps the per-segment prune counters once for the
        // single segment it owns. Accumulate across children to confirm the counters track refine.
        refined_segments += child->stats().lake_prepared_segments;
        refined_scan_rows += child->stats().lake_prepared_scan_rows;

        std::vector<pipeline::ScanSplitContextPtr> refined;
        child->get_split_tasks(&refined);
        for (auto& t : refined) {
            queue.emplace_back(std::move(t));
        }
        child->close();
    }

    EXPECT_EQ(expected, actual) << "prepared-split children must reproduce the full scan exactly";

    // Each of the 4 segments is refined exactly once; with no predicates the pruned range keeps every row,
    // so the per-segment scan-row counter sums to the full row count.
    EXPECT_EQ(4, refined_segments);
    EXPECT_EQ(static_cast<int64_t>(expected.size()), refined_scan_rows);
}

// parse_seek_range() is parsed once and cached in _cached_seek_ranges, then reused on every subsequent
// init_rowset_read_options (a reader's per-split reopens re-run it). Prove the second call reuses the
// cache instead of re-parsing: open() populates the cache, then we overwrite it with a size-1 sentinel;
// a reused cache yields the size-1 sentinel, while a re-parse of these (empty-key) params would yield an
// empty range list and overwrite the sentinel.
TEST_F(LakeTabletReaderSpit, test_seek_range_cached_across_reopen) {
    std::vector<int> keys{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    std::vector<int> vals{2, 4, 6, 8, 10, 12, 14, 16, 18, 20};
    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);
    build_split_test_tablet(tablet, _schema, _tablet_metadata, keys, vals);
    _tablet_metadata->set_version(2);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    TInternalScanRange internal_scan_range;
    internal_scan_range.__set_tablet_id(_tablet_metadata->id());
    internal_scan_range.__set_version(std::to_string(_tablet_metadata->version()));
    TScanRange scan_range;
    scan_range.__set_internal_scan_range(internal_scan_range);

    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), _tablet_metadata, *_schema);
    ASSERT_OK(reader->prepare());
    auto params = generate_tablet_reader_params(&scan_range);

    // open() runs init_rowset_read_options once, which parses the seek range and engages the cache.
    ASSERT_FALSE(reader->_cached_seek_ranges.has_value());
    ASSERT_OK(reader->open(params));
    ASSERT_TRUE(reader->_cached_seek_ranges.has_value());

    // Overwrite the cache with a size-1 sentinel; the reader is fully set up now, so a second
    // init_rowset_read_options is safe. A reused cache yields size 1; a re-parse of the empty-key params
    // would yield size 0 and overwrite the sentinel.
    reader->_cached_seek_ranges = std::vector<SeekRange>(1);
    RowsetReadOptions opts;
    ASSERT_OK(reader->init_rowset_read_options(params, &opts));
    EXPECT_EQ(1u, opts.ranges.size()) << "init_rowset_read_options re-parsed instead of reusing the cache";
    ASSERT_TRUE(reader->_cached_seek_ranges.has_value());
    EXPECT_EQ(1u, reader->_cached_seek_ranges->size());

    reader->close();
}

} // namespace starrocks::lake
