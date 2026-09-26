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

// A multi-statement transaction (SQL BEGIN ... COMMIT, i.e. a TxnInfoPB carrying one load_id per
// statement) is published by merging every statement's op_write for a tablet into one rowset. With
// file bundling on, the load gives each statement a bundle file shared by the tablets of a
// partition, but a tablet's segment goes into it only when the statement wrote that tablet in one
// flush at the end of the load; a statement that flushed more than once writes standalone files, and
// so does the publish when it rewrites the segment of a row-mode partial update. One rowset cannot
// hold both kinds (normalize_rowset_before_save() refuses it), so a small statement next to a large
// one failed the publish on every retry, and a partial update next to a small statement dropped the
// bundle offsets of every segment, which then read another tablet's bytes. The tests drive the real
// path: DeltaWriter -> publish_version with the statements' load_ids -> read back, then rebuild the
// primary index from what was persisted.

#include <gtest/gtest.h>
#include <unistd.h>

#include <ctime>
#include <map>
#include <numeric>
#include <optional>
#include <vector>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "common/config_ingest_fwd.h"
#include "fs/bundle_file.h"
#include "gutil/strings/join.h"
#include "storage/chunk_helper.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/test_util.h"
#include "storage/lake/update_manager.h"
#include "storage/tablet_schema.h"

namespace starrocks::lake {

class LakeMergedBundledSegmentsTest : public TestBase {
public:
    LakeMergedBundledSegmentsTest() : TestBase(kTestGroupPath) {
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | NO  |  NO  |
        //  |   c2   |  INT | NO  |  NO  |
        _tablet_metadata = std::make_shared<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);
        _tablet_metadata->set_next_rowset_id(1);
        _tablet_metadata->set_enable_persistent_index(true);
        _tablet_metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
        auto* schema = _tablet_metadata->mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(PRIMARY_KEYS);
        schema->set_num_rows_per_row_block(65535);
        for (int i = 0; i < 3; i++) {
            auto* column = schema->add_column();
            column->set_unique_id(next_id());
            column->set_name("c" + std::to_string(i));
            column->set_type("INT");
            column->set_is_key(i == 0);
            column->set_is_nullable(false);
            if (i > 0) {
                column->set_aggregation("REPLACE");
            }
        }
        _slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
        _slots.emplace_back(1, "c1", TypeDescriptor{LogicalType::TYPE_INT});
        _c1_slots.emplace_back(&_slots[0]);
        _c1_slots.emplace_back(&_slots[1]);
        _tablet_schema = TabletSchema::create(*schema);
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        clear_and_init_test_dir();
        // Two tablets of one partition, so that a statement's bundle file holds both of their
        // segments and one of them sits at a non-zero offset.
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
        CHECK_OK(_tablet_mgr->create_schema_file(_tablet_metadata->id(), _tablet_metadata->schema()));
        auto other = std::make_shared<TabletMetadata>(*_tablet_metadata);
        other->set_id(next_id());
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*other));
        CHECK_OK(_tablet_mgr->create_schema_file(other->id(), other->schema()));
        _tablets = {_tablet_metadata->id(), other->id()};
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

protected:
    // A row as the reader returns it: (c1, c2).
    using Row = std::pair<int, int>;
    using Rows = std::map<int, Row>;

    // Full rows of a statement written with `factor` hold c1 = key * factor and c2 = key * factor + 1;
    // a partial update of c1 with `factor` writes c1 = key * factor only.
    enum class Write { kFullRow, kC1 };

    static PUniqueId statement_load_id(int64_t lo) {
        PUniqueId id;
        id.set_hi(1);
        id.set_lo(lo);
        return id;
    }

    static std::vector<int> key_range(int begin, int end) {
        std::vector<int> keys(end - begin);
        std::iota(keys.begin(), keys.end(), begin);
        return keys;
    }

    static void add_rows(Rows* rows, const std::vector<int>& keys, int factor) {
        for (int key : keys) {
            (*rows)[key] = Row{key * factor, key * factor + 1};
        }
    }

    // Keys of a tablet are offset by its position in _tablets, so one tablet reading the other's
    // segment shows.
    static std::vector<int> tablet_keys(size_t tablet_idx, const std::vector<int>& keys) {
        std::vector<int> result;
        result.reserve(keys.size());
        for (int key : keys) {
            result.push_back(key + static_cast<int>(tablet_idx) * 1000);
        }
        return result;
    }

    StatusOr<std::unique_ptr<DeltaWriter>> new_writer(int64_t tablet_id, int64_t txn_id, const PUniqueId* load_id,
                                                      Write kind, BundleWritableFileContext* bundle) {
        DeltaWriterBuilder builder;
        builder.set_tablet_manager(_tablet_mgr.get())
                .set_tablet_id(tablet_id)
                .set_txn_id(txn_id)
                .set_partition_id(_partition_id)
                .set_mem_tracker(_mem_tracker.get())
                .set_schema_id(_tablet_schema->id())
                .set_bundle_writable_file_context(bundle);
        if (kind == Write::kC1) {
            builder.set_slot_descriptors(&_c1_slots).set_partial_update_mode(PartialUpdateMode::ROW_MODE);
        }
        if (load_id != nullptr) {
            // A statement of a multi-statement transaction writes its own txn log, keyed by load_id.
            builder.set_load_id(*load_id).set_is_multi_statements_txn(true);
        }
        return builder.build();
    }

    void write_chunk(DeltaWriter* writer, Write kind, const std::vector<int>& keys, int factor) {
        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        auto c2 = Int32Column::create();
        for (int key : keys) {
            c0->append(key);
            c1->append(key * factor);
            c2->append(key * factor + 1);
        }
        Columns columns;
        Chunk::SlotHashMap slot_map;
        columns.emplace_back(std::move(c0));
        slot_map[0] = 0;
        columns.emplace_back(std::move(c1));
        slot_map[1] = 1;
        if (kind == Write::kFullRow) {
            columns.emplace_back(std::move(c2));
            slot_map[2] = 2;
        }
        Chunk chunk(std::move(columns), slot_map);
        std::vector<uint32_t> indexes(keys.size());
        std::iota(indexes.begin(), indexes.end(), 0);
        ASSERT_OK(writer->write(chunk, indexes.data(), indexes.size()));
    }

    // One statement over every tablet, written the way a load channel writes it: one bundle file
    // context for the partition, every writer open before any of them finishes. `flushes` holds the
    // keys of each flush: with a single one, the tablet's only segment is written at the end of the
    // load and goes into the bundle file; with more, write_buffer_size is 1 so every write() flushes,
    // and those segments are standalone files.
    void write_statement(int64_t txn_id, const PUniqueId& load_id, Write kind,
                         const std::vector<std::vector<int>>& flushes, int factor) {
        std::optional<ConfigResetGuard<int64_t>> one_segment_per_write;
        if (flushes.size() > 1) {
            one_segment_per_write.emplace(&config::write_buffer_size, 1);
        }
        auto bundle = std::make_unique<BundleWritableFileContext>();
        std::vector<std::unique_ptr<DeltaWriter>> writers;
        for (int64_t tablet_id : _tablets) {
            ASSIGN_OR_ABORT(auto writer, new_writer(tablet_id, txn_id, &load_id, kind, bundle.get()));
            ASSERT_OK(writer->open());
            writers.emplace_back(std::move(writer));
        }
        for (size_t i = 0; i < _tablets.size(); i++) {
            for (const auto& keys : flushes) {
                ASSERT_NO_FATAL_FAILURE(write_chunk(writers[i].get(), kind, tablet_keys(i, keys), factor));
            }
            ASSERT_OK(writers[i]->finish_with_txnlog());
            writers[i]->close();
        }
    }

    // An ordinary load of full rows of `keys` into `tablet_id`, published as `version`.
    void load(int64_t tablet_id, int64_t version, const std::vector<int>& keys, int factor, bool rebuild_pindex) {
        const int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto writer, new_writer(tablet_id, txn_id, nullptr, Write::kFullRow, nullptr));
        ASSERT_OK(writer->open());
        ASSERT_NO_FATAL_FAILURE(write_chunk(writer.get(), Write::kFullRow, keys, factor));
        ASSERT_OK(writer->finish_with_txnlog());
        writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version, txn_id, rebuild_pindex).status());
    }

    // The statement's txn log for `tablet_id`: `segments` segments, all bundled or all standalone.
    // Adds the bundle offsets to `offsets`, by segment file name.
    void expect_statement_log(int64_t tablet_id, int64_t txn_id, const PUniqueId& load_id, int segments, bool bundled,
                              std::map<std::string, int64_t>* offsets) {
        ASSIGN_OR_ABORT(auto log, _tablet_mgr->get_txn_log(tablet_id, txn_id, load_id));
        const auto& rowset = log->op_write().rowset();
        ASSERT_EQ(segments, rowset.segment_metas_size());
        for (const auto& segment : rowset.segment_metas()) {
            ASSERT_EQ(bundled, segment.has_bundle_file_offset()) << segment.filename();
            if (segment.has_bundle_file_offset()) {
                (*offsets)[segment.filename()] = segment.bundle_file_offset();
            }
        }
    }

    // The rowsets `version` published into `tablet_id` hold `segments[i]` segments each, the ones of
    // `bundled_rowsets` in the bundle files at the offsets their statements wrote them at, the others
    // as standalone files.
    void expect_rowsets(int64_t tablet_id, int64_t version, const std::vector<int>& segments,
                        const std::vector<bool>& bundled_rowsets, const std::map<std::string, int64_t>& offsets) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        std::vector<const RowsetMetadataPB*> published;
        for (const auto& rowset : metadata->rowsets()) {
            if (rowset.version() == version) {
                published.push_back(&rowset);
            }
        }
        ASSERT_EQ(segments.size(), published.size());
        for (size_t i = 0; i < published.size(); i++) {
            ASSERT_EQ(segments[i], published[i]->segment_metas_size()) << "rowset " << i;
            for (const auto& segment : published[i]->segment_metas()) {
                if (!bundled_rowsets[i]) {
                    EXPECT_FALSE(segment.has_bundle_file_offset()) << segment.filename();
                    continue;
                }
                auto it = offsets.find(segment.filename());
                ASSERT_NE(offsets.end(), it) << segment.filename();
                ASSERT_TRUE(segment.has_bundle_file_offset()) << "lost the bundle offset of " << segment.filename();
                EXPECT_EQ(it->second, segment.bundle_file_offset()) << segment.filename();
            }
        }
    }

    StatusOr<TabletMetadataPtr> publish_statements(int64_t tablet_id, int64_t new_version, int64_t txn_id,
                                                   const std::vector<PUniqueId>& load_ids) {
        auto txn_info = TEST_txn_info(txn_id, time(nullptr));
        for (const auto& load_id : load_ids) {
            txn_info.add_load_ids()->CopyFrom(load_id);
        }
        std::vector<TxnInfoPB> txns{std::move(txn_info)};
        return publish_version(_tablet_mgr.get(), PublishTabletInfo(tablet_id), new_version - 1, new_version, txns,
                               false);
    }

    // `tablet_id` at `version` holds exactly `expected`, one live row per key.
    void expect_rows(int64_t tablet_id, int64_t version, const Rows& expected) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        ASSERT_OK(reader->prepare());
        ASSERT_OK(reader->open(TabletReaderParams()));
        Rows actual;
        std::vector<int> duplicates;
        auto chunk = ChunkFactory::new_chunk(*_schema, 128);
        while (true) {
            auto st = reader->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            ASSERT_OK(st);
            for (size_t i = 0; i < chunk->num_rows(); i++) {
                const int key = chunk->get_column_by_index(0)->get(i).get_int32();
                const Row row{chunk->get_column_by_index(1)->get(i).get_int32(),
                              chunk->get_column_by_index(2)->get(i).get_int32()};
                if (!actual.emplace(key, row).second) {
                    duplicates.push_back(key);
                }
            }
            chunk->reset();
        }
        EXPECT_TRUE(duplicates.empty()) << "duplicate primary keys: " << JoinInts(duplicates, ",");
        EXPECT_EQ(expected, actual);
    }

    inline static const std::string kTestGroupPath = "test_lake_merged_bundled_segments_" + std::to_string(getpid());

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    std::vector<SlotDescriptor> _slots;
    std::vector<SlotDescriptor*> _c1_slots;
    std::vector<int64_t> _tablets;
    int64_t _partition_id = next_id();
};

// BEGIN; INSERT a few rows; INSERT enough rows to flush twice; INSERT a few rows; COMMIT. The first
// and last statements bundle their segment, the middle one does not, so the publish splits the
// merged rowset into three consecutive rowsets. The later statements overwrite some keys of the
// earlier ones, so the delete vectors of the first two rowsets are only right if every rssid is
// still what the primary index was given during the publish.
TEST_F(LakeMergedBundledSegmentsTest, small_large_small_statements) {
    const int64_t txn_id = next_id();
    const auto stmt1 = statement_load_id(1);
    const auto stmt2 = statement_load_id(2);
    const auto stmt3 = statement_load_id(3);
    ASSERT_NO_FATAL_FAILURE(write_statement(txn_id, stmt1, Write::kFullRow, {key_range(0, 10)}, 10));
    ASSERT_NO_FATAL_FAILURE(write_statement(txn_id, stmt2, Write::kFullRow, {key_range(5, 15), key_range(15, 20)}, 20));
    ASSERT_NO_FATAL_FAILURE(write_statement(txn_id, stmt3, Write::kFullRow, {key_range(18, 25)}, 30));

    bool any_non_zero_offset = false;
    for (size_t t = 0; t < _tablets.size(); t++) {
        const int64_t tablet_id = _tablets[t];
        std::map<std::string, int64_t> offsets;
        ASSERT_NO_FATAL_FAILURE(expect_statement_log(tablet_id, txn_id, stmt1, 1, /*bundled=*/true, &offsets));
        ASSERT_NO_FATAL_FAILURE(expect_statement_log(tablet_id, txn_id, stmt2, 2, /*bundled=*/false, &offsets));
        ASSERT_NO_FATAL_FAILURE(expect_statement_log(tablet_id, txn_id, stmt3, 1, /*bundled=*/true, &offsets));
        for (const auto& [_, offset] : offsets) {
            any_non_zero_offset = any_non_zero_offset || offset > 0;
        }

        ASSIGN_OR_ABORT(auto base, _tablet_mgr->get_tablet_metadata(tablet_id, 1));
        const uint32_t rowset_id = base->next_rowset_id();
        ASSERT_OK(publish_statements(tablet_id, 2, txn_id, {stmt1, stmt2, stmt3}).status());
        ASSERT_NO_FATAL_FAILURE(expect_rowsets(tablet_id, 2, {1, 2, 1}, {true, false, true}, offsets));
        // Statement 1 takes rssid slot 0, statement 2 slots 1 and 2, statement 3 slot 3.
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, 2));
        EXPECT_EQ(rowset_id, metadata->rowsets(0).id());
        EXPECT_EQ(rowset_id + 1, metadata->rowsets(1).id());
        EXPECT_EQ(rowset_id + 3, metadata->rowsets(2).id());
        EXPECT_EQ(rowset_id + 4, metadata->next_rowset_id());

        Rows expected;
        add_rows(&expected, tablet_keys(t, key_range(0, 5)), 10);
        add_rows(&expected, tablet_keys(t, key_range(5, 18)), 20);
        add_rows(&expected, tablet_keys(t, key_range(18, 25)), 30);
        expect_rows(tablet_id, 2, expected);
    }
    EXPECT_TRUE(any_non_zero_offset) << "the tablets did not share the statements' bundle files";

    // Write every key of the first tablet again, through the primary index the publish left in
    // memory, then through one rebuilt from what was persisted (what a CN restart does). An index
    // entry naming another row than its key's would leave the old row live next to the new one.
    const int64_t tablet_id = _tablets[0];
    ASSERT_NO_FATAL_FAILURE(load(tablet_id, 3, key_range(0, 25), 40, /*rebuild_pindex=*/false));
    Rows expected;
    add_rows(&expected, key_range(0, 25), 40);
    expect_rows(tablet_id, 3, expected);
    ASSERT_NO_FATAL_FAILURE(load(tablet_id, 4, key_range(0, 25), 50, /*rebuild_pindex=*/true));
    add_rows(&expected, key_range(0, 25), 50);
    expect_rows(tablet_id, 4, expected);
}

// BEGIN; UPDATE c1 of some rows (a row-mode partial update); INSERT a few rows; COMMIT. Both
// statements bundle their segment, and the publish rewrites the partial update's segment into a
// standalone file. The INSERT's segment has to keep its place in its bundle file.
TEST_F(LakeMergedBundledSegmentsTest, partial_update_next_to_a_bundled_statement) {
    const auto updated = key_range(0, 12);
    const auto inserted = key_range(12, 24);
    for (size_t t = 0; t < _tablets.size(); t++) {
        ASSERT_NO_FATAL_FAILURE(load(_tablets[t], 2, tablet_keys(t, updated), 10, /*rebuild_pindex=*/false));
    }

    const int64_t txn_id = next_id();
    const auto stmt1 = statement_load_id(1);
    const auto stmt2 = statement_load_id(2);
    ASSERT_NO_FATAL_FAILURE(write_statement(txn_id, stmt1, Write::kC1, {updated}, 20));
    ASSERT_NO_FATAL_FAILURE(write_statement(txn_id, stmt2, Write::kFullRow, {inserted}, 30));

    bool any_non_zero_offset = false;
    for (size_t t = 0; t < _tablets.size(); t++) {
        const int64_t tablet_id = _tablets[t];
        std::map<std::string, int64_t> offsets;
        ASSERT_NO_FATAL_FAILURE(expect_statement_log(tablet_id, txn_id, stmt2, 1, /*bundled=*/true, &offsets));
        for (const auto& [_, offset] : offsets) {
            any_non_zero_offset = any_non_zero_offset || offset > 0;
        }

        ASSERT_OK(publish_statements(tablet_id, 3, txn_id, {stmt1, stmt2}).status());
        ASSERT_NO_FATAL_FAILURE(expect_rowsets(tablet_id, 3, {1, 1}, {false, true}, offsets));

        Rows expected;
        for (int key : tablet_keys(t, updated)) {
            expected[key] = Row{key * 20, key * 10 + 1};
        }
        add_rows(&expected, tablet_keys(t, inserted), 30);
        expect_rows(tablet_id, 3, expected);
    }
    EXPECT_TRUE(any_non_zero_offset) << "the tablets did not share the INSERT's bundle file";
}

} // namespace starrocks::lake
