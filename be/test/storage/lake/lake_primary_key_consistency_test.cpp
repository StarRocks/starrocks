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

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <cassert>
#include <cstdlib>
#include <random>

#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/lake/compaction_task.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/metacache.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/test_util.h"
#include "storage/rowset/segment_iterator.h"
#include "storage/rowset/segment_options.h"
#include "storage/tablet_schema.h"
#include "testutil/deterministic_test_utils.h"

namespace starrocks::lake {

enum PICT_OP {
    UPSERT = 0,
    DELETE = 1,
    GET = 2,
    COMPACT = 3,
    RELOAD = 4,
    MAX = 5,
};

static const std::string kTestGroupPath = "./test_lake_primary_key_consistency";
static const int64_t MaxNumber = 1000000;
static const int64_t MaxN = 10000;
static const size_t MaxUpsert = 4;

class Replayer {
public:
    void upsert(const ChunkPtr& chunk) {
        for (int i = 0; i < chunk->num_rows(); i++) {
            _replayer_index[chunk->columns()[0]->get(i).get_int32()] = chunk->columns()[1]->get(i).get_int32();
        }
    }

    void erase(const ChunkPtr& chunk) {
        for (int i = 0; i < chunk->num_rows(); i++) {
            _replayer_index.erase(chunk->columns()[0]->get(i).get_int32());
        }
    }

    bool check(const ChunkPtr& chunk) {
        std::map<int, int> tmp_chunk;
        for (int i = 0; i < chunk->num_rows(); i++) {
            if (tmp_chunk.count(chunk->columns()[0]->get(i).get_int32()) > 0) {
                // duplicate pk
                LOG(ERROR) << "duplicate pk: " << chunk->columns()[0]->get(i).get_int32();
                return false;
            }
            tmp_chunk[chunk->columns()[0]->get(i).get_int32()] = chunk->columns()[1]->get(i).get_int32();
        }
        if (tmp_chunk.size() != _replayer_index.size()) {
            LOG(ERROR) << "inconsistency row number, actual : " << tmp_chunk.size()
                       << " expected : " << _replayer_index.size();
            return false;
        }
        for (const auto& each : _replayer_index) {
            tmp_chunk.erase(each.first);
        }
        if (!tmp_chunk.empty()) {
            LOG(ERROR) << "inconsistency result.";
            return false;
        }
        return true;
    }

private:
    std::map<int, int> _replayer_index;
};

class LakePrimaryKeyConsistencyTest : public TestBase, testing::WithParamInterface<PrimaryKeyParam> {
public:
    LakePrimaryKeyConsistencyTest() : TestBase(kTestGroupPath) {
        _tablet_metadata = generate_simple_tablet_metadata(PRIMARY_KEYS);
        _tablet_metadata->set_enable_persistent_index(true);
        _tablet_metadata->set_persistent_index_type(GetParam().persistent_index_type);

        _slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
        _slots.emplace_back(1, "c1", TypeDescriptor{LogicalType::TYPE_INT});
        _slots.emplace_back(2, "__op", TypeDescriptor{LogicalType::TYPE_INT});
        _slot_pointers.emplace_back(&_slots[0]);
        _slot_pointers.emplace_back(&_slots[1]);
        _slot_pointers.emplace_back(&_slots[2]);

        _slot_cid_map.emplace(0, 0);
        _slot_cid_map.emplace(1, 1);
        _slot_cid_map.emplace(2, 2);

        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
        _version = 1;

        _random_generator = std::make_unique<DeterRandomGenerator<int, PICT_OP>>(MaxNumber, MaxN, _seed, 0, INT64_MAX);
        std::vector<WeightedItem<PICT_OP>> items;
        items.emplace_back(PICT_OP::UPSERT, 60);
        items.emplace_back(PICT_OP::DELETE, 15);
        items.emplace_back(PICT_OP::GET, 5);
        items.emplace_back(PICT_OP::COMPACT, 10);
        items.emplace_back(PICT_OP::RELOAD, 10);
        _random_op_selector = std::make_unique<WeightedRandomOpSelector<int, PICT_OP>>(_random_generator.get(), items);
        _replayer = std::make_unique<Replayer>();
    }

    void SetUp() override {
        (void)fs::remove_all(kTestGroupPath);
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kTxnLogDirectoryName)));
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
        _old_l0_size = config::l0_max_mem_usage;
        config::l0_max_mem_usage = MaxNumber * (sizeof(int) + sizeof(uint64_t) * 2) / 10;
        _old_memtable_size = config::write_buffer_size;
        config::write_buffer_size = MaxN * (sizeof(int) * 2) / MaxUpsert;
        config::lake_publish_version_slow_log_ms = 0;
        config::enable_pindex_minor_compaction = false;
    }

    void TearDown() override {
        (void)fs::remove_all(kTestGroupPath);
        config::l0_max_mem_usage = _old_l0_size;
        config::write_buffer_size = _old_memtable_size;
    }

    std::pair<ChunkPtr, std::vector<uint32_t>> gen_upsert_data(bool is_upsert) {
        const size_t chunk_size = (size_t)_random_generator->random_n();
        std::vector<int> v0(chunk_size);
        std::vector<int> v1(chunk_size);
        std::vector<uint8_t> v2(chunk_size, is_upsert ? TOpType::UPSERT : TOpType::DELETE);
        _random_generator->random_cols(chunk_size, &v0, &v1);

        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        auto c2 = Int8Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));
        c2->append_numbers(v2.data(), v2.size() * sizeof(uint8_t));
        auto indexes = std::vector<uint32_t>(chunk_size);
        for (uint32_t i = 0; i < chunk_size; i++) {
            indexes[i] = i;
        }
        return {std::make_shared<Chunk>(Columns{c0, c1, c2}, _slot_cid_map), std::move(indexes)};
    }

    ChunkPtr read(int64_t tablet_id, int64_t version) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        auto ret = ChunkHelper::new_chunk(*_schema, 128);
        while (true) {
            auto tmp = ChunkHelper::new_chunk(*_schema, 128);
            auto st = reader->get_next(tmp.get());
            if (st.is_end_of_file()) {
                break;
            }
            CHECK_OK(st);
            ret->append(*tmp);
        }
        return ret;
    }

    Status upsert_op() {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(_tablet_metadata->id())
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .build());
        RETURN_IF_ERROR(delta_writer->open());
        size_t upsert_size = _random_generator->random() % MaxUpsert;
        for (int i = 0; i < upsert_size; i++) {
            auto chunk_index = gen_upsert_data(true);
            RETURN_IF_ERROR(
                    delta_writer->write(*(chunk_index.first), chunk_index.second.data(), chunk_index.second.size()));
            _replayer->upsert(chunk_index.first);
        }
        RETURN_IF_ERROR(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        RETURN_IF_ERROR(publish_single_version(_tablet_metadata->id(), _version + 1, txn_id));
        _version++;
        return Status::OK();
    }

    Status delete_op() {
        auto chunk_index = gen_upsert_data(false);
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(_tablet_metadata->id())
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .build());
        RETURN_IF_ERROR(delta_writer->open());
        RETURN_IF_ERROR(
                delta_writer->write(*(chunk_index.first), chunk_index.second.data(), chunk_index.second.size()));
        RETURN_IF_ERROR(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        RETURN_IF_ERROR(publish_single_version(_tablet_metadata->id(), _version + 1, txn_id));
        _version++;
        _replayer->erase(chunk_index.first);
        return Status::OK();
    }

    Status compact_op() {
        auto txn_id = next_id();
        auto task_context =
                std::make_unique<CompactionTaskContext>(txn_id, _tablet_metadata->id(), _version, false, nullptr);
        ASSIGN_OR_RETURN(auto task, _tablet_mgr->compact(task_context.get()));
        RETURN_IF_ERROR(task->execute(CompactionTask::kNoCancelFn));
        RETURN_IF_ERROR(publish_single_version(_tablet_metadata->id(), _version + 1, txn_id));
        _version++;
        return Status::OK();
    }

    Status reload_op() {
        if (!_update_mgr->try_remove_primary_index_cache(_tablet_metadata->id())) {
            return Status::InternalError("try_remove_primary_index_cache");
        }
        return Status::OK();
    }

    Status get_op() {
        auto chunk = read(_tablet_metadata->id(), _version);
        if (!_replayer->check(chunk)) {
            LOG(FATAL) << "consistency check fail! seed : " << _seed;
        }
        return Status::OK();
    }

    Status run_random_tests() {
        size_t start_second = time(nullptr);
        while (start_second + _run_second >= time(nullptr)) {
            auto op = _random_op_selector->select();
            switch (op) {
            case UPSERT:
                RETURN_IF_ERROR(upsert_op());
                break;
            case DELETE:
                RETURN_IF_ERROR(delete_op());
                break;
            case GET:
                RETURN_IF_ERROR(get_op());
                break;
            case COMPACT:
                RETURN_IF_ERROR(compact_op());
                break;
            case RELOAD:
                RETURN_IF_ERROR(reload_op());
                break;
            default:
                break;
            }
        }
        // check at last.
        return get_op();
    }

protected:
    constexpr static const char* const kTestGroupPath = "test_lake_primary_key_consistency";

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    int64_t _partition_id = next_id();
    std::vector<SlotDescriptor> _slots;
    std::vector<SlotDescriptor*> _slot_pointers;
    Chunk::SlotHashMap _slot_cid_map;
    int64_t _version = 0;

    int _seed = 0;
    int _run_second = 0;
    std::unique_ptr<DeterRandomGenerator<int, PICT_OP>> _random_generator;
    std::unique_ptr<WeightedRandomOpSelector<int, PICT_OP>> _random_op_selector;
    std::unique_ptr<Replayer> _replayer;

    int64_t _old_l0_size = 0;
    int64_t _old_memtable_size = 0;
};

TEST_P(LakePrimaryKeyConsistencyTest, test_local_pk_consistency) {
    _seed = 1719499276; //time(nullptr);
    _run_second = 50;   // 50 second
    auto st = run_random_tests();
    if (!st.ok()) {
        LOG(FATAL) << "run_random_tests fail, st : " << st << " seed : " << _seed;
    }
}

INSTANTIATE_TEST_SUITE_P(LakePrimaryKeyConsistencyTest, LakePrimaryKeyConsistencyTest,
                         ::testing::Values(PrimaryKeyParam{.persistent_index_type = PersistentIndexTypePB::LOCAL}));

} // namespace starrocks::lake