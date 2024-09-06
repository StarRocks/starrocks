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
    UPSERT_WITH_BATCH_PUB = 5,
    PARTIAL_UPDATE_ROW = 6,
    PARTIAL_UPDATE_COLUMN = 7,
    CONDITION_UPDATE = 8,
    MAX = 9,
};

static const std::string kTestGroupPath = "./test_lake_primary_key_consistency";
static const int64_t MaxNumber = 1000000;
static const int64_t MaxN = 10000;
static const size_t MaxUpsert = 4;
static const size_t MaxBatchCnt = 5;
static const int64_t io_failure_percent = 3;

class Replayer {
public:
    enum ReplayerOP {
        UPSERT,
        ERASE,
        PARTIAL_UPSERT,
        PARTIAL_UPDATE,
        CONDITION_UPDATE,
    };

    class ReplayEntry {
    public:
        ReplayEntry(const ReplayerOP& o, const ChunkPtr& cp, const string& cc)
                : op(o), chunk_ptr(cp), condition_col(cc) {}
        // Operation type
        ReplayerOP op;
        // Replay data
        ChunkPtr chunk_ptr;
        // for condition update
        string condition_col;
    };

    void upsert(const ChunkPtr& chunk) { _redo_logs.emplace_back(ReplayerOP::UPSERT, chunk, ""); }

    void erase(const ChunkPtr& chunk) { _redo_logs.emplace_back(ReplayerOP::ERASE, chunk, ""); }

    void partial_upsert(const ChunkPtr& chunk) { _redo_logs.emplace_back(ReplayerOP::PARTIAL_UPSERT, chunk, ""); }

    void partial_update(const ChunkPtr& chunk) { _redo_logs.emplace_back(ReplayerOP::PARTIAL_UPDATE, chunk, ""); }

    void condition_update(const ChunkPtr& chunk, const std::string& condition_col) {
        _redo_logs.emplace_back(ReplayerOP::CONDITION_UPDATE, chunk, condition_col);
    }

    bool check(const ChunkPtr& chunk) {
        std::map<int, std::pair<int, int>> tmp_chunk;
        for (int i = 0; i < chunk->num_rows(); i++) {
            if (tmp_chunk.count(chunk->columns()[0]->get(i).get_int32()) > 0) {
                // duplicate pk
                LOG(ERROR) << "duplicate pk: " << chunk->columns()[0]->get(i).get_int32();
                return false;
            }
            tmp_chunk[chunk->columns()[0]->get(i).get_int32()] = {chunk->columns()[1]->get(i).get_int32(),
                                                                  chunk->columns()[2]->get(i).get_int32()};
        }
        if (tmp_chunk.size() != _replayer_index.size()) {
            LOG(ERROR) << "inconsistency row number, actual : " << tmp_chunk.size()
                       << " expected : " << _replayer_index.size();
            return false;
        }
        for (const auto& each : _replayer_index) {
            // check vals
            auto iter = tmp_chunk.find(each.first);
            if (iter == tmp_chunk.end()) {
                LOG(ERROR) << "inconsistency result, mismatch key : " << each.first;
                return false;
            }
            if (iter->second.first != each.second.first) {
                LOG(ERROR) << "inconsistency result, mismatch c1 : " << iter->second.first << " : "
                           << each.second.first;
                return false;
            }
            if (iter->second.second != each.second.second) {
                LOG(ERROR) << "inconsistency result, mismatch c2 : " << iter->second.second << " : "
                           << each.second.second;
                return false;
            }
            tmp_chunk.erase(each.first);
        }
        if (!tmp_chunk.empty()) {
            LOG(ERROR) << "inconsistency result.";
            return false;
        }
        return true;
    }

    void commit() {
        for (const auto& log : _redo_logs) {
            auto chunk = log.chunk_ptr;
            if (log.op == ReplayerOP::UPSERT) {
                // Upsert
                for (int i = 0; i < chunk->num_rows(); i++) {
                    _replayer_index[chunk->columns()[0]->get(i).get_int32()] = {
                            chunk->columns()[1]->get(i).get_int32(), chunk->columns()[2]->get(i).get_int32()};
                }
            } else if (log.op == ReplayerOP::ERASE) {
                // Delete
                for (int i = 0; i < chunk->num_rows(); i++) {
                    _replayer_index.erase(chunk->columns()[0]->get(i).get_int32());
                }
            } else if (log.op == ReplayerOP::PARTIAL_UPSERT || log.op == ReplayerOP::PARTIAL_UPDATE) {
                // Partial update
                for (int i = 0; i < chunk->num_rows(); i++) {
                    auto iter = _replayer_index.find(chunk->columns()[0]->get(i).get_int32());
                    if (iter != _replayer_index.end()) {
                        _replayer_index[chunk->columns()[0]->get(i).get_int32()] = {
                                chunk->columns()[1]->get(i).get_int32(), iter->second.second};
                    } else if (log.op == ReplayerOP::PARTIAL_UPSERT) {
                        // insert new record with default val
                        _replayer_index[chunk->columns()[0]->get(i).get_int32()] = {
                                chunk->columns()[1]->get(i).get_int32(), 0};
                    } else {
                        // do nothing
                    }
                }
            } else if (log.op == ReplayerOP::CONDITION_UPDATE) {
                // condition update
                auto is_condition_meet_fn = [&](const std::pair<int, int>& current, int index) {
                    if (log.condition_col == "c1") {
                        if (chunk->columns()[1]->get(index).get_int32() >= current.first) {
                            return true;
                        }
                    } else if (log.condition_col == "c2") {
                        if (chunk->columns()[2]->get(index).get_int32() >= current.second) {
                            return true;
                        }
                    } else {
                        // do nothing
                    }
                    return false;
                };
                for (int i = 0; i < chunk->num_rows(); i++) {
                    auto iter = _replayer_index.find(chunk->columns()[0]->get(i).get_int32());
                    if (iter == _replayer_index.end() || is_condition_meet_fn(iter->second, i)) {
                        // update if condition meet or not found
                        // insert new record
                        _replayer_index[chunk->columns()[0]->get(i).get_int32()] = {
                                chunk->columns()[1]->get(i).get_int32(), chunk->columns()[2]->get(i).get_int32()};
                    }
                }
            } else {
                // do nothing
            }
        }
    }

    void abort() { _redo_logs.clear(); }

private:
    // logs for replay.
    std::vector<ReplayEntry> _redo_logs;
    // c0 -> <c1, c2>
    std::map<int, std::pair<int, int>> _replayer_index;
};

class LakePrimaryKeyConsistencyTest : public TestBase, testing::WithParamInterface<PrimaryKeyParam> {
public:
    LakePrimaryKeyConsistencyTest() : TestBase(kTestGroupPath) {
        _tablet_metadata = generate_tablet_metadata(PRIMARY_KEYS);
        _tablet_metadata->set_enable_persistent_index(true);
        _tablet_metadata->set_persistent_index_type(GetParam().persistent_index_type);

        _slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
        _partial_slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
        _slots.emplace_back(1, "c1", TypeDescriptor{LogicalType::TYPE_INT});
        _partial_slots.emplace_back(1, "c1", TypeDescriptor{LogicalType::TYPE_INT});
        _slots.emplace_back(2, "c2", TypeDescriptor{LogicalType::TYPE_INT});
        _slots.emplace_back(3, "__op", TypeDescriptor{LogicalType::TYPE_INT});
        _slot_pointers.emplace_back(&_slots[0]);
        _slot_pointers.emplace_back(&_slots[1]);
        _partial_slot_pointers.emplace_back(&_partial_slots[0]);
        _partial_slot_pointers.emplace_back(&_partial_slots[1]);
        _slot_pointers.emplace_back(&_slots[2]);
        _slot_pointers.emplace_back(&_slots[3]);

        _slot_cid_map.emplace(0, 0);
        _slot_cid_map.emplace(1, 1);
        _slot_cid_map.emplace(2, 2);
        _slot_cid_map.emplace(3, 3);

        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
        _version = 1;

        _random_generator = std::make_unique<DeterRandomGenerator<int, PICT_OP>>(MaxNumber, MaxN, _seed, 0, INT64_MAX);
        std::vector<WeightedItem<PICT_OP>> items;
        items.emplace_back(PICT_OP::UPSERT, 28);
        items.emplace_back(PICT_OP::DELETE, 15);
        items.emplace_back(PICT_OP::GET, 5);
        items.emplace_back(PICT_OP::COMPACT, 10);
        items.emplace_back(PICT_OP::RELOAD, 10);
        items.emplace_back(PICT_OP::UPSERT_WITH_BATCH_PUB, 17);
        items.emplace_back(PICT_OP::PARTIAL_UPDATE_ROW, 5);
        items.emplace_back(PICT_OP::PARTIAL_UPDATE_COLUMN, 5);
        items.emplace_back(PICT_OP::CONDITION_UPDATE, 5);
        _random_op_selector = std::make_unique<WeightedRandomOpSelector<int, PICT_OP>>(_random_generator.get(), items);
        _io_failure_generator =
                std::make_unique<IOFailureGenerator<int, PICT_OP>>(_random_generator.get(), io_failure_percent);
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
        _old_enable_pk_strict_memcheck = config::enable_pk_strict_memcheck;
        config::enable_pk_strict_memcheck = false;
    }

    void TearDown() override {
        (void)fs::remove_all(kTestGroupPath);
        config::l0_max_mem_usage = _old_l0_size;
        config::write_buffer_size = _old_memtable_size;
        config::enable_pk_strict_memcheck = _old_enable_pk_strict_memcheck;
    }

    std::shared_ptr<TabletMetadataPB> generate_tablet_metadata(KeysType keys_type) {
        auto metadata = std::make_shared<TabletMetadata>();
        metadata->set_id(next_id());
        metadata->set_version(1);
        metadata->set_cumulative_point(0);
        metadata->set_next_rowset_id(1);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | NO  |  NO  |
        //  |   c2   |  INT | NO  |  NO  |
        auto schema = metadata->mutable_schema();
        schema->set_keys_type(keys_type);
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
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
            c1->set_aggregation(keys_type == DUP_KEYS ? "NONE" : "REPLACE");
        }
        auto c2 = schema->add_column();
        {
            c2->set_unique_id(next_id());
            c2->set_name("c2");
            c2->set_type("INT");
            c2->set_is_key(false);
            c2->set_is_nullable(false);
            c2->set_aggregation(keys_type == DUP_KEYS ? "NONE" : "REPLACE");
        }
        return metadata;
    }

    std::pair<ChunkPtr, std::vector<uint32_t>> gen_upsert_data(bool is_upsert) {
        const size_t chunk_size = (size_t)_random_generator->random_n();
        std::vector<std::vector<int>> cols(3);
        std::vector<uint8_t> v3(chunk_size, is_upsert ? TOpType::UPSERT : TOpType::DELETE);
        _random_generator->random_cols(chunk_size, &cols);

        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        auto c2 = Int32Column::create();
        auto c3 = Int8Column::create();
        c0->append_numbers(cols[0].data(), cols[0].size() * sizeof(int));
        c1->append_numbers(cols[1].data(), cols[1].size() * sizeof(int));
        c2->append_numbers(cols[2].data(), cols[2].size() * sizeof(int));
        c3->append_numbers(v3.data(), v3.size() * sizeof(uint8_t));
        auto indexes = std::vector<uint32_t>(chunk_size);
        for (uint32_t i = 0; i < chunk_size; i++) {
            indexes[i] = i;
        }
        return {std::make_shared<Chunk>(Columns{c0, c1, c2, c3}, _slot_cid_map), std::move(indexes)};
    }

    std::pair<ChunkPtr, std::vector<uint32_t>> gen_partial_update_data() {
        const size_t chunk_size = (size_t)_random_generator->random_n();
        std::vector<std::vector<int>> cols(2);
        std::vector<uint8_t> v3(chunk_size, TOpType::UPSERT);
        _random_generator->random_cols(chunk_size, &cols);

        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(cols[0].data(), cols[0].size() * sizeof(int));
        c1->append_numbers(cols[1].data(), cols[1].size() * sizeof(int));
        auto indexes = std::vector<uint32_t>(chunk_size);
        for (uint32_t i = 0; i < chunk_size; i++) {
            indexes[i] = i;
        }
        return {std::make_shared<Chunk>(Columns{c0, c1}, _slot_cid_map), std::move(indexes)};
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

    Status partial_update_op(PartialUpdateMode mode) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(_tablet_metadata->id())
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_partial_slot_pointers)
                                                   .set_partial_update_mode(mode)
                                                   .build());
        RETURN_IF_ERROR(delta_writer->open());
        size_t upsert_size = _random_generator->random() % MaxUpsert;
        for (int i = 0; i < upsert_size; i++) {
            auto chunk_index = gen_partial_update_data();
            RETURN_IF_ERROR(
                    delta_writer->write(*(chunk_index.first), chunk_index.second.data(), chunk_index.second.size()));
            if (mode == PartialUpdateMode::ROW_MODE) {
                // upsert
                _replayer->partial_upsert(chunk_index.first);
            } else {
                // update
                _replayer->partial_update(chunk_index.first);
            }
        }
        RETURN_IF_ERROR(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        RETURN_IF_ERROR(publish_single_version(_tablet_metadata->id(), _version + 1, txn_id));
        _version++;
        return Status::OK();
    }

    Status condition_update() {
        auto txn_id = next_id();
        // c2 as merge_condition
        std::string merge_condition = "c2";
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(_tablet_metadata->id())
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_merge_condition(merge_condition)
                                                   .build());
        RETURN_IF_ERROR(delta_writer->open());
        size_t upsert_size = _random_generator->random() % MaxUpsert;
        for (int i = 0; i < upsert_size; i++) {
            auto chunk_index = gen_upsert_data(true);
            RETURN_IF_ERROR(
                    delta_writer->write(*(chunk_index.first), chunk_index.second.data(), chunk_index.second.size()));
            _replayer->condition_update(chunk_index.first, merge_condition);
        }
        RETURN_IF_ERROR(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        RETURN_IF_ERROR(publish_single_version(_tablet_metadata->id(), _version + 1, txn_id));
        _version++;
        return Status::OK();
    }

    Status upsert_with_batch_pub_op() {
        size_t batch_cnt = std::max(_random_generator->random() % MaxBatchCnt, (size_t)1);
        std::vector<int64_t> txn_ids;
        for (int i = 0; i < batch_cnt; i++) {
            auto txn_id = next_id();
            txn_ids.push_back(txn_id);
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
                RETURN_IF_ERROR(delta_writer->write(*(chunk_index.first), chunk_index.second.data(),
                                                    chunk_index.second.size()));
                _replayer->upsert(chunk_index.first);
            }
            RETURN_IF_ERROR(delta_writer->finish_with_txnlog());
            delta_writer->close();
        }
        // Batch Publish version
        auto new_version = _version + batch_cnt;
        RETURN_IF_ERROR(batch_publish(_tablet_metadata->id(), _version, new_version, txn_ids).status());
        _version = new_version;
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
            auto io_failure_guard = _io_failure_generator->generate();
            auto op = _random_op_selector->select();
            auto st = Status::OK();
            switch (op) {
            case UPSERT:
                st = upsert_op();
                break;
            case DELETE:
                st = delete_op();
                break;
            case GET:
                st = get_op();
                break;
            case COMPACT:
                st = compact_op();
                break;
            case RELOAD:
                st = reload_op();
                break;
            case UPSERT_WITH_BATCH_PUB:
                st = upsert_with_batch_pub_op();
                break;
            case PARTIAL_UPDATE_ROW:
                st = partial_update_op(PartialUpdateMode::ROW_MODE);
                break;
            case PARTIAL_UPDATE_COLUMN:
                st = partial_update_op(PartialUpdateMode::COLUMN_UPDATE_MODE);
                break;
            case CONDITION_UPDATE:
                st = condition_update();
                break;
            default:
                break;
            }
            if (io_failure_guard != nullptr) {
                // failure inject
                if (st.ok()) {
                    _replayer->commit();
                } else {
                    _replayer->abort();
                }
            } else {
                // No failure inject
                if (st.ok()) {
                    _replayer->commit();
                } else {
                    _replayer->abort();
                    return st;
                }
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
    std::vector<SlotDescriptor> _partial_slots;
    std::vector<SlotDescriptor*> _partial_slot_pointers;
    Chunk::SlotHashMap _slot_cid_map;
    int64_t _version = 0;

    int _seed = 0;
    int _run_second = 0;
    std::unique_ptr<DeterRandomGenerator<int, PICT_OP>> _random_generator;
    std::unique_ptr<WeightedRandomOpSelector<int, PICT_OP>> _random_op_selector;
    std::unique_ptr<IOFailureGenerator<int, PICT_OP>> _io_failure_generator;
    std::unique_ptr<Replayer> _replayer;

    int64_t _old_l0_size = 0;
    int64_t _old_memtable_size = 0;
    bool _old_enable_pk_strict_memcheck = false;
};

TEST_P(LakePrimaryKeyConsistencyTest, test_local_pk_consistency) {
    _seed = 1719499276; // seed
    _run_second = 50;   // 50 second
    LOG(INFO) << "LakePrimaryKeyConsistencyTest begin, seed : " << _seed;
    auto st = run_random_tests();
    if (!st.ok()) {
        LOG(FATAL) << "run_random_tests fail, st : " << st << " seed : " << _seed;
    }
}

TEST_P(LakePrimaryKeyConsistencyTest, test_random_seed_pk_consistency) {
    _seed = time(nullptr); // use current ts as seed
    _run_second = 50;      // 50 second
    LOG(INFO) << "LakePrimaryKeyConsistencyTest begin, seed : " << _seed;
    auto st = run_random_tests();
    if (!st.ok()) {
        LOG(FATAL) << "run_random_tests fail, st : " << st << " seed : " << _seed;
    }
}

INSTANTIATE_TEST_SUITE_P(LakePrimaryKeyConsistencyTest, LakePrimaryKeyConsistencyTest,
                         ::testing::Values(PrimaryKeyParam{.persistent_index_type = PersistentIndexTypePB::LOCAL},
                                           PrimaryKeyParam{
                                                   .persistent_index_type = PersistentIndexTypePB::CLOUD_NATIVE}));

} // namespace starrocks::lake