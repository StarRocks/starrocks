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

#include <chrono>
#include <cmath>
#include <memory>
#include <thread>
#include <utility>

#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "exec/pipeline/pipeline.h"
#include "exec/pipeline/pipeline_driver.h"
#include "exec/query_cache/cache_manager.h"
#include "exec/query_cache/cache_operator.h"
#include "exec/query_cache/cache_param.h"
#include "exec/query_cache/conjugate_operator.h"
#include "exec/query_cache/lane_arbiter.h"
#include "exec/query_cache/multilane_operator.h"
#include "exec/query_cache/ticket_checker.h"
#include "exec/query_cache/transform_operator.h"
#include "gutil/strings/substitute.h"

namespace starrocks {
struct QueryCacheTest : public ::testing::Test {
    RuntimeState state;
    query_cache::CacheManagerPtr cache_mgr = std::make_shared<query_cache::CacheManager>(10240);
};

TEST_F(QueryCacheTest, testLaneArbiter) {
    size_t num_lanes = 4;
    auto lane_arbiter = std::make_shared<query_cache::LaneArbiter>(num_lanes);
    ASSERT_FALSE(lane_arbiter->preferred_lane().has_value());
    for (auto i = 1; i <= num_lanes; ++i) {
        query_cache::LaneOwnerType lane_owner = i;
        auto result = lane_arbiter->try_acquire_lane(lane_owner);
        ASSERT_EQ(result, query_cache::AcquireResult::AR_PROBE);
        result = lane_arbiter->try_acquire_lane(lane_owner);
        ASSERT_EQ(result, query_cache::AcquireResult::AR_IO);
        lane_arbiter->must_acquire_lane(lane_owner);
        std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
    auto opt_lane = lane_arbiter->preferred_lane();
    ASSERT_TRUE(opt_lane.has_value());
    ASSERT_TRUE(lane_arbiter->try_acquire_lane(5) == query_cache::AcquireResult::AR_BUSY);
    lane_arbiter->mark_processed(2);
    ASSERT_TRUE(lane_arbiter->try_acquire_lane(2) == query_cache::AcquireResult::AR_SKIP);
    lane_arbiter->release_lane(2);

    ASSERT_TRUE(lane_arbiter->try_acquire_lane(1) == query_cache::AcquireResult::AR_IO);
    ASSERT_TRUE(lane_arbiter->try_acquire_lane(3) == query_cache::AcquireResult::AR_IO);
    ASSERT_TRUE(lane_arbiter->try_acquire_lane(3) == query_cache::AcquireResult::AR_IO);
    ASSERT_TRUE(lane_arbiter->try_acquire_lane(5) == query_cache::AcquireResult::AR_PROBE);

    lane_arbiter->enable_passthrough_mode();
    ASSERT_TRUE(lane_arbiter->in_passthrough_mode());
    ASSERT_TRUE(lane_arbiter->try_acquire_lane(6) == query_cache::AcquireResult::AR_IO);
    ASSERT_TRUE(lane_arbiter->try_acquire_lane(7) == query_cache::AcquireResult::AR_IO);
    ASSERT_TRUE(lane_arbiter->try_acquire_lane(8) == query_cache::AcquireResult::AR_IO);
    ASSERT_TRUE(lane_arbiter->try_acquire_lane(9) == query_cache::AcquireResult::AR_IO);

    opt_lane = lane_arbiter->preferred_lane();
    ASSERT_TRUE(opt_lane.has_value());
    lane_arbiter->release_lane(1);

    opt_lane = lane_arbiter->preferred_lane();
    ASSERT_TRUE(opt_lane.has_value());
    lane_arbiter->release_lane(3);

    opt_lane = lane_arbiter->preferred_lane();
    ASSERT_TRUE(opt_lane.has_value());
    lane_arbiter->release_lane(4);

    opt_lane = lane_arbiter->preferred_lane();
    ASSERT_TRUE(opt_lane.has_value());
    lane_arbiter->release_lane(5);

    opt_lane = lane_arbiter->preferred_lane();
    ASSERT_FALSE(opt_lane.has_value());
}

TEST_F(QueryCacheTest, testCacheManager) {
    static constexpr size_t CACHE_CAPACITY = 10240;
    auto cache_mgr = std::make_shared<query_cache::CacheManager>(CACHE_CAPACITY);

    auto create_cache_value = [](size_t byte_size) {
        auto chk = std::make_shared<Chunk>();
        auto col = Int8Column::create();
        auto payload = byte_size - sizeof(query_cache::CacheValue);
        col->resize(payload);
        chk->append_column(col, 0);
        query_cache::CacheValue value(0, 0, {chk});
        return value;
    };

    for (auto i = 0; i < 10; ++i) {
        cache_mgr->populate(strings::Substitute("key_$0", i), create_cache_value(96));
    }

    ASSERT_EQ(cache_mgr->memory_usage(), 960);
    for (auto i = 0; i < 10; ++i) {
        auto status = cache_mgr->probe(strings::Substitute("key_$0", i));
        ASSERT_TRUE(status.ok());
    }

    ASSERT_EQ(cache_mgr->memory_usage(), 960);
    for (auto i = 10; i < 20; ++i) {
        auto status = cache_mgr->probe(strings::Substitute("key_$0", i));
        ASSERT_FALSE(status.ok());
    }
    ASSERT_EQ(cache_mgr->memory_usage(), 960);

    for (auto i = 20; i < 30; ++i) {
        auto status = cache_mgr->populate(strings::Substitute("key_$0", i), create_cache_value(100));
    }
    ASSERT_LE(cache_mgr->memory_usage(), cache_mgr->capacity());

    bool exists = false;
    for (auto i = 20; i < 30; ++i) {
        auto status = cache_mgr->probe(strings::Substitute("key_$0", i));
        if (status.ok()) {
            exists = true;
        }
    }
    ASSERT_TRUE(exists);
    ASSERT_EQ(cache_mgr->capacity(), CACHE_CAPACITY);
    ASSERT_GE(cache_mgr->memory_usage(), 0);
    ASSERT_GE(cache_mgr->lookup_count(), 0);
    ASSERT_GE(cache_mgr->hit_count(), 0);

    cache_mgr->invalidate_all();
    ASSERT_EQ(cache_mgr->capacity(), CACHE_CAPACITY);
    ASSERT_EQ(cache_mgr->memory_usage(), 0);
    ASSERT_GE(cache_mgr->lookup_count(), 0);
    ASSERT_GE(cache_mgr->hit_count(), 0);

    for (auto i = 0; i < 10; ++i) {
        cache_mgr->populate(strings::Substitute("key_$0", i), create_cache_value(96));
    }
    ASSERT_EQ(cache_mgr->capacity(), CACHE_CAPACITY);
    ASSERT_GE(cache_mgr->memory_usage(), 0);
}

ChunkPtr create_test_chunk(query_cache::LaneOwnerType owner, long from, long to, bool is_last_chunk) {
    ChunkPtr chunk = std::make_shared<Chunk>();
    chunk->owner_info().set_owner_id(owner, is_last_chunk);
    if (from == to) {
        return chunk;
    }

    auto column = DoubleColumn::create();
    column->resize(to - from);
    auto& data = dynamic_cast<DoubleColumn*>(column.get())->get_data();
    for (auto i = from; i < to; ++i) {
        data[i - from] = double(i);
    }
    chunk->append_column(column, SlotId(1));
    return chunk;
}
struct Task {
    pipeline::Operators upstream;
    pipeline::OperatorPtr downstream;
    query_cache::CacheOperatorPtr cache_operator;
    query_cache::LaneArbiterPtr lane_arbiter;
};

using Tasks = std::vector<Task>;
query_cache::CacheParam create_test_cache_param(bool force_populate, bool force_passthrough, int num_lanes) {
    query_cache::CacheParam cache_param;
    cache_param.num_lanes = num_lanes;
    cache_param.force_populate = force_populate;
    cache_param.plan_node_id = 10;
    cache_param.digest = "cache_key_";
    cache_param.cache_key_prefixes = {{1, "tablet#1"}, {2, "tablet#2"}, {3, "tablet#3"}, {4, "tablet#4"},
                                      {5, "tablet#5"}, {6, "tablet#6"}, {7, "tablet#7"}, {8, "tablet#8"}};
    cache_param.slot_remapping = {{1, 1}};
    cache_param.reverse_slot_remapping = {{1, 1}};
    if (force_passthrough) {
        cache_param.entry_max_bytes = 0;
        cache_param.entry_max_rows = 0;
    } else {
        cache_param.entry_max_bytes = 4 * 1024 * 1024;
        cache_param.entry_max_rows = 409600;
    }
    return cache_param;
}

using ValidateFunc = std::function<void(double)>;
Tasks create_test_pipelines(const query_cache::CacheParam& cache_param, size_t dop,
                            const query_cache::CacheManagerPtr& cache_mgr, RuntimeState* state,
                            const MapFunc& map_func1, const MapFunc& map_func2, double init_value,
                            const ReduceFunc& reduce_func) {
    int id = 0;
    auto mul2 = std::make_shared<MapOperatorFactory>(++id, map_func1);
    auto plus1 = std::make_shared<MapOperatorFactory>(++id, map_func2);
    auto per_tablet_reducer = std::make_shared<ReducerFactory>(init_value, reduce_func, 1);
    auto per_tablet_reduce_sink = std::make_shared<ReduceSinkOperatorFactory>(++id, per_tablet_reducer.get());
    auto per_tablet_reduce_source = std::make_shared<ReduceSourceOperatorFactory>(++id, per_tablet_reducer);
    auto conjugate_op =
            std::make_shared<query_cache::ConjugateOperatorFactory>(per_tablet_reduce_sink, per_tablet_reduce_source);
    std::vector<OperatorFactoryPtr> opFactories;
    opFactories.push_back(mul2);
    opFactories.push_back(plus1);
    opFactories.push_back(conjugate_op);

    auto cache_id = ++id;
    auto plan_node_id = ++id;
    auto cache_op_factory =
            std::make_shared<query_cache::CacheOperatorFactory>(cache_id, plan_node_id, cache_mgr.get(), cache_param);
    for (auto& opFactorie : opFactories) {
        opFactorie = std::make_shared<query_cache::MultilaneOperatorFactory>(++id, opFactorie, cache_param.num_lanes);
    }
    opFactories.push_back(cache_op_factory);
    auto reducer = std::make_shared<ReducerFactory>(init_value, reduce_func, 1);
    auto reduce_sink = std::make_shared<ReduceSinkOperatorFactory>(++id, reducer.get());
    auto reduce_source = std::make_shared<ReduceSourceOperatorFactory>(++id, reducer);
    opFactories.push_back(reduce_sink);
    Tasks tasks;
    tasks.resize(dop);
    for (auto i = 0; i < dop; ++i) {
        pipeline::Pipeline pipeline(0, opFactories);
        auto upstream_operators = pipeline.create_operators(dop, i);
        auto downstream_operator = reduce_source->create(dop, i);
        tasks[i].upstream = std::move(upstream_operators);
        tasks[i].downstream = std::move(downstream_operator);
    }

    for (auto k = 0; k < dop; ++k) {
        size_t cache_op_idx = 0;
        query_cache::CacheOperatorPtr cache_op = nullptr;
        auto& upstream = tasks[k].upstream;
        for (size_t i = 0, size = upstream.size(); i < size; ++i) {
            if (cache_op = std::dynamic_pointer_cast<query_cache::CacheOperator>(upstream[i]); cache_op != nullptr) {
                cache_op_idx = i;
                break;
            }
        }
        tasks[k].cache_operator = cache_op;
        tasks[k].lane_arbiter = cache_op->lane_arbiter();
        query_cache::MultilaneOperators multilane_operators;
        for (size_t i = 0, size = cache_op_idx; i < size; ++i) {
            auto* ml_op = dynamic_cast<query_cache::MultilaneOperator*>(upstream[i].get());
            ml_op->set_lane_arbiter(cache_op->lane_arbiter());
            multilane_operators.push_back(ml_op);
        }
        tasks[k].cache_operator->set_multilane_operators(std::move(multilane_operators));

        for (auto& i : upstream) {
            i->prepare(state);
        }

        tasks[k].downstream->prepare(state);
    }
    return tasks;
}

bool exec_test_pipeline(Task& task, RuntimeState* state, const ChunkPtr& input_chunk, int max_step,
                        bool set_first_op_finished) {
    auto& first_op = task.upstream[0];
    auto& upstream = task.upstream;
    bool pushed = input_chunk == nullptr;
    int num_steps = 0;
    int first_unfinished_idx = 0;
    if (set_first_op_finished) {
        first_op->set_finishing(state);
    }
    for (; first_unfinished_idx < upstream.size() && upstream[first_unfinished_idx]->is_finished();
         ++first_unfinished_idx) {
    }
    if (first_unfinished_idx == upstream.size()) {
        return true;
    }

    if (first_unfinished_idx > 0) {
        upstream[first_unfinished_idx]->set_finishing(state);
    }

    while (true) {
        if (!first_op->is_finished() && first_op->need_input() && !pushed) {
            LOG(WARNING) << strings::Substitute("[EXEC] Push input chunk: op=$0, num_rows=$1, tablet_id=$2, eof=$3",
                                                first_op->get_name(), input_chunk->num_rows(),
                                                input_chunk->owner_info().owner_id(),
                                                input_chunk->owner_info().is_last_chunk());
            first_op->push_chunk(state, input_chunk);
            pushed = true;
        }
        bool movable = false;
        for (auto i = first_unfinished_idx + 1; i < upstream.size() && num_steps < max_step; ++i) {
            auto curr_op = upstream[i - 1];
            auto next_op = upstream[i];
            if (curr_op->is_finished()) {
                next_op->set_finishing(state);
                movable = true;
                first_unfinished_idx = i;
                continue;
            }
            if (curr_op->has_output() && next_op->need_input()) {
                movable = true;
                auto chunk_or_status = curr_op->pull_chunk(state);
                DCHECK(chunk_or_status.ok());
                auto chunk = chunk_or_status.value();
                if (chunk == nullptr) {
                    continue;
                }
                LOG(WARNING) << strings::Substitute(
                        "[EXEC] Transfer chunk: from_op=$0, to_op=$1, num_rows=$2, tablet_id=$3, eof=$4",
                        curr_op->get_name(), next_op->get_name(), chunk->num_rows(), chunk->owner_info().owner_id(),
                        chunk->owner_info().is_last_chunk());
                next_op->push_chunk(state, chunk);
                num_steps += pushed;
                if (curr_op->is_finished()) {
                    first_unfinished_idx = i;
                    next_op->set_finishing(state);
                }
            }
        }
        if (first_unfinished_idx == upstream.size() - 1) {
            DCHECK(upstream.back()->is_finished());
            return true;
        }
        if ((pushed || first_unfinished_idx > 0) && (!movable || num_steps > max_step)) {
            return false;
        }
    }
}

struct Action {
    query_cache::LaneOwnerType owner_id;
    int64_t version = 1;
    long from;
    long to;
    int max_step;
    bool send_eof = false;
    bool set_first_op_finished = false;
    query_cache::AcquireResult expect_acquire_result;
    bool expect_probe_result = false;
    bool expect_finished = false;
    std::string to_string() const {
        return strings::Substitute(
                "Action{owner_id=$0, rows=[$1, $2), max_step=$3, eof=$4, set_first_op_finished=$5, "
                "expect_acquire_result=$6, expect_probe_result=$7, expect_finished=$8}",
                owner_id, from, to, max_step, send_eof, set_first_op_finished, expect_acquire_result,
                expect_probe_result, expect_finished);
    }
    static Action just_run(int max_step, bool expect_finished) {
        return Action{.owner_id = 0, .max_step = max_step, .expect_finished = expect_finished};
    }

    static Action emit_eof(query_cache::LaneOwnerType owner_id) {
        return Action{.owner_id = owner_id,
                      .from = 0,
                      .to = 0,
                      .max_step = 1,
                      .send_eof = true,
                      .expect_acquire_result = query_cache::AR_IO,
                      .expect_finished = false};
    }

    static Action finish() {
        return Action{.owner_id = 0, .max_step = INT_MAX, .set_first_op_finished = true, .expect_finished = true};
    }

    static Action acquire_lane_busy(query_cache::LaneOwnerType owner_id) {
        return Action{.owner_id = owner_id, .expect_acquire_result = query_cache::AR_BUSY};
    }

    static Action acquire_lane_skip(query_cache::LaneOwnerType owner_id) {
        return Action{.owner_id = owner_id, .expect_acquire_result = query_cache::AR_SKIP};
    }

    static Action cache_hit(query_cache::LaneOwnerType owner_id, int64_t version = 1) {
        return Action{.owner_id = owner_id,
                      .version = version,
                      .expect_acquire_result = query_cache::AR_PROBE,
                      .expect_probe_result = true};
    }

    static Action cache_miss_and_emit_first_chunk(query_cache::LaneOwnerType owner_id, long from, long to, int max_step,
                                                  bool send_eof, bool expect_finished, int64_t version = 1) {
        return Action{.owner_id = owner_id,
                      .version = version,
                      .from = from,
                      .to = to,
                      .max_step = max_step,
                      .send_eof = send_eof,
                      .expect_acquire_result = query_cache::AR_PROBE,
                      .expect_probe_result = false,
                      .expect_finished = expect_finished};
    }

    static Action cache_partial_hit_and_emit_first_chunk(query_cache::LaneOwnerType owner_id, long from, long to,
                                                         int max_step, bool send_eof, bool expect_finished,
                                                         int64_t version) {
        return Action{.owner_id = owner_id,
                      .version = version,
                      .from = from,
                      .to = to,
                      .max_step = max_step,
                      .send_eof = send_eof,
                      .expect_acquire_result = query_cache::AR_PROBE,
                      .expect_probe_result = true,
                      .expect_finished = expect_finished};
    }

    static Action emit_remain_chunk(query_cache::LaneOwnerType owner_id, long from, long to, int max_step,
                                    bool send_eof, bool expect_finished) {
        return Action{.owner_id = owner_id,
                      .from = from,
                      .to = to,
                      .max_step = max_step,
                      .send_eof = send_eof,
                      .expect_acquire_result = query_cache::AR_IO,
                      .expect_probe_result = false,
                      .expect_finished = expect_finished};
    }
};

using Actions = std::vector<Action>;
void take_action(Task& task, const Action& action, RuntimeState* state) {
    if (action.owner_id == 0) {
        auto finished = exec_test_pipeline(task, state, nullptr, action.max_step, action.set_first_op_finished);
        ASSERT_EQ(finished, action.expect_finished);
        return;
    }

    auto exec_action = [](Task& task, const Action& action, RuntimeState* state) {
        auto chunk = create_test_chunk(action.owner_id, action.from, action.to, action.send_eof);
        auto finished = exec_test_pipeline(task, state, chunk, action.max_step, action.set_first_op_finished);
        ASSERT_EQ(finished, action.expect_finished);
    };

    auto ac_result = task.lane_arbiter->try_acquire_lane(action.owner_id);
    ASSERT_EQ(ac_result, action.expect_acquire_result);
    if (ac_result == query_cache::AcquireResult::AR_PROBE) {
        auto probe_result = task.cache_operator->probe_cache(action.owner_id, action.version);
        ASSERT_EQ(probe_result, action.expect_probe_result);
        task.cache_operator->reset_lane(state, action.owner_id);
        if (!probe_result) {
            exec_action(task, action, state);
            return;
        }
        auto real_version = task.cache_operator->cached_version(action.owner_id);
        ASSERT_TRUE(real_version <= action.version);
        if (real_version < action.version) {
            exec_action(task, action, state);
        }
    } else if (ac_result == query_cache::AcquireResult::AR_IO) {
        exec_action(task, action, state);
    } else {
        return;
    }
}

static MapFunc mul2_func = [](double a) { return 2 * a; };
static MapFunc plus1_func = [](double a) { return a + 1; };
static ReduceFunc add_func = [](double a, double b) { return a + b; };
static MapFunc pi_map1_func = [](double a) {
    long l = (long)a;
    long s = -(l & 1L);
    return (double)((((l << 1) + 1L) ^ s) - s);
};
static MapFunc pi_map2_func = [](double a) { return 4.0 / a; };

using ValidateFuncGenerator = std::function<ValidateFunc(double)>;
static ValidateFuncGenerator eq_validator_gen = [](double expect) {
    return [expect](double actual) {
        LOG(INFO) << strings::Substitute("eq_validate: expect=$0, actual=$1", expect, actual);
        ASSERT_EQ(expect, actual);
    };
};

static ValidateFuncGenerator approx_validator_gen = [](double expect) {
    return [expect](double actual) {
        auto abs_value = std::max(std::abs(expect), std::abs(actual));
        LOG(INFO) << strings::Substitute("approx_validate: expect=$0, actual=$1", expect, actual);
        ASSERT_TRUE(abs_value == 0.0 || std::abs(expect - actual) / abs_value < 0.001);
    };
};

void test_framework_with_with_options(const query_cache::CacheManagerPtr& cache_mgr, bool force_populate,
                                      bool force_passthrough, int num_lanes, int dop, RuntimeState& state_object,
                                      const MapFunc& map1, const MapFunc& map2, double init_value,
                                      const ReduceFunc& reduce, const Actions& pre_passthrough_actions,
                                      const Actions& post_passthrough_actions, const ValidateFunc& validate_func) {
    auto* state = &state_object;
    auto cache_param = create_test_cache_param(force_populate, force_passthrough, num_lanes);
    auto tasks = create_test_pipelines(cache_param, dop, std::move(cache_mgr), state, std::move(map1), std::move(map2),
                                       init_value, std::move(reduce));
    auto& task = tasks[0];
    auto i = 0;
    for (const auto& a : pre_passthrough_actions) {
        LOG(INFO) << strings::Substitute("[PRE_PASSTHROUGH_ACTION] action#$0=$1", i, a.to_string());
        take_action(task, a, state);
        ++i;
    }
    task.cache_operator->lane_arbiter()->enable_passthrough_mode();
    for (const auto& a : post_passthrough_actions) {
        LOG(INFO) << strings::Substitute("[POST_PASSTHROUGH_ACTION] action#$0=$1", i, a.to_string());
        take_action(task, a, state);
        ++i;
    }
    ASSERT_TRUE(task.upstream.back()->is_finished());
    ASSERT_TRUE(!task.downstream->is_finished() && task.downstream->has_output());
    auto chunk_or_status = task.downstream->pull_chunk(state);
    ASSERT_TRUE(chunk_or_status.ok());
    auto chunk = chunk_or_status.value();
    auto column = chunk->get_column_by_slot_id(SlotId(1));
    auto& data = dynamic_cast<DoubleColumn*>(column.get())->get_data();
    validate_func(data[0]);
}

void test_framework(query_cache::CacheManagerPtr cache_mgr, int num_lanes, int dop, RuntimeState& state_object,
                    MapFunc map1, MapFunc map2, double init_value, ReduceFunc reduce,
                    const Actions& pre_passthrough_actions, const Actions& post_passthrough_actions,
                    const ValidateFunc& validate_func) {
    test_framework_with_with_options(std::move(cache_mgr), false, false, num_lanes, dop, state_object, std::move(map1),
                                     std::move(map2), init_value, std::move(reduce), pre_passthrough_actions,
                                     post_passthrough_actions, std::move(validate_func));
}

void test_framework_force_populate(query_cache::CacheManagerPtr cache_mgr, int num_lanes, int dop,
                                   RuntimeState& state_object, MapFunc map1, MapFunc map2, double init_value,
                                   ReduceFunc reduce, const Actions& pre_passthrough_actions,
                                   const Actions& post_passthrough_actions, const ValidateFunc& validate_func) {
    test_framework_with_with_options(std::move(cache_mgr), true, false, num_lanes, dop, state_object, std::move(map1),
                                     std::move(map2), init_value, std::move(reduce), pre_passthrough_actions,
                                     post_passthrough_actions, std::move(validate_func));
}

void test_framework_force_populate_and_passthrough(query_cache::CacheManagerPtr cache_mgr, int num_lanes, int dop,
                                                   RuntimeState& state_object, MapFunc map1, MapFunc map2,
                                                   double init_value, ReduceFunc reduce,
                                                   const Actions& pre_passthrough_actions,
                                                   const Actions& post_passthrough_actions,
                                                   const ValidateFunc& validate_func) {
    test_framework_with_with_options(std::move(cache_mgr), true, true, num_lanes, dop, state_object, std::move(map1),
                                     std::move(map2), init_value, std::move(reduce), pre_passthrough_actions,
                                     post_passthrough_actions, std::move(validate_func));
}

TEST_F(QueryCacheTest, testMultilane) {
    Actions actions = {
            Action::cache_miss_and_emit_first_chunk(1, 0, 10, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(2, 10, 11, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(3, 11, 15, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(4, 15, 20, 1, false, false),
            Action::acquire_lane_busy(5),
            Action::acquire_lane_busy(6),
            Action::emit_remain_chunk(2, 40, 44, 1, false, false),
            Action::emit_remain_chunk(1, 31, 40, 1, true, false),
            Action::emit_remain_chunk(4, 44, 50, 1, true, false),
            Action::emit_remain_chunk(3, 20, 31, 1, false, false),
            Action::just_run(INT_MAX, false),
            Action::cache_miss_and_emit_first_chunk(5, 50, 60, 1, true, false),
            Action::cache_miss_and_emit_first_chunk(6, 60, 100, 1, true, false),
            Action::emit_eof(2),
            Action::emit_eof(3),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 4, 1, state, mul2_func, plus1_func, 0.0, add_func, actions, {},
                   eq_validator_gen(10000.0));
}

TEST_F(QueryCacheTest, testOneLane) {
    Actions actions = {
            Action::cache_miss_and_emit_first_chunk(1, 0, 10, 1, false, false),
            Action::emit_eof(1),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 1, 1, state, mul2_func, plus1_func, 0.0, add_func, actions, {}, eq_validator_gen(100.0));
}

TEST_F(QueryCacheTest, testOneLanePassthroughBeforeEOFSent) {
    Actions pre_passthrough_actions = {
            Action::cache_miss_and_emit_first_chunk(1, 0, 10, 1, false, false),
    };
    Actions post_passthrough_actions = {
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 1, 1, state, mul2_func, plus1_func, 0.0, add_func, pre_passthrough_actions,
                   post_passthrough_actions, eq_validator_gen(100.0));
}

TEST_F(QueryCacheTest, testOneLanePassthroughAfterEOFSent) {
    Actions pre_passthrough_actions = {
            Action::cache_miss_and_emit_first_chunk(1, 0, 10, 1, false, false),
            Action::emit_eof(1),
            Action::just_run(INT_MAX, false),
    };
    Actions post_passthrough_actions = {
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 1, 1, state, mul2_func, plus1_func, 0.0, add_func, pre_passthrough_actions,
                   post_passthrough_actions, eq_validator_gen(100.0));
}

TEST_F(QueryCacheTest, testOneLaneEmitManyChunks) {
    Actions pre_passthrough_actions = {
            Action::cache_miss_and_emit_first_chunk(1, 0, 10, 1, false, false),
            Action::emit_remain_chunk(1, 10, 17, 1, false, false),
            Action::emit_remain_chunk(1, 17, 1000, 1, false, false),
            Action::acquire_lane_busy(2),
            Action::emit_eof(1),
            Action::just_run(INT_MAX, false),
            Action::acquire_lane_skip(1),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 1, 1, state, pi_map1_func, pi_map2_func, 0.0, add_func, pre_passthrough_actions, {},
                   approx_validator_gen(3.1415926535));

    Actions probe_actions = {
            Action::cache_hit(1), Action::acquire_lane_busy(2),    Action::acquire_lane_skip(1),
            Action::finish(),     Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 1, 1, state, pi_map1_func, pi_map2_func, 0.0, add_func, probe_actions, {},
                   approx_validator_gen(3.1415926535));
}

TEST_F(QueryCacheTest, testOneLaneEmitManyChunksPassthroughBeforeEOFSent) {
    Actions pre_passthrough_actions = {
            Action::cache_miss_and_emit_first_chunk(1, 0, 10, 1, false, false),
            Action::emit_remain_chunk(1, 10, 17, 1, false, false),
            Action::emit_remain_chunk(1, 17, 1000, 1, false, false),
            Action::acquire_lane_busy(2),
    };
    Actions post_passthrough_actions = {
            Action::emit_eof(1),
            Action::just_run(INT_MAX, false),
            Action::emit_remain_chunk(1, 1000, 1000, 1, false, false),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 1, 1, state, pi_map1_func, pi_map2_func, 0.0, add_func, pre_passthrough_actions,
                   post_passthrough_actions, approx_validator_gen(3.1415926535));

    Actions probe_actions = {
            Action::cache_miss_and_emit_first_chunk(1, 0, 1000, 1, false, false),
            Action::acquire_lane_busy(2),
            Action::emit_remain_chunk(1, 1000, 2000, 1, false, false),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 1, 1, state, pi_map1_func, pi_map2_func, 0.0, add_func, probe_actions, {},
                   approx_validator_gen(3.1415926535));
}

TEST_F(QueryCacheTest, testOneLaneEmitManyChunksPassthroughAfterEOFSent) {
    Actions pre_passthrough_actions = {
            Action::cache_miss_and_emit_first_chunk(1, 0, 10, 1, false, false),
            Action::emit_remain_chunk(1, 10, 17, 1, false, false),
            Action::emit_remain_chunk(1, 17, 100, 1, false, false),
            Action::acquire_lane_busy(2),
            Action::emit_eof(1),
            Action::just_run(INT_MAX, false),
    };
    Actions post_passthrough_actions = {
            Action::emit_remain_chunk(1, 100, 100, 1, false, false),
            Action::emit_remain_chunk(2, 100, 1000, 1, false, false),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 1, 1, state, pi_map1_func, pi_map2_func, 0.0, add_func, pre_passthrough_actions,
                   post_passthrough_actions, approx_validator_gen(3.1415926535));

    Actions probe_actions = {
            Action::cache_hit(1),
            Action::acquire_lane_busy(2),
            Action::acquire_lane_skip(1),
            Action::just_run(INT_MAX, false),
            Action::cache_miss_and_emit_first_chunk(2, 100, 1000, 1, false, false),
            Action::emit_eof(2),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 1, 1, state, pi_map1_func, pi_map2_func, 0.0, add_func, probe_actions, {},
                   approx_validator_gen(3.1415926535));
}

TEST_F(QueryCacheTest, testMultiLaneEmitManyChunks) {
    Actions pre_passthrough_actions = {
            Action::cache_miss_and_emit_first_chunk(1, 2, 10, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(4, 15, 30, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(2, 0, 2, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(3, 10, 15, 1, false, false),
            Action::acquire_lane_busy(5),
            Action::acquire_lane_busy(7),
            Action::acquire_lane_busy(6),
            Action::emit_remain_chunk(1, 31, 100, 1, false, false),
            Action::emit_remain_chunk(2, 30, 31, 1, false, false),
            Action::emit_eof(3),
            Action::emit_eof(4),
            Action::just_run(INT_MAX, false),
            Action::cache_miss_and_emit_first_chunk(6, 511, 1000, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(7, 100, 511, 1, false, false),
            Action::acquire_lane_busy(5),
            Action::acquire_lane_busy(8),
            Action::emit_remain_chunk(1, 1000, 1100, 1, false, false),
            Action::emit_remain_chunk(2, 1100, 1512, 1, false, false),
            Action::acquire_lane_busy(5),
            Action::acquire_lane_busy(8),
            Action::emit_eof(6),
            Action::emit_eof(7),
            Action::just_run(INT_MAX, false),
            Action::cache_miss_and_emit_first_chunk(5, 1512, 1512, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(8, 1512, 1600, 1, false, false),
            Action::emit_remain_chunk(1, 1600, 1700, 1, false, false),
            Action::emit_remain_chunk(2, 1700, 2000, 1, false, false),
            Action::emit_eof(1),
            Action::emit_eof(2),
            Action::emit_remain_chunk(5, 2000, 7000, 1, false, false),
            Action::emit_remain_chunk(8, 7000, 10000, 1, false, false),
            Action::emit_eof(8),
            Action::emit_eof(5),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 4, 1, state, pi_map1_func, pi_map2_func, 0.0, add_func, pre_passthrough_actions, {},
                   approx_validator_gen(3.1415926535));

    Actions pre_passthrough_actions2 = {
            Action::cache_miss_and_emit_first_chunk(1, 0, 100, 1, true, false),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework_force_populate(cache_mgr, 4, 1, state, mul2_func, plus1_func, 0.0, add_func,
                                  pre_passthrough_actions2, {}, eq_validator_gen(10000.00));

    test_framework_force_populate(cache_mgr, 4, 1, state, pi_map1_func, pi_map2_func, 0.0, add_func,
                                  pre_passthrough_actions, {}, approx_validator_gen(3.1415926535));

    Actions probe_actions = {
            Action::cache_hit(1),
            Action::cache_hit(2),
            Action::cache_hit(8),
            Action::cache_hit(7),
            Action::acquire_lane_busy(3),
            Action::acquire_lane_busy(4),
            Action::acquire_lane_busy(5),
            Action::acquire_lane_busy(6),
            Action::acquire_lane_skip(1),
            Action::acquire_lane_skip(2),
            Action::acquire_lane_skip(8),
            Action::acquire_lane_skip(7),
            Action::just_run(INT_MAX, false),
            Action::cache_hit(3),
            Action::cache_hit(4),
            Action::cache_hit(5),
            Action::cache_hit(6),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 4, 1, state, pi_map1_func, pi_map2_func, 0.0, add_func, probe_actions, {},
                   approx_validator_gen(3.1415926535));
}

TEST_F(QueryCacheTest, testMultiLaneEmitManyChunksPassthrough1) {
    Actions pre_passthrough_actions = {
            Action::cache_miss_and_emit_first_chunk(1, 2, 10, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(4, 15, 30, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(2, 0, 2, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(3, 10, 15, 1, false, false),
            Action::acquire_lane_busy(5),
            Action::acquire_lane_busy(7),
            Action::acquire_lane_busy(6),
    };
    Actions post_passthrough_actions = {
            Action::emit_remain_chunk(1, 31, 100, 1, false, false),
            Action::emit_remain_chunk(2, 30, 31, 1, false, false),
            Action::just_run(INT_MAX, false),
            Action::emit_remain_chunk(6, 511, 1000, 1, false, false),
            Action::emit_remain_chunk(7, 100, 511, 1, false, false),
            Action::emit_remain_chunk(1, 1000, 1100, 1, false, false),
            Action::emit_remain_chunk(2, 1100, 1512, 1, false, false),
            Action::just_run(INT_MAX, false),
            Action::emit_remain_chunk(5, 1512, 1512, 1, false, false),
            Action::emit_remain_chunk(6, 1512, 1600, 1, false, false),
            Action::emit_remain_chunk(1, 1600, 1700, 1, false, false),
            Action::emit_remain_chunk(2, 1700, 2000, 1, false, false),
            Action::emit_remain_chunk(5, 2000, 7000, 1, false, false),
            Action::emit_remain_chunk(6, 7000, 10000, 1, false, false),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 4, 1, state, pi_map1_func, pi_map2_func, 0.0, add_func, pre_passthrough_actions,
                   post_passthrough_actions, approx_validator_gen(3.1415926535));

    Actions probe_actions = {
            Action::cache_miss_and_emit_first_chunk(1, 200, 1000, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(4, 1500, 3000, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(2, 0, 200, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(3, 1000, 1500, 1, false, false),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 4, 1, state, pi_map1_func, pi_map2_func, 0.0, add_func, probe_actions, {},
                   approx_validator_gen(3.1415926535));

    Actions probe_actions2 = {
            Action::cache_hit(1), Action::cache_hit(2), Action::cache_hit(3),
            Action::cache_hit(4), Action::finish(),     Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 4, 1, state, pi_map1_func, pi_map2_func, 0.0, add_func, probe_actions2, {},
                   approx_validator_gen(3.1415926535));
}

TEST_F(QueryCacheTest, testMultiLaneEmitManyChunksPassthrough2) {
    Actions pre_passthrough_actions = {
            Action::cache_miss_and_emit_first_chunk(1, 2, 10, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(4, 15, 30, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(2, 0, 2, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(3, 10, 15, 1, false, false),
            Action::acquire_lane_busy(5),
            Action::acquire_lane_busy(7),
            Action::acquire_lane_busy(6),
            Action::emit_remain_chunk(1, 31, 100, 1, false, false),
            Action::emit_remain_chunk(2, 30, 31, 1, false, false),
            Action::emit_eof(1),
            Action::emit_eof(2),
            Action::just_run(INT_MAX, false),
    };
    Actions post_passthrough_actions = {
            Action::emit_remain_chunk(6, 511, 1000, 1, false, false),
            Action::emit_remain_chunk(7, 100, 511, 1, false, false),
            Action::emit_remain_chunk(3, 1000, 1100, 1, false, false),
            Action::emit_remain_chunk(4, 1100, 1512, 1, false, false),
            Action::just_run(INT_MAX, false),
            Action::emit_remain_chunk(5, 1512, 1512, 1, false, false),
            Action::emit_remain_chunk(6, 1512, 1600, 1, false, false),
            Action::emit_remain_chunk(3, 1600, 1700, 1, false, false),
            Action::emit_remain_chunk(4, 1700, 2000, 1, false, false),
            Action::emit_remain_chunk(5, 2000, 7000, 1, false, false),
            Action::emit_remain_chunk(6, 7000, 10000, 1, false, false),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 4, 1, state, mul2_func, plus1_func, 0.0, add_func, pre_passthrough_actions,
                   post_passthrough_actions, eq_validator_gen(10000'0000.0));

    Actions probe_actions = {
            Action::cache_hit(1),
            Action::cache_hit(2),
            Action::cache_miss_and_emit_first_chunk(4, 15, 30, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(3, 10, 15, 1, false, false),
            Action::just_run(INT_MAX, false),
            Action::cache_miss_and_emit_first_chunk(5, 100, 2000, 1, false, false),
            Action::emit_eof(3),
            Action::emit_eof(4),
            Action::emit_eof(5),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 4, 1, state, mul2_func, plus1_func, 0.0, add_func, probe_actions, {},
                   eq_validator_gen(4000'000.0));

    Actions probe_actions2 = {
            Action::cache_hit(1), Action::cache_hit(2), Action::cache_hit(3),
            Action::cache_hit(4), Action::finish(),     Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 4, 1, state, mul2_func, plus1_func, 0.0, add_func, probe_actions2, {},
                   eq_validator_gen(10000.0));
}

TEST_F(QueryCacheTest, testMultiLaneMissingEOF) {
    Actions pre_passthrough_actions = {
            Action::cache_miss_and_emit_first_chunk(1, 2, 10, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(4, 15, 30, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(2, 0, 2, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(3, 10, 15, 1, false, false),
            Action::acquire_lane_busy(5),
            Action::acquire_lane_busy(7),
            Action::acquire_lane_busy(6),
            Action::emit_remain_chunk(1, 31, 100, 1, false, false),
            Action::emit_remain_chunk(2, 30, 31, 1, false, false),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 4, 1, state, mul2_func, plus1_func, 0.0, add_func, pre_passthrough_actions, {},
                   eq_validator_gen(10000.0));

    Actions probe_actions = {
            Action::cache_hit(1),
            Action::cache_hit(2),
            Action::cache_hit(3),
            Action::cache_hit(4),
            Action::just_run(INT_MAX, false),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 4, 1, state, mul2_func, plus1_func, 0.0, add_func, probe_actions, {},
                   eq_validator_gen(10000.0));
}

TEST_F(QueryCacheTest, testMultiLaneFinishBeforeEOFHandled) {
    Actions pre_passthrough_actions = {
            Action::cache_miss_and_emit_first_chunk(1, 2, 10, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(4, 15, 30, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(2, 0, 2, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(3, 10, 15, 1, false, false),
            Action::acquire_lane_busy(5),
            Action::acquire_lane_busy(7),
            Action::acquire_lane_busy(6),
            Action::emit_remain_chunk(1, 31, 100, 1, false, false),
            Action::emit_remain_chunk(2, 30, 31, 1, false, false),
            Action::emit_eof(1),
            Action::emit_eof(2),
            Action::emit_eof(3),
            Action::emit_eof(4),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 4, 1, state, mul2_func, plus1_func, 0.0, add_func, pre_passthrough_actions, {},
                   eq_validator_gen(10000.0));

    Actions probe_actions = {
            Action::cache_hit(1),
            Action::cache_hit(2),
            Action::cache_hit(3),
            Action::cache_hit(4),
            Action::just_run(INT_MAX, false),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 4, 1, state, mul2_func, plus1_func, 0.0, add_func, probe_actions, {},
                   eq_validator_gen(10000.0));
}

TEST_F(QueryCacheTest, testMultiLaneCacheMiss) {
    Actions pre_passthrough_actions = {
            Action::cache_miss_and_emit_first_chunk(1, 0, 33, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(9, 33, 66, 1, false, false),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 4, 1, state, mul2_func, plus1_func, 0.0, add_func, pre_passthrough_actions, {},
                   eq_validator_gen(4356.0));

    Actions probe_actions = {
            Action::cache_hit(1),
            Action::cache_miss_and_emit_first_chunk(9, 33, 66, 1, false, false),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 4, 1, state, mul2_func, plus1_func, 0.0, add_func, probe_actions, {},
                   eq_validator_gen(4356.0));
}

TEST_F(QueryCacheTest, testMultiLanePassthroughWithEmptyTabletNotCached) {
    Actions actions = {
            Action::cache_miss_and_emit_first_chunk(1, 0, 0, 1, true, false),
            Action::cache_miss_and_emit_first_chunk(2, 0, 100, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(3, 0, 0, 1, true, false),
            Action::cache_miss_and_emit_first_chunk(4, 0, 0, 1, true, false),
            Action::just_run(INT_MAX, false),
            Action::emit_remain_chunk(2, 100, 150, 1, false, false),
            Action::emit_remain_chunk(5, 150, 151, 1, false, false),
            Action::emit_remain_chunk(6, 151, 152, 1, false, false),
            Action::emit_remain_chunk(7, 152, 153, 1, false, false),
            Action::emit_remain_chunk(8, 153, 154, 1, false, false),
            Action::emit_remain_chunk(2, 154, 200, 1, false, false),
            Action::emit_remain_chunk(5, 200, 300, 1, false, false),
            Action::emit_remain_chunk(6, 300, 400, 1, false, false),
            Action::emit_remain_chunk(7, 400, 600, 1, false, false),
            Action::emit_remain_chunk(8, 600, 700, 1, false, false),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework_force_populate_and_passthrough(cache_mgr, 4, 1, state, mul2_func, plus1_func, 0.0, add_func, actions,
                                                  {}, eq_validator_gen(490000.0));

    Actions probe_actions = {
            Action::cache_miss_and_emit_first_chunk(1, 0, 0, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(3, 0, 0, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(4, 0, 0, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(2, 0, 99, 1, false, false),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework_force_populate_and_passthrough(cache_mgr, 4, 1, state, mul2_func, plus1_func, 0.0, add_func,
                                                  probe_actions, {}, eq_validator_gen(9801));
}

TEST_F(QueryCacheTest, testMultiLanePassthroughWithEmptyTabletCached) {
    Actions actions = {
            Action::cache_miss_and_emit_first_chunk(1, 0, 0, 1, true, false),
            Action::cache_miss_and_emit_first_chunk(3, 0, 0, 1, true, false),
            Action::cache_miss_and_emit_first_chunk(4, 0, 0, 1, true, false),
            Action::just_run(INT_MAX, false),
            Action::emit_remain_chunk(2, 0, 100, 1, false, false),
            Action::emit_remain_chunk(2, 100, 150, 1, false, false),
            Action::emit_remain_chunk(5, 150, 151, 1, false, false),
            Action::emit_remain_chunk(6, 151, 152, 1, false, false),
            Action::emit_remain_chunk(7, 152, 153, 1, false, false),
            Action::emit_remain_chunk(8, 153, 154, 1, false, false),
            Action::emit_remain_chunk(2, 154, 200, 1, false, false),
            Action::emit_remain_chunk(5, 200, 300, 1, false, false),
            Action::emit_remain_chunk(6, 300, 400, 1, false, false),
            Action::emit_remain_chunk(7, 400, 600, 1, false, false),
            Action::emit_remain_chunk(8, 600, 700, 1, false, false),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework_force_populate_and_passthrough(cache_mgr, 4, 1, state, mul2_func, plus1_func, 0.0, add_func, actions,
                                                  {}, eq_validator_gen(490000.0));

    Actions probe_actions = {
            Action::cache_miss_and_emit_first_chunk(4, 0, 0, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(1, 0, 0, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(3, 0, 0, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(2, 0, 99, 1, false, false),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework_force_populate_and_passthrough(cache_mgr, 4, 1, state, mul2_func, plus1_func, 0.0, add_func,
                                                  probe_actions, {}, eq_validator_gen(9801));
}

TEST_F(QueryCacheTest, testMultiLanePassthroughWithoutEmptyTablet) {
    Actions actions = {
            Action::cache_miss_and_emit_first_chunk(1, 0, 10, 1, true, false),
            Action::cache_miss_and_emit_first_chunk(2, 10, 30, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(3, 30, 50, 1, true, false),
            Action::cache_miss_and_emit_first_chunk(4, 50, 100, 1, true, false),
            Action::just_run(INT_MAX, false),
            Action::emit_remain_chunk(2, 100, 150, 1, false, false),
            Action::emit_remain_chunk(5, 150, 151, 1, false, false),
            Action::emit_remain_chunk(6, 151, 152, 1, false, false),
            Action::emit_remain_chunk(7, 152, 153, 1, false, false),
            Action::emit_remain_chunk(8, 153, 154, 1, false, false),
            Action::emit_remain_chunk(2, 154, 200, 1, false, false),
            Action::emit_remain_chunk(5, 200, 300, 1, false, false),
            Action::emit_remain_chunk(6, 300, 400, 1, false, false),
            Action::emit_remain_chunk(7, 400, 600, 1, false, false),
            Action::emit_remain_chunk(8, 600, 700, 1, false, false),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework_force_populate_and_passthrough(cache_mgr, 4, 1, state, mul2_func, plus1_func, 0.0, add_func, actions,
                                                  {}, eq_validator_gen(490000.0));

    Actions probe_actions = {
            Action::cache_miss_and_emit_first_chunk(1, 0, 10, 1, true, false),
            Action::cache_miss_and_emit_first_chunk(2, 10, 30, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(4, 50, 100, 1, true, false),
            Action::cache_miss_and_emit_first_chunk(3, 30, 50, 1, true, false),
            Action::just_run(INT_MAX, false),
            Action::emit_remain_chunk(5, 100, 140, 1, false, false),
            Action::emit_remain_chunk(6, 140, 141, 1, false, false),
            Action::emit_remain_chunk(7, 141, 200, 1, false, false),
            Action::emit_remain_chunk(8, 200, 300, 1, false, false),
            Action::emit_remain_chunk(2, 300, 369, 1, false, false),
            Action::emit_remain_chunk(5, 369, 700, 1, false, false),
            Action::emit_remain_chunk(6, 700, 1000, 1, false, false),
            Action::emit_remain_chunk(7, 1000, 1111, 1, false, false),
            Action::emit_remain_chunk(8, 1111, 1121, 1, false, false),
            Action::emit_remain_chunk(2, 1121, 2000, 1, false, false),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework_force_populate_and_passthrough(cache_mgr, 4, 1, state, mul2_func, plus1_func, 0.0, add_func,
                                                  probe_actions, {}, eq_validator_gen(4000000));
}

TEST_F(QueryCacheTest, testOneTabletPartialHit) {
    Actions actions = {
            Action::cache_miss_and_emit_first_chunk(1, 0, 10, 1, false, false),
            Action::emit_remain_chunk(1, 10, 1000, 1, true, false),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 4, 1, state, mul2_func, plus1_func, 0.0, add_func, actions, {},
                   eq_validator_gen(1000'000.0));

    Actions probe_partial_hit_actions = {
            Action::cache_partial_hit_and_emit_first_chunk(1, 1000, 1333, 1, false, false, 2),
            Action::emit_remain_chunk(1, 1333, 2000, 1, true, false),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 4, 1, state, mul2_func, plus1_func, 0.0, add_func, probe_partial_hit_actions, {},
                   eq_validator_gen(4000'000.0));

    Actions probe_total_hit_actions = {
            Action::cache_hit(1, 2),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };

    test_framework(cache_mgr, 4, 1, state, mul2_func, plus1_func, 0.0, add_func, probe_total_hit_actions, {},
                   eq_validator_gen(4000'000.0));

    Actions probe_partial_hit_actions2 = {
            Action::cache_partial_hit_and_emit_first_chunk(1, 2000, 2001, 1, false, false, 4),
            Action::emit_remain_chunk(1, 2001, 2999, 1, true, false),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 4, 1, state, mul2_func, plus1_func, 0.0, add_func, probe_partial_hit_actions2, {},
                   eq_validator_gen(8994'001.0));

    Actions probe_total_hit_actions2 = {
            Action::cache_hit(1, 4),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 4, 1, state, mul2_func, plus1_func, 0.0, add_func, probe_total_hit_actions2, {},
                   eq_validator_gen(8994'001.0));

    Actions probe_miss_actions2 = {
            Action::cache_miss_and_emit_first_chunk(1, 0, 99, 1, true, false, 3),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 4, 1, state, mul2_func, plus1_func, 0.0, add_func, probe_miss_actions2, {},
                   eq_validator_gen(9801.0));
}

TEST_F(QueryCacheTest, testPartialHit) {
    Actions actions = {
            Action::cache_miss_and_emit_first_chunk(4, 34, 35, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(1, 0, 3, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(2, 3, 11, 1, false, false),
            Action::cache_miss_and_emit_first_chunk(3, 11, 34, 1, false, false),
            Action::emit_remain_chunk(1, 35, 500, 1, true, false),
            Action::emit_eof(2),
            Action::emit_eof(3),
            Action::emit_eof(4),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 4, 1, state, mul2_func, plus1_func, 0.0, add_func, actions, {},
                   eq_validator_gen(250000.0));

    Actions probe_partial_hit_actions = {
            Action::cache_partial_hit_and_emit_first_chunk(1, 500, 510, 1, false, false, 2),
            Action::cache_miss_and_emit_first_chunk(5, 3, 35, 1, false, false, 2),
            Action::cache_miss_and_emit_first_chunk(6, 510, 600, 1, false, false, 2),
            Action::emit_remain_chunk(1, 600, 601, 1, false, false),
            Action::emit_remain_chunk(1, 601, 605, 1, false, false),
            Action::emit_remain_chunk(5, 605, 799, 1, true, false),
            Action::emit_eof(1),
            Action::emit_eof(6),
            Action::cache_miss_and_emit_first_chunk(7, 799, 800, 1, false, false, 2),
            Action::acquire_lane_busy(8),
            Action::just_run(INT_MAX, false),
            Action::cache_miss_and_emit_first_chunk(8, 800, 801, 1, true, false, 2),
            Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 4, 1, state, mul2_func, plus1_func, 0.0, add_func, probe_partial_hit_actions, {},
                   eq_validator_gen(801.0 * 801.0));

    Actions probe_total_hit_actions = {
            Action::cache_hit(1, 2),         Action::cache_hit(5, 2),
            Action::cache_hit(6, 2),         Action::cache_hit(7, 2),
            Action::acquire_lane_busy(8),    Action::just_run(INT_MAX, false),
            Action::cache_hit(8, 2),         Action::finish(),
            Action::just_run(INT_MAX, true),
    };
    test_framework(cache_mgr, 4, 1, state, mul2_func, plus1_func, 0.0, add_func, probe_total_hit_actions, {},
                   eq_validator_gen(801.0 * 801.0));
}

TEST_F(QueryCacheTest, testTicketChecker) {
    auto test_func1 = [](int64_t id, int n) {
        auto ticket_checker = std::make_shared<query_cache::TicketChecker>();
        for (auto i = 0; i < n; ++i) {
            ticket_checker->enter(1L, i + 1 == n);
            ASSERT_EQ(ticket_checker->are_all_ready(id), i + 1 == n);
        }
        for (auto i = 0; i < n; ++i) {
            ASSERT_EQ(ticket_checker->leave(1L), i + 1 == n);
        }
    };

    test_func1(1L, 1);
    test_func1(1L, 10);
    test_func1(1L, 100);

    auto test_func2 = [](int64_t id, int n) {
        auto ticket_checker = std::make_shared<query_cache::TicketChecker>();
        for (auto i = 0; i < n; ++i) {
            ticket_checker->enter(1L, i + 1 == n);
            ASSERT_EQ(ticket_checker->are_all_ready(id), i + 1 == n);
            ASSERT_EQ(ticket_checker->leave(1L), i + 1 == n);
        }
    };

    test_func2(1L, 1);
    test_func2(1L, 10);
    test_func2(1L, 100);
}

} // namespace starrocks
