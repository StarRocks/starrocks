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

#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "base/metrics.h"
#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "common/config_ingest_fwd.h"
#include "compute_env/load/load_stream_mgr.h"
#include "compute_env/load/stream_load_context.h"
#include "compute_env/load/stream_load_metrics.h"
#include "compute_env/load/stream_load_pipe.h"
#include "data_workflows/load/stream_load/stream_load_executor.h"
#include "exec/exec_env.h"
#include "exec/pipeline/exec_state_reporter.h"
#include "exec/pipeline/fragment_execution_params.h"
#include "exec/runtime/fragment_context.h"
#include "exec/runtime/query_context.h"
#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "librdkafka/rdkafka.h"
#include "librdkafka/rdkafka_mock.h"
#include "librdkafka/rdkafkacpp.h"
#include "orchestration/fragment_executor.h"
#include "orchestration/routine_load_task_executor.h"
#include "orchestration/stream_load_orchestrator.h"
#include "runtime/kafka_scan_range_report.h"
#include "runtime/runtime_metrics.h"
#include "runtime/runtime_state.h"

namespace starrocks::orchestration {

namespace {

const std::string kTopic = "routine_load_kafka_scan_range_test";
const std::string kGroupId = "routine-load-kafka-scan-range-test";
// the time a commit of consumer group offsets waits for the broker, as the RPC of the FE allows it
const int kCommitTimeoutMs = 10000;

TUniqueId make_load_id(int64_t lo) {
    TUniqueId id;
    id.__set_hi(67025);
    id.__set_lo(lo);
    return id;
}

TScanRangeParams to_scan_range_params(const TBrokerScanRange& broker_scan_range) {
    TScanRangeParams params;
    params.scan_range.__set_broker_scan_range(broker_scan_range);
    return params;
}

int64_t elapsed_ms(std::chrono::steady_clock::time_point since) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - since).count();
}

// Polls the condition for at most 30s.
bool wait_until(const std::function<bool()>& condition) {
    for (int i = 0; i < 3000; ++i) {
        if (condition()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return condition();
}

// What a consumer holds of its commits that timed out: the queues of their results, and how many of the queues have
// their result already. Reading the queues leaves their results in them.
std::pair<size_t, size_t> commit_results(KafkaDataConsumer* consumer) {
    std::unique_lock<std::mutex> l(consumer->_lock);
    size_t completed = 0;
    for (rd_kafka_queue_t* queue : consumer->_commit_result_queues) {
        if (rd_kafka_queue_length(queue) > 0) {
            ++completed;
        }
    }
    return {consumer->_commit_result_queues.size(), completed};
}

// Reads a pipe up to its end, as the file scanner of a Kafka scan range does: reading the pipe while it is empty and
// not finished returns TimedOut instead of blocking.
StatusOr<std::string> read_all(StreamLoadPipe* pipe) {
    std::string data;
    char buf[1024];
    for (int i = 0; i < 3000; ++i) {
        size_t size = sizeof(buf);
        bool eof = false;
        Status st = pipe->read(reinterpret_cast<uint8_t*>(buf), &size, &eof);
        if (st.is_time_out()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }
        RETURN_IF_ERROR(st);
        if (eof) {
            return data;
        }
        data.append(buf, size);
    }
    return Status::TimedOut("the pipe did not reach its end");
}

// Keeps scan-range consumptions queued until the test has inspected their task count.
class ThreadPoolBlocker {
public:
    ~ThreadPoolBlocker() { release(); }

    Status block(ThreadPool* pool) {
        RETURN_IF_ERROR(pool->update_max_threads(1));
        auto released = _released.get_future().share();
        return pool->submit_func([released] { released.wait(); });
    }

    void release() {
        if (!_did_release) {
            _did_release = true;
            _released.set_value();
        }
    }

private:
    std::promise<void> _released;
    bool _did_release = false;
};

} // namespace

// The Kafka scan ranges of a routine load round in the coordinated mode, on a topic of librdkafka's in-process mock
// cluster: RoutineLoadTaskExecutor prepares their pipes and consumes the partitions into them, FragmentExecutor hands
// the pipes over to the fragment of the round, and the FE commits the consumer group offsets through the executor
// once the round's transaction commits.
class RoutineLoadKafkaScanRangeTest : public testing::Test {
protected:
    void SetUp() override {
        _previous_task_count_registry = RuntimeMetrics::instance()->routine_load_task_count.registry();
        std::string error;
        std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
        ASSERT_EQ(RdKafka::Conf::CONF_OK, conf->set("test.mock.num.brokers", "1", error));
        ASSERT_EQ(RdKafka::Conf::CONF_OK, conf->set("log_level", "0", error));
        ASSERT_EQ(RdKafka::Conf::CONF_OK, conf->set("linger.ms", "0", error));
        _producer.reset(RdKafka::Producer::create(conf.get(), error));
        ASSERT_NE(nullptr, _producer) << error;
        auto* cluster = rd_kafka_handle_mock_cluster(_producer->c_ptr());
        ASSERT_NE(nullptr, cluster);
        ASSERT_EQ(RD_KAFKA_RESP_ERR_NO_ERROR, rd_kafka_mock_topic_create(cluster, kTopic.c_str(), 2, 1));
        _brokers = rd_kafka_mock_cluster_bootstraps(cluster);

        _exec_env = ExecEnv::GetInstance();
        ASSERT_NE(nullptr, _exec_env->load_stream_mgr());
        _stream_load_orchestrator = std::make_unique<StreamLoadOrchestrator>(_exec_env, nullptr);
        _executor = std::make_unique<RoutineLoadTaskExecutor>(_exec_env, _stream_load_orchestrator.get(),
                                                              &_stream_load_executor);
        ASSERT_OK(_executor->init(&_metrics));
    }

    void TearDown() override {
        // Waits for the consumptions that still run, before the mock cluster goes away with the producer.
        if (_executor != nullptr) {
            _executor->stop();
            _executor.reset();
        }
        // This gauge is a process singleton: registering it in the fixture temporarily hides it from its original
        // registry, whose hook stays registered. Restore it so later tests retain the process metric.
        _metrics.deregister_hook("routine_load_task_count");
        auto* task_count = &RuntimeMetrics::instance()->routine_load_task_count;
        if (_previous_task_count_registry != nullptr) {
            EXPECT_TRUE(_previous_task_count_registry->register_metric("routine_load_task_count", task_count));
        } else {
            task_count->hide();
        }
    }

    LoadStreamMgr* load_stream_mgr() const { return _exec_env->load_stream_mgr(); }

    std::string task_count() {
        _metrics.trigger_hook();
        return _metrics.get_metric("routine_load_task_count")->to_string();
    }

    RdKafka::ErrorCode produce(int32_t partition, const std::vector<std::string>& payloads) {
        for (const auto& payload : payloads) {
            auto error = _producer->produce(kTopic, partition, RdKafka::Producer::RK_MSG_COPY,
                                            const_cast<char*>(payload.data()), payload.size(), nullptr, 0, 0, nullptr);
            if (error != RdKafka::ERR_NO_ERROR) {
                return error;
            }
        }
        return _producer->flush(10000);
    }

    TBrokerScanRange make_scan_range(const TUniqueId& load_id, const std::map<int32_t, int64_t>& begin_offsets,
                                     const std::map<std::string, std::string>& properties) const {
        TBrokerRangeDesc range;
        range.__set_file_type(TFileType::FILE_STREAM);
        range.__set_format_type(TFileFormatType::FORMAT_CSV_PLAIN);
        range.__set_splittable(false);
        range.__set_path("");
        range.__set_start_offset(0);
        range.__set_size(-1);
        range.__set_load_id(load_id);

        TKafkaScanRange kafka_scan_range;
        kafka_scan_range.__set_job_id(42);
        kafka_scan_range.__set_brokers(_brokers);
        kafka_scan_range.__set_topic(kTopic);
        kafka_scan_range.__set_partition_begin_offset(begin_offsets);
        kafka_scan_range.__set_properties(properties);
        // an upper bound only: the window ends once its partitions have been at their end for idle_end_ms
        kafka_scan_range.__set_max_consume_ms(30000);
        kafka_scan_range.__set_max_consume_bytes(64 * 1024 * 1024);
        kafka_scan_range.__set_idle_end_ms(200);

        TBrokerScanRange scan_range;
        scan_range.__set_ranges({range});
        scan_range.params.__set_column_separator(',');
        scan_range.params.__set_row_delimiter('\n');
        scan_range.params.__set_db_name("db");
        scan_range.params.__set_table_name("tbl");
        scan_range.params.__set_label("round_label");
        scan_range.params.__set_txn_id(7);
        scan_range.__set_kafka_scan_range(kafka_scan_range);
        return scan_range;
    }

    TBrokerScanRange make_scan_range(const TUniqueId& load_id, const std::map<int32_t, int64_t>& begin_offsets) const {
        return make_scan_range(load_id, begin_offsets, {{"group.id", kGroupId}, {"fetch.wait.max.ms", "10"}});
    }

    PKafkaCommitOffsetProxyRequest make_commit_request(const std::string& group_id) const {
        std::map<std::string, std::string> properties;
        if (!group_id.empty()) {
            properties["group.id"] = group_id;
        }
        return make_commit_request_with_properties(properties);
    }

    PKafkaCommitOffsetProxyRequest make_commit_request_with_properties(
            const std::map<std::string, std::string>& properties) const {
        PKafkaCommitOffsetProxyRequest request;
        auto* kafka_info = request.mutable_kafka_info();
        kafka_info->set_brokers(_brokers);
        kafka_info->set_topic(kTopic);
        for (const auto& [key, value] : properties) {
            auto* property = kafka_info->add_properties();
            property->set_key(key);
            property->set_val(value);
        }
        return request;
    }

    // The coordinator, the only broker of the cluster, answers the next OffsetCommit request delay_ms late.
    rd_kafka_resp_err_t delay_next_offset_commit(int delay_ms) const {
        auto* cluster = rd_kafka_handle_mock_cluster(_producer->c_ptr());
        // 8 is the ApiKey of OffsetCommit
        return rd_kafka_mock_broker_push_request_error_rtts(cluster, 1, 8, 1, RD_KAFKA_RESP_ERR_NO_ERROR, delay_ms);
    }

    // The offset the consumer group resumes from in a partition, as the cluster holds it.
    int64_t committed_offset(const std::string& group_id, int32_t partition) const {
        std::string error;
        std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
        if (conf->set("bootstrap.servers", _brokers, error) != RdKafka::Conf::CONF_OK ||
            conf->set("group.id", group_id, error) != RdKafka::Conf::CONF_OK) {
            return -2;
        }
        std::unique_ptr<RdKafka::KafkaConsumer> consumer(RdKafka::KafkaConsumer::create(conf.get(), error));
        if (consumer == nullptr) {
            return -2;
        }
        std::vector<RdKafka::TopicPartition*> partitions{RdKafka::TopicPartition::create(kTopic, partition)};
        auto err = consumer->committed(partitions, 10000);
        int64_t offset = err == RdKafka::ERR_NO_ERROR ? partitions[0]->offset() : -2;
        for (auto* topic_partition : partitions) {
            delete topic_partition;
        }
        consumer->close();
        return offset;
    }

    // The parameters of a fragment instance. UnifiedExecPlanFragmentParams requires every instance to carry its backend
    // number, pipeline dop and sender id, and checks them in debug builds.
    static TExecPlanFragmentParams fragment_params(const TPlanFragmentExecParams& exec_params) {
        TExecPlanFragmentParams params;
        params.__set_backend_num(0);
        params.__set_pipeline_dop(1);
        params.__set_params(exec_params);
        params.params.__set_sender_id(0);
        return params;
    }

    // The parameters of a fragment of a round, as the FE deploys it.
    static TExecPlanFragmentParams round_fragment_params(const TPlanFragmentExecParams& exec_params) {
        TExecPlanFragmentParams params = fragment_params(exec_params);
        TQueryOptions query_options;
        query_options.__set_query_type(TQueryType::LOAD);
        query_options.__set_load_job_type(TLoadJobType::ROUTINE_LOAD);
        params.__set_query_options(query_options);
        return params;
    }

    // What FragmentExecutor::prepare() does, in its order, for the stream load pipes of a fragment of a round, the rows
    // it loads and its Kafka scan ranges.
    Status prepare_round_fragment(FragmentExecutor* fragment_executor,
                                  const pipeline::UnifiedExecPlanFragmentParams& request) const {
        RETURN_IF_ERROR(fragment_executor->_prepare_stream_load_pipe(_exec_env, request));
        fragment_executor->_prepare_routine_load_rows_count(request);
        return fragment_executor->_prepare_kafka_scan_ranges(request);
    }

    // Sets up the fragment of a round as FragmentExecutor::prepare() has when it comes to the Kafka scan ranges. The
    // query and the fragment get the execution services of the process as in prepare(), since cancelling the fragment
    // looks up the driver executor through them.
    void prepare_fragment(FragmentExecutor* fragment_executor, const pipeline::QueryContextPtr& query_ctx) const {
        const QueryExecutionServices* services = &_exec_env->query_execution_services();
        query_ctx->set_query_execution_services(services);
        fragment_executor->_query_ctx_hold = query_ctx;
        fragment_executor->_query_ctx = query_ctx.get();
        fragment_executor->_fragment_ctx = std::make_shared<pipeline::FragmentContext>();
        TUniqueId fragment_id;
        TQueryOptions query_options;
        TQueryGlobals query_globals;
        auto state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, services, _exec_env);
        TUniqueId query_id;
        state->init_mem_trackers(query_id);
        state->set_query_ctx(query_ctx.get(), &query_ctx->query_runtime_state(), query_ctx->object_pool());
        fragment_executor->_fragment_ctx->set_runtime_state(std::move(state));
        fragment_executor->set_routine_load_task_executor(_executor.get());
    }

    std::string _brokers;
    std::unique_ptr<RdKafka::Producer> _producer;
    ExecEnv* _exec_env = nullptr;
    StreamLoadExecutor _stream_load_executor;
    MetricRegistry* _previous_task_count_registry = nullptr;
    MetricRegistry _metrics{"routine_load_kafka_scan_range_test"};
    std::unique_ptr<StreamLoadOrchestrator> _stream_load_orchestrator;
    std::unique_ptr<RoutineLoadTaskExecutor> _executor;
};

TEST_F(RoutineLoadKafkaScanRangeTest, commit_offsets_needs_the_kafka_info) {
    PKafkaCommitOffsetProxyRequest request;
    request.add_partition_ids(0);
    request.add_offsets(1);
    std::string group_id;
    std::vector<std::shared_ptr<DataConsumer>> evicted;
    Status st = _executor->commit_kafka_offsets(request, kCommitTimeoutMs, &group_id, &evicted);
    EXPECT_TRUE(st.is_invalid_argument()) << st;
}

TEST_F(RoutineLoadKafkaScanRangeTest, commit_offsets_pairs_each_partition_with_an_offset) {
    auto request = make_commit_request(kGroupId);
    request.add_partition_ids(0);
    request.add_partition_ids(1);
    request.add_offsets(1);
    std::string group_id;
    std::vector<std::shared_ptr<DataConsumer>> evicted;
    Status st = _executor->commit_kafka_offsets(request, kCommitTimeoutMs, &group_id, &evicted);
    EXPECT_TRUE(st.is_invalid_argument()) << st;
    EXPECT_NE(std::string::npos, st.message().find("2 partitions but 1 offsets")) << st;
}

// A round that consumed nothing has no offset to commit, so no consumer is needed. The group id comes back for the
// logs of the FE either way.
TEST_F(RoutineLoadKafkaScanRangeTest, commit_offsets_without_partitions_commits_nothing) {
    std::string group_id;
    std::vector<std::shared_ptr<DataConsumer>> evicted;
    ASSERT_OK(_executor->commit_kafka_offsets(make_commit_request(kGroupId), kCommitTimeoutMs, &group_id, &evicted));
    EXPECT_EQ(kGroupId, group_id);
    ASSERT_OK(_executor->commit_kafka_offsets(make_commit_request(""), kCommitTimeoutMs, &group_id, &evicted));
    EXPECT_EQ("unknown", group_id);
}

// The request carries the offset of the last consumed message of each partition, and the consumer group then resumes
// after it.
TEST_F(RoutineLoadKafkaScanRangeTest, commit_offsets_moves_the_group_past_the_consumed_messages) {
    ASSERT_EQ(RdKafka::ERR_NO_ERROR, produce(0, {"a,1", "b,2", "c,3"}));
    auto request = make_commit_request(kGroupId);
    request.add_partition_ids(0);
    request.add_offsets(2);
    std::string group_id;
    std::vector<std::shared_ptr<DataConsumer>> evicted;
    ASSERT_OK(_executor->commit_kafka_offsets(request, kCommitTimeoutMs, &group_id, &evicted));
    EXPECT_EQ(kGroupId, group_id);
    EXPECT_EQ(3, committed_offset(kGroupId, 0));
}

// A commit waits for the broker at most the time its request allows, so that a coordinator that does not answer holds
// the shared RPC thread no longer than the FE waits for it. The commit can still complete later.
TEST_F(RoutineLoadKafkaScanRangeTest, commit_offsets_waits_for_the_broker_at_most_its_timeout) {
    ASSERT_EQ(RdKafka::ERR_NO_ERROR, produce(0, {"a,1"}));
    auto* cluster = rd_kafka_handle_mock_cluster(_producer->c_ptr());
    ASSERT_NE(nullptr, cluster);
    // every answer of the broker, the one that finds the coordinator included, comes 5s late
    ASSERT_EQ(RD_KAFKA_RESP_ERR_NO_ERROR, rd_kafka_mock_broker_set_rtt(cluster, -1, 5000));
    auto request = make_commit_request(kGroupId);
    request.add_partition_ids(0);
    request.add_offsets(0);
    std::string group_id;
    std::vector<std::shared_ptr<DataConsumer>> evicted;

    const auto started_at = std::chrono::steady_clock::now();
    Status st = _executor->commit_kafka_offsets(request, 500, &group_id, &evicted);
    const auto waited = std::chrono::steady_clock::now() - started_at;
    EXPECT_TRUE(st.is_time_out()) << st;
    EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(waited).count(), 3000);
    ASSERT_EQ(RD_KAFKA_RESP_ERR_NO_ERROR, rd_kafka_mock_broker_set_rtt(cluster, -1, 0));
}

// A consumer fetches nothing for a new assignment while a commit of it is still on its way. The pool keeps the
// consumer of a commit that timed out apart, so that the next round of the job, which consumes with the properties the
// FE commits with, does not wait for the commit before it reads the messages already there.
TEST_F(RoutineLoadKafkaScanRangeTest, consumption_does_not_wait_for_a_commit_that_timed_out) {
    ASSERT_EQ(RdKafka::ERR_NO_ERROR, produce(0, {"a,1", "b,2"}));
    const std::map<std::string, std::string> properties{{"group.id", kGroupId}, {"fetch.wait.max.ms", "10"}};
    auto request = make_commit_request_with_properties(properties);
    request.add_partition_ids(0);
    request.add_offsets(0);
    std::string group_id;
    std::vector<std::shared_ptr<DataConsumer>> evicted;
    // the first commit finds the coordinator, and its consumer goes back to the pool
    ASSERT_OK(_executor->commit_kafka_offsets(request, kCommitTimeoutMs, &group_id, &evicted));
    ASSERT_EQ(RD_KAFKA_RESP_ERR_NO_ERROR, delay_next_offset_commit(5000));
    request.set_offsets(0, 1);
    Status st = _executor->commit_kafka_offsets(request, 300, &group_id, &evicted);
    ASSERT_TRUE(st.is_time_out()) << st;
    EXPECT_EQ(1u, _executor->_data_consumer_pool._committing.size());
    EXPECT_TRUE(_executor->_data_consumer_pool._pool.empty());

    const TUniqueId load_id = make_load_id(27);
    auto ctx_or = _executor->prepare_kafka_scan_range(make_scan_range(load_id, {{0, 0}}, properties));
    ASSERT_OK(ctx_or.status());
    std::shared_ptr<StreamLoadContext> ctx(ctx_or.value(), StreamLoadContext::release);
    auto pipe = load_stream_mgr()->get(load_id);
    ASSERT_NE(nullptr, pipe);
    const auto started_at = std::chrono::steady_clock::now();
    ASSERT_OK(_executor->start_kafka_scan_range(ctx.get(), std::make_shared<KafkaScanRangeReport>(), nullptr));
    ASSERT_TRUE(wait_until([&] { return pipe->finished(); }));
    // The consumption ends 200ms after it reached the end of the partition, long before the commit completes. With the
    // consumer of the commit, it would read nothing until then.
    EXPECT_LT(elapsed_ms(started_at), 3000);
    auto data = read_all(pipe.get());
    ASSERT_OK(data.status());
    EXPECT_EQ("a,1\nb,2\n", data.value());
}

// The later commits of the group share the consumer of a commit that timed out instead of taking one each. They reach
// the coordinator after the commit on its way, and the last one sets where the group resumes. Once they have all
// completed and no commit uses the consumer, the background worker pools it again for the rounds of the job.
TEST_F(RoutineLoadKafkaScanRangeTest, later_commits_share_the_consumer_of_a_commit_that_timed_out) {
    ASSERT_EQ(RdKafka::ERR_NO_ERROR, produce(0, {"a,1", "b,2", "c,3"}));
    auto request = make_commit_request(kGroupId);
    request.add_partition_ids(0);
    request.add_offsets(0);
    std::string group_id;
    std::vector<std::shared_ptr<DataConsumer>> evicted;
    ASSERT_OK(_executor->commit_kafka_offsets(request, kCommitTimeoutMs, &group_id, &evicted));
    ASSERT_EQ(RD_KAFKA_RESP_ERR_NO_ERROR, delay_next_offset_commit(2000));
    request.set_offsets(0, 1);
    Status st = _executor->commit_kafka_offsets(request, 300, &group_id, &evicted);
    ASSERT_TRUE(st.is_time_out()) << st;
    auto& pool = _executor->_data_consumer_pool;
    ASSERT_EQ(1u, pool._committing.size());
    const UniqueId kept_apart = pool._committing.front()->id();

    request.set_offsets(0, 2);
    ASSERT_OK(_executor->commit_kafka_offsets(request, kCommitTimeoutMs, &group_id, &evicted));
    EXPECT_EQ(3, committed_offset(kGroupId, 0));
    ASSERT_EQ(1u, pool._committing.size());
    EXPECT_EQ(kept_apart, pool._committing.front()->id());
    EXPECT_FALSE(pool._committing.front()->has_commit_in_flight());

    pool._clean_idle_consumer_bg();
    EXPECT_TRUE(pool._committing.empty());
    ASSERT_EQ(1u, pool._pool.size());
    EXPECT_EQ(kept_apart, pool._pool.front()->id());
    EXPECT_TRUE(evicted.empty());
}

// While the coordinator answers later than the commits wait and the rounds go on, a commit of the group may always be
// using the consumer kept apart. Each commit that returns the consumer frees the results of the commits that have
// completed, so the consumer holds the results of the commits still on their way only, not one for every commit that
// timed out.
TEST_F(RoutineLoadKafkaScanRangeTest, commits_that_keep_using_a_consumer_free_the_results_of_the_completed_ones) {
    ASSERT_EQ(RdKafka::ERR_NO_ERROR, produce(0, {"a,1", "b,2", "c,3", "d,4", "e,5"}));
    auto request = make_commit_request(kGroupId);
    request.add_partition_ids(0);
    request.add_offsets(0);
    std::string group_id;
    std::vector<std::shared_ptr<DataConsumer>> evicted;
    ASSERT_OK(_executor->commit_kafka_offsets(request, kCommitTimeoutMs, &group_id, &evicted));
    for (int i = 0; i < 3; ++i) {
        ASSERT_EQ(RD_KAFKA_RESP_ERR_NO_ERROR, delay_next_offset_commit(1000));
    }
    auto& pool = _executor->_data_consumer_pool;
    // stands for a commit of the group that still uses the consumer kept apart
    std::shared_ptr<DataConsumer> in_use;
    for (int64_t offset = 1; offset <= 3; ++offset) {
        request.set_offsets(0, offset);
        Status st = _executor->commit_kafka_offsets(request, 50, &group_id, &evicted);
        ASSERT_TRUE(st.is_time_out()) << st;
        ASSERT_EQ(1u, pool._committing.size());
        if (in_use == nullptr) {
            in_use = pool._committing.front();
        }
        EXPECT_EQ(in_use->id(), pool._committing.front()->id());
    }
    auto* consumer = static_cast<KafkaDataConsumer*>(in_use.get());
    ASSERT_TRUE(wait_until([&] {
        auto [held, completed] = commit_results(consumer);
        return held > 0 && held == completed;
    }));

    request.set_offsets(0, 4);
    ASSERT_OK(_executor->commit_kafka_offsets(request, kCommitTimeoutMs, &group_id, &evicted));
    EXPECT_EQ(0u, commit_results(consumer).first);
    EXPECT_FALSE(in_use->has_commit_in_flight());
    EXPECT_EQ(5, committed_offset(kGroupId, 0));
    EXPECT_TRUE(evicted.empty());
}

// A disabled pool keeps the consumer of a commit that timed out apart too, rather than handing it to the RPC to close
// after the response: closing it would hold the RPC thread until the commit completes. The background worker closes it
// once its commit has completed, as the pool has no room for it.
TEST_F(RoutineLoadKafkaScanRangeTest, disabled_pool_keeps_the_consumer_of_a_commit_that_timed_out) {
    const int32_t saved_pool_size = config::routine_load_kafka_consumer_pool_size;
    DeferOp restore_pool_size([&] { config::routine_load_kafka_consumer_pool_size = saved_pool_size; });
    config::routine_load_kafka_consumer_pool_size = 0;
    ASSERT_EQ(RdKafka::ERR_NO_ERROR, produce(0, {"a,1"}));
    ASSERT_EQ(RD_KAFKA_RESP_ERR_NO_ERROR, delay_next_offset_commit(2000));
    auto request = make_commit_request(kGroupId);
    request.add_partition_ids(0);
    request.add_offsets(0);
    std::string group_id;
    std::vector<std::shared_ptr<DataConsumer>> evicted;

    Status st = _executor->commit_kafka_offsets(request, 300, &group_id, &evicted);
    ASSERT_TRUE(st.is_time_out()) << st;
    EXPECT_TRUE(evicted.empty());
    auto& pool = _executor->_data_consumer_pool;
    ASSERT_EQ(1u, pool._committing.size());

    ASSERT_TRUE(wait_until([&] { return !pool._committing.front()->has_commit_in_flight(); }));
    pool._clean_idle_consumer_bg();
    EXPECT_TRUE(pool._committing.empty());
    EXPECT_TRUE(pool._pool.empty());
    EXPECT_EQ(1, committed_offset(kGroupId, 0));
}

TEST_F(RoutineLoadKafkaScanRangeTest, prepare_rejects_a_range_that_is_not_one_stream_with_a_load_id) {
    const TUniqueId load_id = make_load_id(1);
    const TBrokerScanRange scan_range = make_scan_range(load_id, {{0, 0}});

    auto no_range = scan_range;
    no_range.ranges.clear();
    auto two_ranges = scan_range;
    two_ranges.ranges.push_back(scan_range.ranges[0]);
    auto file_range = scan_range;
    file_range.ranges[0].__set_file_type(TFileType::FILE_LOCAL);
    auto no_load_id = scan_range;
    no_load_id.ranges[0].__isset.load_id = false;
    for (const auto* invalid : {&no_range, &two_ranges, &file_range, &no_load_id}) {
        auto ctx = _executor->prepare_kafka_scan_range(*invalid);
        EXPECT_TRUE(ctx.status().is_invalid_argument()) << ctx.status();
    }

    auto no_partition = scan_range;
    no_partition.kafka_scan_range.partition_begin_offset.clear();
    auto ctx = _executor->prepare_kafka_scan_range(no_partition);
    EXPECT_TRUE(ctx.status().is_invalid_argument()) << ctx.status();
    EXPECT_NE(std::string::npos, ctx.status().message().find("has no partition to consume")) << ctx.status();

    // none of them got as far as registering a pipe
    EXPECT_EQ(nullptr, load_stream_mgr()->get(load_id));
}

TEST_F(RoutineLoadKafkaScanRangeTest, prepare_registers_the_pipe_and_fills_the_context) {
    const TUniqueId load_id = make_load_id(2);
    auto scan_range = make_scan_range(load_id, {{0, 5}, {1, 7}});
    scan_range.kafka_scan_range.__set_need_source_metadata(true);
    scan_range.kafka_scan_range.__set_need_message_key(true);
    scan_range.kafka_scan_range.__set_need_message_headers(false);
    auto ctx_or = _executor->prepare_kafka_scan_range(scan_range);
    ASSERT_OK(ctx_or.status());
    StreamLoadContext* ctx = ctx_or.value();

    EXPECT_EQ(TLoadType::ROUTINE_LOAD, ctx->load_type);
    EXPECT_EQ(TLoadSourceType::KAFKA, ctx->load_src_type);
    EXPECT_EQ(42, ctx->job_id);
    EXPECT_EQ("db", ctx->db);
    EXPECT_EQ("tbl", ctx->table);
    EXPECT_EQ("round_label", ctx->label);
    EXPECT_EQ(7, ctx->txn_id);
    EXPECT_EQ(TFileFormatType::FORMAT_CSV_PLAIN, ctx->format);
    EXPECT_EQ(30000, ctx->max_interval_ms);
    EXPECT_EQ(64 * 1024 * 1024, ctx->max_batch_size);
    EXPECT_EQ(200, ctx->kafka_idle_end_ms);
    ASSERT_TRUE(ctx->kafka_row_delimiter.has_value());
    EXPECT_EQ('\n', ctx->kafka_row_delimiter.value());
    EXPECT_TRUE(ctx->kafka_scan_range_mode);
    ASSERT_NE(nullptr, ctx->kafka_info);
    EXPECT_EQ(_brokers, ctx->kafka_info->brokers);
    EXPECT_EQ(kTopic, ctx->kafka_info->topic);
    std::map<int32_t, int64_t> begin_offsets{{0, 5}, {1, 7}};
    EXPECT_EQ(begin_offsets, ctx->kafka_info->begin_offset);
    EXPECT_EQ(kGroupId, ctx->kafka_info->properties.at("group.id"));
    EXPECT_TRUE(ctx->kafka_info->need_source_metadata);
    EXPECT_TRUE(ctx->kafka_info->need_message_key);
    EXPECT_FALSE(ctx->kafka_info->need_message_headers);

    // the file scanner opens the pipe by the load id of the range
    auto pipe = load_stream_mgr()->get(load_id);
    ASSERT_NE(nullptr, pipe);
    EXPECT_EQ(static_cast<MessageBodySink*>(pipe.get()), ctx->body_sink.get());

    // another scan range with the same load id cannot take the pipe over
    auto duplicate = _executor->prepare_kafka_scan_range(scan_range);
    EXPECT_FALSE(duplicate.ok());
    EXPECT_NE(std::string::npos, duplicate.status().message().find("failed to register the pipe"))
            << duplicate.status();
    EXPECT_EQ(pipe.get(), load_stream_mgr()->get(load_id).get());

    // the caller owns the context, and releasing it unregisters the pipe
    pipe.reset();
    StreamLoadContext::release(ctx);
    EXPECT_EQ(nullptr, load_stream_mgr()->get(load_id));
}

TEST_F(RoutineLoadKafkaScanRangeTest, task_count_covers_queued_and_successful_consumption) {
    auto ctx_or = _executor->prepare_kafka_scan_range(make_scan_range(make_load_id(20), {{0, 0}}));
    ASSERT_OK(ctx_or.status());
    std::shared_ptr<StreamLoadContext> ctx(ctx_or.value(), StreamLoadContext::release);
    ThreadPoolBlocker blocker;
    ASSERT_OK(blocker.block(_executor->_thread_pool.get()));
    EXPECT_EQ("0", task_count());

    ASSERT_OK(_executor->start_kafka_scan_range(ctx.get(), std::make_shared<KafkaScanRangeReport>(), nullptr));
    EXPECT_EQ("1", task_count());
    blocker.release();
    _executor->_thread_pool->wait();
    EXPECT_TRUE(load_stream_mgr()->get(make_load_id(20))->finished());
    EXPECT_EQ("0", task_count());
}

TEST_F(RoutineLoadKafkaScanRangeTest, failed_consumption_releases_the_task_count) {
    auto scan_range = make_scan_range(make_load_id(21), {{0, 0}}, {{"fetch.wait.max.ms", "not-a-number"}});
    auto ctx_or = _executor->prepare_kafka_scan_range(scan_range);
    ASSERT_OK(ctx_or.status());
    std::shared_ptr<StreamLoadContext> ctx(ctx_or.value(), StreamLoadContext::release);
    ThreadPoolBlocker blocker;
    ASSERT_OK(blocker.block(_executor->_thread_pool.get()));
    bool failed = false;

    ASSERT_OK(_executor->start_kafka_scan_range(ctx.get(), std::make_shared<KafkaScanRangeReport>(),
                                                [&](const Status&) { failed = true; }));
    EXPECT_EQ("1", task_count());
    blocker.release();
    _executor->_thread_pool->wait();
    EXPECT_TRUE(failed);
    EXPECT_EQ("0", task_count());
}

TEST_F(RoutineLoadKafkaScanRangeTest, shutdown_releases_the_count_of_discarded_consumption) {
    auto ctx_or = _executor->prepare_kafka_scan_range(make_scan_range(make_load_id(22), {{0, 0}}));
    ASSERT_OK(ctx_or.status());
    std::shared_ptr<StreamLoadContext> ctx(ctx_or.value(), StreamLoadContext::release);
    ThreadPoolBlocker blocker;
    ASSERT_OK(blocker.block(_executor->_thread_pool.get()));

    ASSERT_OK(_executor->start_kafka_scan_range(ctx.get(), std::make_shared<KafkaScanRangeReport>(), nullptr));
    EXPECT_EQ("1", task_count());
    auto shutdown = std::async(std::launch::async, [&] { _executor->_thread_pool->shutdown(); });
    const bool shutting_down = wait_until([&] { return !_executor->_thread_pool->is_pool_status_ok(); });
    blocker.release();
    shutdown.get();
    EXPECT_TRUE(shutting_down);
    EXPECT_FALSE(load_stream_mgr()->get(make_load_id(22))->finished());
    EXPECT_EQ("0", task_count());
}

TEST_F(RoutineLoadKafkaScanRangeTest, rejected_submission_releases_the_task_count) {
    auto ctx_or = _executor->prepare_kafka_scan_range(make_scan_range(make_load_id(23), {{0, 0}}));
    ASSERT_OK(ctx_or.status());
    std::shared_ptr<StreamLoadContext> ctx(ctx_or.value(), StreamLoadContext::release);
    _executor->_thread_pool->shutdown();

    EXPECT_FALSE(_executor->start_kafka_scan_range(ctx.get(), std::make_shared<KafkaScanRangeReport>(), nullptr).ok());
    EXPECT_EQ("0", task_count());
}

// The fragment of a scan range that waits for messages fails or is cancelled, which cancels its pipe: the consumption
// stops within moments instead of at the end of its consume time, does not call back into the fragment, and the pipe
// keeps the reason it was cancelled with. With enable.partition.eof off, the empty partition never reports its end, as
// when the broker sends nothing.
TEST_F(RoutineLoadKafkaScanRangeTest, cancelled_pipe_stops_a_consumption_that_waits_for_messages) {
    const TUniqueId load_id = make_load_id(24);
    std::map<std::string, std::string> properties{{"group.id", kGroupId}, {"fetch.wait.max.ms", "10"}};
    properties["enable.partition.eof"] = "false";
    auto scan_range = make_scan_range(load_id, {{0, 0}}, properties);
    scan_range.kafka_scan_range.__set_max_consume_ms(120000);
    auto ctx_or = _executor->prepare_kafka_scan_range(scan_range);
    ASSERT_OK(ctx_or.status());
    std::shared_ptr<StreamLoadContext> ctx(ctx_or.value(), StreamLoadContext::release);
    auto pipe = load_stream_mgr()->get(load_id);
    ASSERT_NE(nullptr, pipe);

    bool failed = false;
    ASSERT_OK(_executor->start_kafka_scan_range(ctx.get(), std::make_shared<KafkaScanRangeReport>(),
                                                [&](const Status&) { failed = true; }));
    // let the consumption assign its partition and wait for messages
    std::this_thread::sleep_for(std::chrono::seconds(2));
    EXPECT_EQ("1", task_count());

    const auto cancelled_at = std::chrono::steady_clock::now();
    pipe->cancel(Status::Cancelled("the round was cancelled"));
    ASSERT_TRUE(wait_until([&] { return task_count() == "0"; }));
    const auto stopped = std::chrono::steady_clock::now() - cancelled_at;
    EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(stopped).count(), 10000);
    _executor->_thread_pool->wait();

    EXPECT_FALSE(failed);
    EXPECT_FALSE(pipe->finished());
    auto data = read_all(pipe.get());
    EXPECT_FALSE(data.ok());
    EXPECT_NE(std::string::npos, data.status().message().find("the round was cancelled")) << data.status();
}

// A pipe takes a small message into its write buffer without checking whether it was cancelled, so a scan range whose
// messages keep coming checks its pipe while it appends them too: it stops within moments of its fragment failing or
// being cancelled, as one that waits for messages does, instead of when the write buffer fills up or its consume time
// ends.
TEST_F(RoutineLoadKafkaScanRangeTest, cancelled_pipe_stops_a_consumption_that_keeps_receiving_messages) {
    const TUniqueId load_id = make_load_id(26);
    std::map<std::string, std::string> properties{{"group.id", kGroupId}, {"fetch.wait.max.ms", "10"}};
    properties["enable.partition.eof"] = "false";
    auto scan_range = make_scan_range(load_id, {{0, 0}}, properties);
    scan_range.kafka_scan_range.__set_max_consume_ms(120000);
    auto ctx_or = _executor->prepare_kafka_scan_range(scan_range);
    ASSERT_OK(ctx_or.status());
    std::shared_ptr<StreamLoadContext> ctx(ctx_or.value(), StreamLoadContext::release);
    auto pipe = load_stream_mgr()->get(load_id);
    ASSERT_NE(nullptr, pipe);

    // a one-byte message about every millisecond: the rows of minutes fit the write buffer of the pipe
    std::atomic<bool> producing{true};
    std::thread producer([&] {
        char payload[] = "a";
        while (producing.load()) {
            (void)_producer->produce(kTopic, 0, RdKafka::Producer::RK_MSG_COPY, payload, 1, nullptr, 0, 0, nullptr);
            _producer->poll(0);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        (void)_producer->flush(10000);
    });
    DeferOp stop_producing([&] {
        producing = false;
        producer.join();
    });

    bool failed = false;
    ASSERT_OK(_executor->start_kafka_scan_range(ctx.get(), std::make_shared<KafkaScanRangeReport>(),
                                                [&](const Status&) { failed = true; }));
    // let the consumption assign its partition and append messages for a while
    std::this_thread::sleep_for(std::chrono::seconds(2));
    EXPECT_EQ("1", task_count());

    const auto cancelled_at = std::chrono::steady_clock::now();
    pipe->cancel(Status::Cancelled("the round was cancelled"));
    ASSERT_TRUE(wait_until([&] { return task_count() == "0"; }));
    EXPECT_LT(elapsed_ms(cancelled_at), 5000);
    _executor->_thread_pool->wait();

    EXPECT_FALSE(failed);
    EXPECT_FALSE(pipe->finished());
    auto data = read_all(pipe.get());
    EXPECT_FALSE(data.ok());
    EXPECT_NE(std::string::npos, data.status().message().find("the round was cancelled")) << data.status();
}

// A consumption that still waits for a thread when its fragment fails or is cancelled does not start: it creates no
// consumers and assigns no partitions. Creating a consumer of this scan range would fail on the property and fail the
// fragment, which therefore does not happen.
TEST_F(RoutineLoadKafkaScanRangeTest, queued_consumption_of_a_cancelled_pipe_does_not_start) {
    const TUniqueId load_id = make_load_id(25);
    auto scan_range =
            make_scan_range(load_id, {{0, 0}}, {{"group.id", kGroupId}, {"fetch.wait.max.ms", "not-a-number"}});
    auto ctx_or = _executor->prepare_kafka_scan_range(scan_range);
    ASSERT_OK(ctx_or.status());
    std::shared_ptr<StreamLoadContext> ctx(ctx_or.value(), StreamLoadContext::release);
    auto pipe = load_stream_mgr()->get(load_id);
    ASSERT_NE(nullptr, pipe);
    ThreadPoolBlocker blocker;
    ASSERT_OK(blocker.block(_executor->_thread_pool.get()));
    bool failed = false;

    ASSERT_OK(_executor->start_kafka_scan_range(ctx.get(), std::make_shared<KafkaScanRangeReport>(),
                                                [&](const Status&) { failed = true; }));
    EXPECT_EQ("1", task_count());
    pipe->cancel(Status::Cancelled("the round was cancelled"));
    blocker.release();
    _executor->_thread_pool->wait();

    EXPECT_FALSE(failed);
    EXPECT_EQ("0", task_count());
    auto data = read_all(pipe.get());
    EXPECT_FALSE(data.ok());
    EXPECT_NE(std::string::npos, data.status().message().find("the round was cancelled")) << data.status();
}

TEST_F(RoutineLoadKafkaScanRangeTest, fragment_without_kafka_scan_ranges_prepares_nothing) {
    // a plain stream load reads a broker scan range from a pipe too, but without a kafka scan range
    auto stream_load_range = make_scan_range(make_load_id(3), {{0, 0}});
    stream_load_range.__isset.kafka_scan_range = false;
    TScanRangeParams hdfs_range;
    hdfs_range.scan_range.__set_hdfs_scan_range(THdfsScanRange());

    TPlanFragmentExecParams exec_params;
    exec_params.per_node_scan_ranges[1] = {to_scan_range_params(stream_load_range), hdfs_range};
    TExecPlanFragmentParams params = fragment_params(exec_params);
    pipeline::UnifiedExecPlanFragmentParams request(params, params);

    // nothing is touched: this executor has neither a fragment context nor a routine load executor
    FragmentExecutor fragment_executor;
    ASSERT_OK(fragment_executor._prepare_kafka_scan_ranges(request));
    EXPECT_EQ(nullptr, load_stream_mgr()->get(make_load_id(3)));
}

TEST_F(RoutineLoadKafkaScanRangeTest, fragment_kafka_scan_ranges_need_the_routine_load_executor) {
    TPlanFragmentExecParams exec_params;
    exec_params.per_node_scan_ranges[1] = {to_scan_range_params(make_scan_range(make_load_id(4), {{0, 0}}))};
    TExecPlanFragmentParams params = fragment_params(exec_params);
    pipeline::UnifiedExecPlanFragmentParams request(params, params);

    FragmentExecutor fragment_executor;
    Status st = fragment_executor._prepare_kafka_scan_ranges(request);
    EXPECT_TRUE(st.is_service_unavailable()) << st;
}

// Each Kafka scan range of the fragment, whether it comes per node or per driver sequence, gets a pipe that the
// fragment owns. What the scan ranges consumed is merged into the report of the fragment instance before their pipes
// finish, so it is complete by the time the scanners reach the end.
TEST_F(RoutineLoadKafkaScanRangeTest, fragment_consumes_each_kafka_scan_range_into_its_pipe) {
    ASSERT_EQ(RdKafka::ERR_NO_ERROR, produce(0, {"a,1", "b,2"}));
    ASSERT_EQ(RdKafka::ERR_NO_ERROR, produce(1, {"c,3"}));
    const TUniqueId per_node_load_id = make_load_id(5);
    const TUniqueId per_driver_load_id = make_load_id(6);

    TPlanFragmentExecParams exec_params;
    exec_params.per_node_scan_ranges[1] = {to_scan_range_params(make_scan_range(per_node_load_id, {{0, 0}}))};
    std::map<TPlanNodeId, std::map<int32_t, std::vector<TScanRangeParams>>> per_driver_scan_ranges;
    per_driver_scan_ranges[2][0] = {to_scan_range_params(make_scan_range(per_driver_load_id, {{1, 0}}))};
    exec_params.__set_node_to_per_driver_seq_scan_ranges(per_driver_scan_ranges);
    TExecPlanFragmentParams params = round_fragment_params(exec_params);
    pipeline::UnifiedExecPlanFragmentParams request(params, params);

    auto query_ctx = std::make_shared<pipeline::QueryContext>();
    FragmentExecutor fragment_executor;
    prepare_fragment(&fragment_executor, query_ctx);
    ASSERT_OK(prepare_round_fragment(&fragment_executor, request));

    auto* fragment_ctx = fragment_executor._fragment_ctx.get();
    EXPECT_EQ(2u, fragment_ctx->_fragment_attachments.size());
    // like a classic task, the instance names its topic and the partitions it consumes, each with the offset it
    // consumes from, as the source of its rejected records
    EXPECT_EQ(R"({"format":"kafka","topic":")" + kTopic + R"(","partitions":[0,1],"begin_offsets":[0,0]})",
              fragment_ctx->runtime_state()->query_options().routine_load_source_info);
    std::shared_ptr<KafkaScanRangeReport> report = fragment_ctx->runtime_state()->kafka_scan_range_report();
    ASSERT_NE(nullptr, report);
    auto per_node_pipe = load_stream_mgr()->get(per_node_load_id);
    auto per_driver_pipe = load_stream_mgr()->get(per_driver_load_id);
    ASSERT_NE(nullptr, per_node_pipe);
    ASSERT_NE(nullptr, per_driver_pipe);

    ASSERT_TRUE(wait_until([&] { return per_node_pipe->finished() && per_driver_pipe->finished(); }));
    TKafkaScanRangeReport consumed;
    report->to_thrift(&consumed);
    std::map<int32_t, int64_t> consumed_offsets{{0, 1}, {1, 0}};
    EXPECT_EQ(consumed_offsets, consumed.partition_cmt_offset);
    EXPECT_EQ(3, consumed.received_rows);
    EXPECT_EQ(9, consumed.received_bytes);
    EXPECT_TRUE(consumed.reached_end);

    // the final exec status report of the instance carries what its scan ranges consumed, an earlier one does not
    auto* runtime_state = fragment_ctx->runtime_state();
    auto final_report = pipeline::ExecStateReporter::create_report_exec_status_params(
            query_ctx.get(), fragment_ctx, runtime_state->runtime_profile(), runtime_state->load_channel_profile(),
            Status::OK(), true);
    ASSERT_TRUE(final_report->__isset.kafka_scan_range_report);
    EXPECT_EQ(consumed_offsets, final_report->kafka_scan_range_report.partition_cmt_offset);
    EXPECT_EQ(3, final_report->kafka_scan_range_report.received_rows);
    auto running_report = pipeline::ExecStateReporter::create_report_exec_status_params(
            query_ctx.get(), fragment_ctx, runtime_state->runtime_profile(), runtime_state->load_channel_profile(),
            Status::OK(), false);
    EXPECT_FALSE(running_report->__isset.kafka_scan_range_report);

    auto per_node_data = read_all(per_node_pipe.get());
    ASSERT_OK(per_node_data.status());
    EXPECT_EQ("a,1\nb,2\n", per_node_data.value());
    auto per_driver_data = read_all(per_driver_pipe.get());
    ASSERT_OK(per_driver_data.status());
    EXPECT_EQ("c,3\n", per_driver_data.value());
    EXPECT_TRUE(fragment_ctx->final_status().ok()) << fragment_ctx->final_status();

    // like a classic routine load task, the instance counts the rows it loaded in the stream load metrics of the BE once
    // it finishes, and not the bytes it received, which only HTTP stream loads count
    auto* metrics = StreamLoadMetrics::instance();
    const int64_t loaded_rows = metrics->stream_load_rows_total.value();
    const int64_t received_bytes = metrics->stream_receive_bytes_total.value();
    runtime_state->update_num_rows_load_sink(3);
    ASSERT_TRUE(fragment_ctx->_finish_cb != nullptr);
    fragment_ctx->_finish_cb(fragment_ctx);
    EXPECT_EQ(loaded_rows + 3, metrics->stream_load_rows_total.value());
    EXPECT_EQ(received_bytes, metrics->stream_receive_bytes_total.value());
}

// A scan range whose consumption fails fails its fragment with the error before cancelling its pipe, so that the
// fragment reports this error rather than what its scanner makes of the cancelled pipe.
TEST_F(RoutineLoadKafkaScanRangeTest, failed_consumption_fails_the_fragment_before_its_pipe) {
    const TUniqueId load_id = make_load_id(7);
    // librdkafka rejects the value, so no consumer of the scan range can be created
    auto scan_range =
            make_scan_range(load_id, {{0, 0}}, {{"group.id", kGroupId}, {"fetch.wait.max.ms", "not-a-number"}});
    TPlanFragmentExecParams exec_params;
    exec_params.per_node_scan_ranges[1] = {to_scan_range_params(scan_range)};
    TExecPlanFragmentParams params = round_fragment_params(exec_params);
    pipeline::UnifiedExecPlanFragmentParams request(params, params);

    auto query_ctx = std::make_shared<pipeline::QueryContext>();
    FragmentExecutor fragment_executor;
    prepare_fragment(&fragment_executor, query_ctx);
    ASSERT_OK(prepare_round_fragment(&fragment_executor, request));
    auto pipe = load_stream_mgr()->get(load_id);
    ASSERT_NE(nullptr, pipe);

    auto* fragment_ctx = fragment_executor._fragment_ctx.get();
    ASSERT_TRUE(wait_until([&] { return !fragment_ctx->final_status().ok(); }));
    Status fragment_status = fragment_ctx->final_status();
    EXPECT_NE(std::string::npos, fragment_status.message().find("fetch.wait.max.ms")) << fragment_status;
    EXPECT_TRUE(fragment_ctx->runtime_state()->is_cancelled());

    // the scanner of the pipe sees the same error
    auto data = read_all(pipe.get());
    EXPECT_FALSE(data.ok());
    EXPECT_NE(std::string::npos, data.status().message().find("fetch.wait.max.ms")) << data.status();

    // a failed instance counts nothing in the stream load metrics
    auto* metrics = StreamLoadMetrics::instance();
    const int64_t loaded_rows = metrics->stream_load_rows_total.value();
    fragment_ctx->runtime_state()->update_num_rows_load_sink(3);
    ASSERT_TRUE(fragment_ctx->_finish_cb != nullptr);
    fragment_ctx->_finish_cb(fragment_ctx);
    EXPECT_EQ(loaded_rows, metrics->stream_load_rows_total.value());
}

// A round that shuffles its rows to their sinks loads them in the instances after the exchange, which read no Kafka
// scan range: they count the rows their sinks loaded, while the instances that read the scan ranges load none. The
// fragment of a classic task, which the task prepares without the routine load executor, and a fragment of another
// load count nothing here.
TEST_F(RoutineLoadKafkaScanRangeTest, fragment_after_the_exchange_counts_the_rows_its_sink_loaded) {
    TPlanFragmentExecParams exec_params;
    TExecPlanFragmentParams params = round_fragment_params(exec_params);
    pipeline::UnifiedExecPlanFragmentParams request(params, params);

    auto query_ctx = std::make_shared<pipeline::QueryContext>();
    FragmentExecutor fragment_executor;
    prepare_fragment(&fragment_executor, query_ctx);
    ASSERT_OK(prepare_round_fragment(&fragment_executor, request));
    auto* fragment_ctx = fragment_executor._fragment_ctx.get();
    EXPECT_TRUE(fragment_ctx->_fragment_attachments.empty());
    auto* metrics = StreamLoadMetrics::instance();
    const int64_t loaded_rows = metrics->stream_load_rows_total.value();
    fragment_ctx->runtime_state()->update_num_rows_load_sink(4);
    ASSERT_TRUE(fragment_ctx->_finish_cb != nullptr);
    fragment_ctx->_finish_cb(fragment_ctx);
    EXPECT_EQ(loaded_rows + 4, metrics->stream_load_rows_total.value());

    FragmentExecutor classic_fragment_executor;
    prepare_fragment(&classic_fragment_executor, std::make_shared<pipeline::QueryContext>());
    classic_fragment_executor.set_routine_load_task_executor(nullptr);
    ASSERT_OK(prepare_round_fragment(&classic_fragment_executor, request));
    EXPECT_TRUE(classic_fragment_executor._fragment_ctx->_finish_cb == nullptr);

    auto stream_load_params = params;
    stream_load_params.query_options.__set_load_job_type(TLoadJobType::STREAM_LOAD);
    pipeline::UnifiedExecPlanFragmentParams stream_load_request(stream_load_params, stream_load_params);
    FragmentExecutor stream_load_fragment_executor;
    prepare_fragment(&stream_load_fragment_executor, std::make_shared<pipeline::QueryContext>());
    ASSERT_OK(prepare_round_fragment(&stream_load_fragment_executor, stream_load_request));
    EXPECT_TRUE(stream_load_fragment_executor._fragment_ctx->_finish_cb == nullptr);
}

// When a scan range of the fragment fails to prepare, the pipes of the scan ranges prepared before it are closed and
// unregistered, and nothing reaches the fragment.
TEST_F(RoutineLoadKafkaScanRangeTest, scan_range_that_fails_to_prepare_closes_the_prepared_ones) {
    const TUniqueId prepared_load_id = make_load_id(8);
    TPlanFragmentExecParams exec_params;
    exec_params.per_node_scan_ranges[1] = {to_scan_range_params(make_scan_range(prepared_load_id, {{0, 0}})),
                                           to_scan_range_params(make_scan_range(make_load_id(9), {}))};
    TExecPlanFragmentParams params = fragment_params(exec_params);
    pipeline::UnifiedExecPlanFragmentParams request(params, params);

    auto query_ctx = std::make_shared<pipeline::QueryContext>();
    FragmentExecutor fragment_executor;
    prepare_fragment(&fragment_executor, query_ctx);
    Status st = fragment_executor._prepare_kafka_scan_ranges(request);
    EXPECT_TRUE(st.is_invalid_argument()) << st;

    EXPECT_EQ(nullptr, load_stream_mgr()->get(prepared_load_id));
    auto* fragment_ctx = fragment_executor._fragment_ctx.get();
    EXPECT_TRUE(fragment_ctx->_fragment_attachments.empty());
    EXPECT_EQ(nullptr, fragment_ctx->runtime_state()->kafka_scan_range_report());
}

} // namespace starrocks::orchestration
