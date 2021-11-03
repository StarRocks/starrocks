// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once
#include <memory>
#include <mutex>

#include "common/statusor.h"
#include "exprs/expr_context.h"
#include "exprs/predicate.h"
#include "exprs/vectorized/runtime_filter_bank.h"

namespace starrocks {
namespace pipeline {
class RuntimeFilterHolder;
using RuntimeFilterHolderPtr = std::unique_ptr<RuntimeFilterHolder>;
using RuntimeInFilter = starrocks::ExprContext;
using RuntimeBloomFilter = starrocks::vectorized::RuntimeFilterBuildDescriptor;
using RuntimeBloomFilterProbeDescriptor = starrocks::vectorized::RuntimeFilterProbeDescriptor;
using RuntimeBloomFilterProbeDescriptorPtr = RuntimeBloomFilterProbeDescriptor*;
using RuntimeBloomFilterRunningContext = starrocks::vectorized::JoinRuntimeFilter::RunningContext;
using RuntimeInFilterPtr = RuntimeInFilter*;
using RuntimeBloomFilterPtr = RuntimeBloomFilter*;
using RuntimeInFilters = std::list<RuntimeInFilterPtr>;
using RuntimeBloomFilters = std::list<RuntimeBloomFilterPtr>;
struct RuntimeFilterCollector;
using RuntimeFilterCollectorPtr = std::unique_ptr<RuntimeFilterCollector>;
using RuntimeFilterProbeCollector = starrocks::vectorized::RuntimeFilterProbeCollector;
using Predicate = starrocks::Predicate;
struct RuntimeBloomFilterBuildParam;
using RuntimeBloomFilterBuildParams = std::list<RuntimeBloomFilterBuildParam>;
struct RuntimeBloomFilterBuildParam {
    RuntimeBloomFilterBuildParam(bool eq_null, const ColumnPtr& column, size_t column_offset, size_t ht_row_count)
            : eq_null(eq_null), column(column), column_offset(column_offset), ht_row_count(ht_row_count) {}
    bool eq_null;
    ColumnPtr column;
    size_t column_offset;
    size_t ht_row_count;
};

struct RuntimeFilterCollector {
    RuntimeFilterCollector(RuntimeInFilters&& in_filters, RuntimeBloomFilters&& bloom_filters)
            : _in_filters(std::move(in_filters)), _bloom_filters(std::move(bloom_filters)) {}

    RuntimeBloomFilters& get_bloom_filters() { return _bloom_filters; }
    RuntimeInFilters& get_in_filters() { return _in_filters; }

    std::vector<RuntimeInFilterPtr> get_in_filters_bounded_by_tuple_ids(const std::vector<TupleId>& tuple_ids) {
        std::lock_guard<std::mutex> lock(_mutex);
        std::vector<ExprContext*> selected_in_filters;
        for (auto* in_filter : _in_filters) {
            if (in_filter->root()->is_bound(tuple_ids)) {
                selected_in_filters.push_back(in_filter);
            }
        }
        return selected_in_filters;
    }

private:
    std::mutex _mutex;
    // local runtime in-filter
    RuntimeInFilters _in_filters;
    // global/local runtime bloom-filter(including max-min filter)
    RuntimeBloomFilters _bloom_filters;
};

class RuntimeFilterHolder {
public:
    void add_collector(RuntimeFilterCollectorPtr&& collector) {
        _collector_ownership = std::move(collector);
        _collector.store(_collector_ownership.get(), std::memory_order_release);
    }
    RuntimeFilterCollector* get_collector() { return _collector.load(std::memory_order_acquire); }
    bool is_ready() { return get_collector() != nullptr; }

private:
    RuntimeFilterCollectorPtr _collector_ownership;
    std::atomic<RuntimeFilterCollector*> _collector;
};

class RuntimeFilterHub {
public:
    void add_holder(TPlanNodeId id) { _holders.emplace(std::make_pair(id, std::make_unique<RuntimeFilterHolder>())); }
    void add_collector(TPlanNodeId id, RuntimeFilterCollectorPtr&& collector) {
        get_holder(id)->add_collector(std::move(collector));
    }

    RuntimeBloomFilters& get_bloom_filters(TPlanNodeId id) {
        return get_holder(id)->get_collector()->get_bloom_filters();
    }

    void close_all_in_filters(RuntimeState* state) {
        for (auto& [_, holder] : _holders) {
            if (auto* collector = holder->get_collector()) {
                for (auto& in_filter : collector->get_in_filters()) {
                    in_filter->close(state);
                }
            }
        }
    }

    std::vector<RuntimeFilterHolder*> gather_holders(std::set<TPlanNodeId> ids) {
        std::vector<RuntimeFilterHolder*> holders;
        holders.reserve(ids.size());
        for (auto id : ids) {
            holders.push_back(get_holder(id).get());
        }
        return holders;
    }

private:
    RuntimeFilterHolderPtr& get_holder(TPlanNodeId id) {
        auto it = _holders.find(id);
        DCHECK(it != _holders.end());
        return it->second;
    }
    std::mutex _mutex;
    std::unordered_map<TPlanNodeId, RuntimeFilterHolderPtr> _holders;
};

struct RuntimeBloomFilterEvalContext {
    void prepare(RuntimeProfile* runtime_profile) {
        join_runtime_filter_timer = ADD_TIMER(runtime_profile, "JoinRuntimeFilterTime");
        join_runtime_filter_input_counter = ADD_COUNTER(runtime_profile, "JoinRuntimeFilterInputRows", TUnit::UNIT);
        join_runtime_filter_output_counter = ADD_COUNTER(runtime_profile, "JoinRuntimeFilterOutputRows", TUnit::UNIT);
        join_runtime_filter_eval_counter = ADD_COUNTER(runtime_profile, "JoinRuntimeFilterEvaluate", TUnit::UNIT);
    }

    std::map<double, RuntimeBloomFilterProbeDescriptorPtr> selectivity;
    size_t input_chunk_nums = 0;
    int run_filter_nums = 0;
    std::unordered_map<size_t, RuntimeBloomFilterRunningContext> running_contexts;
    RuntimeProfile::Counter* join_runtime_filter_timer = nullptr;
    RuntimeProfile::Counter* join_runtime_filter_input_counter = nullptr;
    RuntimeProfile::Counter* join_runtime_filter_output_counter = nullptr;
    RuntimeProfile::Counter* join_runtime_filter_eval_counter = nullptr;
};

class RefCountedRuntimeFilterProbeCollector;
using RefCountedRuntimeFilterProbeCollectorPtr = std::shared_ptr<RefCountedRuntimeFilterProbeCollector>;
class RefCountedRuntimeFilterProbeCollector {
public:
    RefCountedRuntimeFilterProbeCollector(size_t num_operators_generated,
                                          RuntimeFilterProbeCollector&& rf_probe_collector)
            : _count((num_operators_generated << 32) | num_operators_generated),
              _num_operators_generated(num_operators_generated),
              _rf_probe_collector(std::move(rf_probe_collector)) {}

    Status prepare(RuntimeState* state, const RowDescriptor& row_desc, RuntimeProfile* p) {
        if ((_count.fetch_sub(1) & 0xffff'ffffull) == _num_operators_generated) {
            RETURN_IF_ERROR(_rf_probe_collector.prepare(state, row_desc, p));
            RETURN_IF_ERROR(_rf_probe_collector.open(state));
        }
        return Status::OK();
    }

    void close(RuntimeState* state) {
        static constexpr size_t k = 1ull << 32;
        if (_count.fetch_sub(k) == k) {
            _rf_probe_collector.close(state);
        }
    }

    RuntimeFilterProbeCollector* get_rf_probe_collector() { return &_rf_probe_collector; }

private:
    std::atomic<size_t> _count;
    const size_t _num_operators_generated;
    RuntimeFilterProbeCollector _rf_probe_collector;
};
class PartialRuntimeFilterMerger {
public:
    PartialRuntimeFilterMerger(ObjectPool* pool, size_t limit, size_t num_builders)
            : _pool(pool),
              _limit(limit),
              _num_active_builders(num_builders),
              _ht_row_counts(num_builders),
              _partial_in_filters(num_builders),
              _partial_bloom_filter_build_params(num_builders) {}

    StatusOr<bool> add_partial_filters(
            size_t idx, size_t ht_row_count, std::list<ExprContext*>&& partial_in_filters,
            std::list<RuntimeBloomFilterBuildParam>&& partial_bloom_filter_build_params,
            std::list<vectorized::RuntimeFilterBuildDescriptor*>&& bloom_filter_descriptors) {
        DCHECK(idx < _partial_bloom_filter_build_params.size());
        _ht_row_counts[idx] = ht_row_count;
        _partial_in_filters[idx] = std::move(partial_in_filters);
        _partial_bloom_filter_build_params[idx] = std::move(partial_bloom_filter_build_params);
        if (1 == _num_active_builders--) {
            _bloom_filter_descriptors = std::move(bloom_filter_descriptors);
            merge_in_filters();
            merge_bloom_filters();
            return true;
        }
        return false;
    }

    RuntimeInFilters get_total_in_filters() { return _partial_in_filters[0]; }

    RuntimeBloomFilters get_total_bloom_filters() { return _bloom_filter_descriptors; }

    Status merge_in_filters() {
        bool can_merge_in_filters = true;
        size_t num_rows = 0;
        ssize_t k = -1;
        //squeeze _partial_in_filters and eliminate empty in-filter lists generated by empty hash tables.
        for (auto i = 0; i < _ht_row_counts.size(); ++i) {
            auto& in_filters = _partial_in_filters[i];
            // empty in-filter list is generated by empty hash tables, so skip it.
            if (_ht_row_counts[i] == 0) {
                continue;
            }
            // empty in-filter list is generated by non-empty hash tables(size>1024), in-filters can not be merged.
            if (in_filters.empty()) {
                can_merge_in_filters = false;
                break;
            }
            // move in-filter list indexed by i to slot indexed by k, eliminates holes in the middle.
            ++k;
            if (k < i) {
                _partial_in_filters[k] = std::move(_partial_in_filters[i]);
            }
            num_rows += _ht_row_counts[i];
        }

        can_merge_in_filters = can_merge_in_filters && (num_rows <= 1024) && k >= 0;
        if (!can_merge_in_filters) {
            _partial_in_filters[0].clear();
            return Status::OK();
        }
        // only merge k partial in-filter list
        _partial_in_filters.resize(k + 1);

        auto& total_in_filters = _partial_in_filters[0];
        for (auto i = 1; i < _partial_in_filters.size(); ++i) {
            auto& in_filters = _partial_in_filters[i];
            auto total_in_filter_it = total_in_filters.begin();
            auto in_filter_it = in_filters.begin();
            while (total_in_filter_it != total_in_filters.end()) {
                auto& total_in_filter = *(total_in_filter_it++);
                auto& in_filter = *(in_filter_it++);
                if (total_in_filter == nullptr || in_filter == nullptr) {
                    total_in_filter = nullptr;
                    continue;
                }
                auto* total_in_filter_pred = down_cast<Predicate*>(total_in_filter->root());
                auto* in_filter_pred = down_cast<Predicate*>(in_filter->root());
                RETURN_IF_ERROR(total_in_filter_pred->merge(in_filter_pred));
            }
        }
        std::remove(total_in_filters.begin(), total_in_filters.end(), nullptr);
        return Status::OK();
    }

    Status merge_bloom_filters() {
        if (_partial_bloom_filter_build_params.empty()) {
            return Status::OK();
        }
        size_t row_count = 0;
        for (auto count : _ht_row_counts) {
            row_count += count;
        }
        for (auto& desc : _bloom_filter_descriptors) {
            desc->set_is_pipeline(true);
            // skip if it does not have consumer.
            if (!desc->has_consumer()) continue;
            // skip if ht.size() > limit and it's only for local.
            if (!desc->has_remote_targets() && row_count > _limit) continue;
            PrimitiveType build_type = desc->build_expr_type();
            vectorized::JoinRuntimeFilter* filter =
                    vectorized::RuntimeFilterHelper::create_runtime_bloom_filter(_pool, build_type);
            if (filter == nullptr) continue;
            filter->init(row_count);
            filter->set_join_mode(desc->join_mode());
            desc->set_runtime_filter(filter);
        }
        for (auto& params : _partial_bloom_filter_build_params) {
            auto desc_it = _bloom_filter_descriptors.begin();
            auto param_it = params.begin();
            while (param_it != params.end()) {
                auto& desc = *(desc_it++);
                auto& param = *(param_it++);
                if (desc->runtime_filter() == nullptr) {
                    continue;
                }
                auto status = vectorized::RuntimeFilterHelper::fill_runtime_bloom_filter(
                        param.column, desc->build_expr_type(), desc->runtime_filter(), param.column_offset,
                        param.eq_null);
                if (!status.ok()) {
                    desc->set_runtime_filter(nullptr);
                }
            }
        }
        return Status::OK();
    }

private:
    ObjectPool* _pool;
    const size_t _limit;
    std::atomic<size_t> _num_active_builders;
    std::vector<size_t> _ht_row_counts;
    std::vector<RuntimeInFilters> _partial_in_filters;
    std::vector<RuntimeBloomFilterBuildParams> _partial_bloom_filter_build_params;
    RuntimeBloomFilters _bloom_filter_descriptors;
};

} // namespace pipeline
} // namespace starrocks