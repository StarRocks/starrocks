// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exprs/vectorized/runtime_filter_bank.h"

#include <thread>

#include "column/column.h"
#include "exec/pipeline/runtime_filter_types.h"
#include "exprs/vectorized/in_const_predicate.hpp"
#include "exprs/vectorized/literal.h"
#include "exprs/vectorized/runtime_filter.h"
#include "gutil/strings/substitute.h"
#include "runtime/primitive_type.h"
#include "runtime/primitive_type_infra.h"
#include "simd/simd.h"
#include "util/time.h"

namespace starrocks::vectorized {

// 0x1. initial global runtime filter impl
// 0x2. change simd-block-filter hash function.
static const uint8_t RF_VERSION = 0x2;

struct FilterBuilder {
    template <PrimitiveType ptype>
    JoinRuntimeFilter* operator()() {
        return new RuntimeBloomFilter<ptype>();
    }
};

JoinRuntimeFilter* RuntimeFilterHelper::create_join_runtime_filter(ObjectPool* pool, PrimitiveType type) {
    JoinRuntimeFilter* filter = type_dispatch_filter(type, (JoinRuntimeFilter*)nullptr, FilterBuilder());
    if (pool != nullptr && filter != nullptr) {
        return pool->add(filter);
    } else {
        return filter;
    }
}

size_t RuntimeFilterHelper::max_runtime_filter_serialized_size(const JoinRuntimeFilter* rf) {
    size_t size = sizeof(RF_VERSION);
    size += rf->max_serialized_size();
    return size;
}
size_t RuntimeFilterHelper::serialize_runtime_filter(const JoinRuntimeFilter* rf, uint8_t* data) {
    size_t offset = 0;
    // put version at the head.
    memcpy(data + offset, &RF_VERSION, sizeof(RF_VERSION));
    offset += sizeof(RF_VERSION);
    offset += rf->serialize(data + offset);
    return offset;
}

void RuntimeFilterHelper::deserialize_runtime_filter(ObjectPool* pool, JoinRuntimeFilter** rf, const uint8_t* data,
                                                     size_t size) {
    *rf = nullptr;

    size_t offset = 0;

    // read version first.
    uint8_t version = 0;
    memcpy(&version, data, sizeof(version));
    offset += sizeof(version);
    if (version != RF_VERSION) {
        // version mismatch and skip this chunk.
        return;
    }

    // peek primitive type.
    PrimitiveType type;
    memcpy(&type, data + offset, sizeof(type));
    JoinRuntimeFilter* filter = create_join_runtime_filter(pool, type);
    DCHECK(filter != nullptr);
    if (filter != nullptr) {
        offset += filter->deserialize(data + offset);
        DCHECK(offset == size);
        *rf = filter;
    }
}

JoinRuntimeFilter* RuntimeFilterHelper::create_runtime_bloom_filter(ObjectPool* pool, PrimitiveType type) {
    JoinRuntimeFilter* filter = create_join_runtime_filter(pool, type);
    return filter;
}

struct FilterIniter {
    template <PrimitiveType ptype>
    auto operator()(const ColumnPtr& column, size_t column_offset, JoinRuntimeFilter* expr, bool eq_null) {
        using ColumnType = typename RunTimeTypeTraits<ptype>::ColumnType;
        auto* filter = (RuntimeBloomFilter<ptype>*)(expr);

        if (column->is_nullable()) {
            auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(column);
            auto& data_array = ColumnHelper::as_raw_column<ColumnType>(nullable_column->data_column())->get_data();
            for (size_t j = column_offset; j < data_array.size(); j++) {
                if (!nullable_column->is_null(j)) {
                    filter->insert(&data_array[j]);
                } else {
                    if (eq_null) {
                        filter->insert(nullptr);
                    }
                }
            }

        } else {
            auto& data_ptr = ColumnHelper::as_raw_column<ColumnType>(column)->get_data();
            for (size_t j = column_offset; j < data_ptr.size(); j++) {
                filter->insert(&data_ptr[j]);
            }
        }
        return nullptr;
    }
};

Status RuntimeFilterHelper::fill_runtime_bloom_filter(const ColumnPtr& column, PrimitiveType type,
                                                      JoinRuntimeFilter* filter, size_t column_offset, bool eq_null) {
    type_dispatch_filter(type, nullptr, FilterIniter(), column, column_offset, filter, eq_null);
    return Status::OK();
}

StatusOr<ExprContext*> RuntimeFilterHelper::rewrite_as_runtime_filter(ObjectPool* pool, ExprContext* conjunct,
                                                                      Chunk* chunk) {
    auto left_child = conjunct->root()->get_child(0);
    auto right_child = conjunct->root()->get_child(1);
    // all of the child(1) in expr is in build chunk
    ASSIGN_OR_RETURN(auto res, conjunct->evaluate(right_child, chunk));
    DCHECK_EQ(res->size(), 1);
    ColumnPtr col;
    if (res->is_constant()) {
        col = res;
    } else if (res->is_nullable()) {
        if (res->is_null(0)) {
            col = ColumnHelper::create_const_null_column(1);
        } else {
            auto data_col = down_cast<NullableColumn*>(res.get())->data_column();
            col = std::make_shared<ConstColumn>(data_col, 1);
        }
    } else {
        col = std::make_shared<ConstColumn>(res, 1);
    }

    auto literal = pool->add(new VectorizedLiteral(std::move(col), right_child->type()));
    auto new_expr = conjunct->root()->clone(pool);
    auto new_left = left_child->clone(pool);
    new_expr->clear_children();
    new_expr->add_child(new_left);
    new_expr->add_child(literal);
    return pool->add(new ExprContext(new_expr));
}

Status RuntimeFilterBuildDescriptor::init(ObjectPool* pool, const TRuntimeFilterDescription& desc) {
    _filter_id = desc.filter_id;
    _build_expr_order = desc.expr_order;
    _has_remote_targets = desc.has_remote_targets;
    _join_mode = desc.build_join_mode;

    if (desc.__isset.runtime_filter_merge_nodes) {
        _merge_nodes = desc.runtime_filter_merge_nodes;
    }
    _has_consumer = false;
    if (desc.__isset.plan_node_id_to_target_expr && desc.plan_node_id_to_target_expr.size() != 0) {
        _has_consumer = true;
    }
    if (!desc.__isset.build_expr) {
        return Status::NotFound("build_expr not found");
    }
    if (desc.__isset.sender_finst_id) {
        _sender_finst_id = desc.sender_finst_id;
    }
    if (desc.__isset.broadcast_grf_senders) {
        _broadcast_grf_senders.insert(desc.broadcast_grf_senders.begin(), desc.broadcast_grf_senders.end());
    }
    if (desc.__isset.broadcast_grf_destinations) {
        _broadcast_grf_destinations = desc.broadcast_grf_destinations;
    }

    RETURN_IF_ERROR(Expr::create_expr_tree(pool, desc.build_expr, &_build_expr_ctx));
    return Status::OK();
}

Status RuntimeFilterProbeDescriptor::init(ObjectPool* pool, const TRuntimeFilterDescription& desc,
                                          TPlanNodeId node_id) {
    _filter_id = desc.filter_id;
    _is_local = !desc.has_remote_targets;
    _build_plan_node_id = desc.build_plan_node_id;
    _runtime_filter.store(nullptr);
    _join_mode = desc.build_join_mode;

    bool not_found = true;
    if (desc.__isset.plan_node_id_to_target_expr) {
        const auto& it = const_cast<TRuntimeFilterDescription&>(desc).plan_node_id_to_target_expr.find(node_id);
        if (it != desc.plan_node_id_to_target_expr.end()) {
            not_found = false;
            RETURN_IF_ERROR(Expr::create_expr_tree(pool, it->second, &_probe_expr_ctx));
        }
    }

    if (desc.__isset.bucketseq_to_instance) {
        _bucketseq_to_partition = desc.bucketseq_to_instance;
    }

    if (not_found) {
        return Status::NotFound("plan node id not found. node_id = " + std::to_string(node_id));
    }
    return Status::OK();
}

Status RuntimeFilterProbeDescriptor::prepare(RuntimeState* state, const RowDescriptor& row_desc, RuntimeProfile* p) {
    if (_probe_expr_ctx != nullptr) {
        RETURN_IF_ERROR(_probe_expr_ctx->prepare(state));
    }
    _open_timestamp = UnixMillis();
    _latency_timer = ADD_COUNTER(p, strings::Substitute("JoinRuntimeFilter/$0/latency", _filter_id), TUnit::TIME_NS);
    // not set yet.
    _latency_timer->set((int64_t)(-1));
    return Status::OK();
}

Status RuntimeFilterProbeDescriptor::open(RuntimeState* state) {
    if (_probe_expr_ctx != nullptr) {
        RETURN_IF_ERROR(_probe_expr_ctx->open(state));
    }
    return Status::OK();
}

void RuntimeFilterProbeDescriptor::close(RuntimeState* state) {
    if (_probe_expr_ctx != nullptr) {
        _probe_expr_ctx->close(state);
    }
}

void RuntimeFilterProbeDescriptor::replace_probe_expr_ctx(RuntimeState* state, const RowDescriptor& row_desc,
                                                          ExprContext* new_probe_expr_ctx) {
    // close old probe expr
    _probe_expr_ctx->close(state);
    // create new probe expr and open it.
    _probe_expr_ctx = state->obj_pool()->add(new ExprContext(new_probe_expr_ctx->root()));
    _probe_expr_ctx->prepare(state);
    _probe_expr_ctx->open(state);
}

std::string RuntimeFilterProbeDescriptor::debug_string() const {
    std::stringstream ss;
    ss << "RFDptr(filter_id=" << _filter_id << ", probe_expr=";
    if (_probe_expr_ctx != nullptr) {
        ss << "(addr = " << _probe_expr_ctx << ", expr = " << _probe_expr_ctx->root()->debug_string() << ")";
    } else {
        ss << "nullptr";
    }
    ss << ", rf=";
    const JoinRuntimeFilter* rf = _runtime_filter.load();
    if (rf != nullptr) {
        ss << rf->debug_string();
    } else {
        ss << "nullptr";
    }
    ss << ")";
    return ss.str();
}

static const int default_runtime_filter_wait_timeout_ms = 1000;

RuntimeFilterProbeCollector::RuntimeFilterProbeCollector() : _wait_timeout_ms(default_runtime_filter_wait_timeout_ms) {}

RuntimeFilterProbeCollector::RuntimeFilterProbeCollector(RuntimeFilterProbeCollector&& that) noexcept
        : _descriptors(std::move(that._descriptors)),
          _wait_timeout_ms(that._wait_timeout_ms),
          _scan_wait_timeout_ms(that._scan_wait_timeout_ms),
          _eval_context(that._eval_context),
          _plan_node_id(that._plan_node_id) {}

Status RuntimeFilterProbeCollector::prepare(RuntimeState* state, const RowDescriptor& row_desc,
                                            RuntimeProfile* profile) {
    _runtime_profile = profile;
    for (auto& it : _descriptors) {
        RuntimeFilterProbeDescriptor* rf_desc = it.second;
        RETURN_IF_ERROR(rf_desc->prepare(state, row_desc, profile));
    }
    return Status::OK();
}
Status RuntimeFilterProbeCollector::open(RuntimeState* state) {
    for (auto& it : _descriptors) {
        RuntimeFilterProbeDescriptor* rf_desc = it.second;
        RETURN_IF_ERROR(rf_desc->open(state));
    }
    return Status::OK();
}
void RuntimeFilterProbeCollector::close(RuntimeState* state) {
    for (auto& it : _descriptors) {
        RuntimeFilterProbeDescriptor* rf_desc = it.second;
        rf_desc->close(state);
    }
}

// do_evaluate is reentrant, can be called concurrently by multiple operators that shared the same
// RuntimeFilterProbeCollector.
void RuntimeFilterProbeCollector::do_evaluate(vectorized::Chunk* chunk, RuntimeBloomFilterEvalContext& eval_context) {
    if ((eval_context.input_chunk_nums++ & 31) == 0) {
        update_selectivity(chunk, eval_context);
        return;
    }
    if (!eval_context.selectivity.empty()) {
        auto& selection = eval_context.running_context.selection;
        eval_context.running_context.use_merged_selection = false;
        for (auto& kv : eval_context.selectivity) {
            RuntimeFilterProbeDescriptor* rf_desc = kv.second;
            const JoinRuntimeFilter* filter = rf_desc->runtime_filter();
            if (filter == nullptr) {
                continue;
            }
            auto* ctx = rf_desc->probe_expr_ctx();
            ColumnPtr column = EVALUATE_NULL_IF_ERROR(ctx, ctx->root(), chunk);
            // for colocate grf
            eval_context.running_context.bucketseq_to_partition = rf_desc->bucketseq_to_partition();
            filter->evaluate(column.get(), &eval_context.running_context);

            auto true_count = SIMD::count_nonzero(selection);
            eval_context.run_filter_nums += 1;

            if (true_count == 0) {
                chunk->set_num_rows(0);
                return;
            } else {
                chunk->filter(selection);
            }
        }
    }
}
void RuntimeFilterProbeCollector::init_counter() {
    _eval_context.join_runtime_filter_timer = ADD_TIMER(_runtime_profile, "JoinRuntimeFilterTime");
    _eval_context.join_runtime_filter_input_counter =
            ADD_COUNTER(_runtime_profile, "JoinRuntimeFilterInputRows", TUnit::UNIT);
    _eval_context.join_runtime_filter_output_counter =
            ADD_COUNTER(_runtime_profile, "JoinRuntimeFilterOutputRows", TUnit::UNIT);
    _eval_context.join_runtime_filter_eval_counter =
            ADD_COUNTER(_runtime_profile, "JoinRuntimeFilterEvaluate", TUnit::UNIT);
}

void RuntimeFilterProbeCollector::evaluate(vectorized::Chunk* chunk) {
    if (_descriptors.empty()) return;
    if (_eval_context.join_runtime_filter_timer == nullptr) {
        init_counter();
    }
    evaluate(chunk, _eval_context);
}

void RuntimeFilterProbeCollector::evaluate(vectorized::Chunk* chunk, RuntimeBloomFilterEvalContext& eval_context) {
    if (_descriptors.empty()) return;
    size_t before = chunk->num_rows();
    if (before == 0) return;

    {
        SCOPED_TIMER(eval_context.join_runtime_filter_timer);
        eval_context.join_runtime_filter_input_counter->update(before);
        eval_context.run_filter_nums = 0;
        do_evaluate(chunk, eval_context);
        size_t after = chunk->num_rows();
        eval_context.join_runtime_filter_output_counter->update(after);
        eval_context.join_runtime_filter_eval_counter->update(eval_context.run_filter_nums);
    }
}

void RuntimeFilterProbeCollector::update_selectivity(vectorized::Chunk* chunk,
                                                     RuntimeBloomFilterEvalContext& eval_context) {
    eval_context.selectivity.clear();
    size_t chunk_size = chunk->num_rows();
    auto& merged_selection = eval_context.running_context.merged_selection;
    auto& use_merged_selection = eval_context.running_context.use_merged_selection;
    use_merged_selection = true;
    for (auto& it : _descriptors) {
        RuntimeFilterProbeDescriptor* rf_desc = it.second;
        const JoinRuntimeFilter* filter = rf_desc->runtime_filter();
        if (filter == nullptr) {
            continue;
        }
        auto& selection = eval_context.running_context.use_merged_selection
                                  ? eval_context.running_context.merged_selection
                                  : eval_context.running_context.selection;
        auto ctx = rf_desc->probe_expr_ctx();
        ColumnPtr column = EVALUATE_NULL_IF_ERROR(ctx, ctx->root(), chunk);
        // for colocate grf
        eval_context.running_context.bucketseq_to_partition = rf_desc->bucketseq_to_partition();
        // true count is not accummulated, it is evaluated for each RF respectively
        filter->evaluate(column.get(), &eval_context.running_context);
        auto true_count = SIMD::count_nonzero(selection);
        eval_context.run_filter_nums += 1;
        double selectivity = true_count * 1.0 / chunk_size;
        if (selectivity <= 0.5) {     // useful filter
            if (selectivity < 0.05) { // very useful filter, could early return
                eval_context.selectivity.clear();
                eval_context.selectivity.emplace(selectivity, rf_desc);
                chunk->filter(selection);
                return;
            }

            // Only choose three most selective runtime filters
            if (eval_context.selectivity.size() < 3) {
                eval_context.selectivity.emplace(selectivity, rf_desc);
            } else {
                auto it = eval_context.selectivity.end();
                it--;
                if (selectivity < it->first) {
                    eval_context.selectivity.erase(it);
                    eval_context.selectivity.emplace(selectivity, rf_desc);
                }
            }

            if (use_merged_selection) {
                use_merged_selection = false;
            } else {
                uint8_t* dest = merged_selection.data();
                const uint8_t* src = selection.data();
                for (size_t j = 0; j < chunk_size; ++j) {
                    dest[j] = src[j] & dest[j];
                }
            }
        }
    }
    if (!eval_context.selectivity.empty()) {
        chunk->filter(merged_selection);
    }
}

void RuntimeFilterProbeCollector::push_down(RuntimeFilterProbeCollector* parent, const std::vector<TupleId>& tuple_ids,
                                            std::set<TPlanNodeId>& local_rf_waiting_set) {
    if (this == parent) return;
    auto iter = parent->_descriptors.begin();
    while (iter != parent->_descriptors.end()) {
        RuntimeFilterProbeDescriptor* desc = iter->second;
        if (desc->is_bound(tuple_ids)) {
            add_descriptor(desc);
            if (desc->is_local()) {
                local_rf_waiting_set.insert(desc->build_plan_node_id());
            }
            iter = parent->_descriptors.erase(iter);
        } else {
            ++iter;
        }
    }
}

std::string RuntimeFilterProbeCollector::debug_string() const {
    std::stringstream ss;
    ss << "RFColl(";
    for (auto& it : _descriptors) {
        RuntimeFilterProbeDescriptor* desc = it.second;
        if (desc != nullptr) {
            ss << "[" << desc->debug_string() << "]";
        }
    }
    ss << ")";
    return ss.str();
}

void RuntimeFilterProbeCollector::add_descriptor(RuntimeFilterProbeDescriptor* desc) {
    _descriptors[desc->filter_id()] = desc;
}

void RuntimeFilterProbeCollector::wait() {
    if (_descriptors.empty()) return;

    std::list<RuntimeFilterProbeDescriptor*> wait_list;
    for (auto& it : _descriptors) {
        wait_list.push_back(it.second);
    }

    int wait_time = _wait_timeout_ms;
    const int wait_interval = 5;
    auto wait_duration = std::chrono::milliseconds(wait_interval);
    while (wait_time >= 0 && !wait_list.empty()) {
        auto it = wait_list.begin();
        while (it != wait_list.end()) {
            auto* rf = (*it)->runtime_filter();
            if (rf != nullptr) {
                it = wait_list.erase(it);
            } else {
                ++it;
            }
        }
        if (wait_list.empty()) break;
        std::this_thread::sleep_for(wait_duration);
        wait_time -= wait_interval;
    }

    if (_descriptors.size() != 0) {
        for (const auto& it : _descriptors) {
            auto* rf = it.second;
            int filter_id = rf->filter_id();
            bool ready = (rf->runtime_filter() != nullptr);
            VLOG_FILE << "RuntimeFilterCollector::wait. filter_id = " << filter_id
                      << ", ready = " << std::to_string(ready);
        }
    }
}

void RuntimeFilterProbeDescriptor::set_runtime_filter(const JoinRuntimeFilter* rf) {
    const JoinRuntimeFilter* expected = nullptr;
    _runtime_filter.compare_exchange_strong(expected, rf, std::memory_order_seq_cst, std::memory_order_seq_cst);
    if (_ready_timestamp == 0 && rf != nullptr && _latency_timer != nullptr) {
        _ready_timestamp = UnixMillis();
        _latency_timer->set((_ready_timestamp - _open_timestamp) * 1000);
    }
}
void RuntimeFilterProbeDescriptor::set_shared_runtime_filter(const std::shared_ptr<const JoinRuntimeFilter>& rf) {
    std::shared_ptr<const JoinRuntimeFilter> old_value = nullptr;
    if (std::atomic_compare_exchange_strong(&_shared_runtime_filter, &old_value, rf)) {
        set_runtime_filter(_shared_runtime_filter.get());
    }
}

} // namespace starrocks::vectorized
