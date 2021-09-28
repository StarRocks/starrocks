// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exprs/vectorized/runtime_filter_bank.h"

#include <thread>

#include "column/column.h"
#include "exprs/vectorized/in_const_predicate.hpp"
#include "gutil/strings/substitute.h"
#include "simd/simd.h"
#include "util/time.h"
namespace starrocks::vectorized {

// 0x1. initial global runtime filter impl
// 0x2. change simd-block-filter hash function.
static const uint8_t RF_VERSION = 0x2;

#define APPLY_FOR_ALL_PRIMITIVE_TYPE(M) \
    M(TYPE_TINYINT)                     \
    M(TYPE_SMALLINT)                    \
    M(TYPE_INT)                         \
    M(TYPE_BIGINT)                      \
    M(TYPE_LARGEINT)                    \
    M(TYPE_FLOAT)                       \
    M(TYPE_DOUBLE)                      \
    M(TYPE_VARCHAR)                     \
    M(TYPE_CHAR)                        \
    M(TYPE_DATE)                        \
    M(TYPE_DATETIME)                    \
    M(TYPE_DECIMALV2)                   \
    M(TYPE_DECIMAL32)                   \
    M(TYPE_DECIMAL64)                   \
    M(TYPE_DECIMAL128)                  \
    M(TYPE_BOOLEAN)

JoinRuntimeFilter* RuntimeFilterHelper::create_join_runtime_filter(ObjectPool* pool, PrimitiveType type) {
    JoinRuntimeFilter* filter = nullptr;
    switch (type) {
#define M(NAME)                                                 \
    case PrimitiveType::NAME: {                                 \
        filter = new RuntimeBloomFilter<PrimitiveType::NAME>(); \
        break;                                                  \
    }
        APPLY_FOR_ALL_PRIMITIVE_TYPE(M)
#undef M
    default:;
    }
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

ExprContext* RuntimeFilterHelper::create_runtime_in_filter(RuntimeState* state, ObjectPool* pool, Expr* probe_expr,
                                                           bool eq_null) {
    TExprNode node;
    PrimitiveType probe_type = probe_expr->type().type;

    // create TExprNode
    node.__set_use_vectorized(true);
    node.__set_node_type(TExprNodeType::IN_PRED);
    TScalarType tscalar_type;
    tscalar_type.__set_type(TPrimitiveType::BOOLEAN);
    TTypeNode ttype_node;
    ttype_node.__set_type(TTypeNodeType::SCALAR);
    ttype_node.__set_scalar_type(tscalar_type);
    TTypeDesc t_type_desc;
    t_type_desc.types.push_back(ttype_node);
    node.__set_type(t_type_desc);
    node.in_predicate.__set_is_not_in(false);
    node.__set_opcode(TExprOpcode::FILTER_IN);
    node.__isset.vector_opcode = true;
    node.__set_vector_opcode(to_in_opcode(probe_type));

    // create template of in-predicate.
    // and fill actual IN values later.
    switch (probe_type) {
#define M(NAME)                                                                               \
    case PrimitiveType::NAME: {                                                               \
        auto* in_pred = pool->add(new VectorizedInConstPredicate<PrimitiveType::NAME>(node)); \
        Status st = in_pred->prepare(state);                                                  \
        if (!st.ok()) return nullptr;                                                         \
        in_pred->add_child(Expr::copy(pool, probe_expr));                                     \
        in_pred->set_is_join_runtime_filter();                                                \
        in_pred->set_eq_null(eq_null);                                                        \
        auto* ctx = pool->add(new ExprContext(in_pred));                                      \
        return ctx;                                                                           \
    }
        APPLY_FOR_ALL_PRIMITIVE_TYPE(M)
#undef M
    default:
        return nullptr;
    }
}

Status RuntimeFilterHelper::fill_runtime_in_filter(const ColumnPtr& column, Expr* probe_expr, ExprContext* filter) {
    PrimitiveType type = probe_expr->type().type;
    Expr* expr = filter->root();

    DCHECK(column != nullptr);
    if (!column->is_nullable()) {
        switch (type) {
#define M(FIELD_TYPE)                                                                 \
    case PrimitiveType::FIELD_TYPE: {                                                 \
        using ColumnType = typename RunTimeTypeTraits<FIELD_TYPE>::ColumnType;        \
        auto* in_pre = (VectorizedInConstPredicate<FIELD_TYPE>*)(expr);               \
        auto& data_ptr = ColumnHelper::as_raw_column<ColumnType>(column)->get_data(); \
        for (size_t j = 1; j < data_ptr.size(); j++) {                                \
            in_pre->insert(&data_ptr[j]);                                             \
        }                                                                             \
        break;                                                                        \
    }
            APPLY_FOR_ALL_PRIMITIVE_TYPE(M)
#undef M
        default:;
        }
    } else {
        switch (type) {
#define M(FIELD_TYPE)                                                                                           \
    case PrimitiveType::FIELD_TYPE: {                                                                           \
        using ColumnType = typename RunTimeTypeTraits<FIELD_TYPE>::ColumnType;                                  \
        auto* in_pre = (VectorizedInConstPredicate<FIELD_TYPE>*)(expr);                                         \
        auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(column);                            \
        auto& data_array = ColumnHelper::as_raw_column<ColumnType>(nullable_column->data_column())->get_data(); \
        for (size_t j = 1; j < data_array.size(); j++) {                                                        \
            if (!nullable_column->is_null(j)) {                                                                 \
                in_pre->insert(&data_array[j]);                                                                 \
            } else {                                                                                            \
                in_pre->insert(nullptr);                                                                        \
            }                                                                                                   \
        }                                                                                                       \
        break;                                                                                                  \
    }
            APPLY_FOR_ALL_PRIMITIVE_TYPE(M)
#undef M
        default:;
        }
    }
    return Status::OK();
}

JoinRuntimeFilter* RuntimeFilterHelper::create_runtime_bloom_filter(ObjectPool* pool, PrimitiveType type) {
    JoinRuntimeFilter* filter = create_join_runtime_filter(pool, type);
    return filter;
}

Status RuntimeFilterHelper::fill_runtime_bloom_filter(const ColumnPtr& column, PrimitiveType type,
                                                      JoinRuntimeFilter* filter) {
    JoinRuntimeFilter* expr = filter;
    if (!column->is_nullable()) {
        switch (type) {
#define M(FIELD_TYPE)                                                                 \
    case PrimitiveType::FIELD_TYPE: {                                                 \
        using ColumnType = typename RunTimeTypeTraits<FIELD_TYPE>::ColumnType;        \
        auto* filter = (RuntimeBloomFilter<PrimitiveType::FIELD_TYPE>*)(expr);        \
        auto& data_ptr = ColumnHelper::as_raw_column<ColumnType>(column)->get_data(); \
        for (size_t j = 1; j < data_ptr.size(); j++) {                                \
            filter->insert(&data_ptr[j]);                                             \
        }                                                                             \
        break;                                                                        \
    }
            APPLY_FOR_ALL_PRIMITIVE_TYPE(M)
#undef M
        default:;
        }
    } else {
        switch (type) {
#define M(FIELD_TYPE)                                                                                           \
    case PrimitiveType::FIELD_TYPE: {                                                                           \
        using ColumnType = typename RunTimeTypeTraits<FIELD_TYPE>::ColumnType;                                  \
        auto* filter = (RuntimeBloomFilter<PrimitiveType::FIELD_TYPE>*)(expr);                                  \
        auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(column);                            \
        auto& data_array = ColumnHelper::as_raw_column<ColumnType>(nullable_column->data_column())->get_data(); \
        for (size_t j = 1; j < data_array.size(); j++) {                                                        \
            if (!nullable_column->is_null(j)) {                                                                 \
                filter->insert(&data_array[j]);                                                                 \
            } else {                                                                                            \
                filter->insert(nullptr);                                                                        \
            }                                                                                                   \
        }                                                                                                       \
        break;                                                                                                  \
    }
            APPLY_FOR_ALL_PRIMITIVE_TYPE(M)
#undef M
        default:;
        }
    }

    return Status::OK();
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

    RETURN_IF_ERROR(Expr::create_expr_tree(pool, desc.build_expr, &_build_expr_ctx));
    return Status::OK();
}

Status RuntimeFilterProbeDescriptor::init(ObjectPool* pool, const TRuntimeFilterDescription& desc,
                                          TPlanNodeId node_id) {
    _filter_id = desc.filter_id;
    _runtime_filter.store(nullptr);

    bool not_found = true;
    if (desc.__isset.plan_node_id_to_target_expr) {
        const auto& it = const_cast<TRuntimeFilterDescription&>(desc).plan_node_id_to_target_expr.find(node_id);
        if (it != desc.plan_node_id_to_target_expr.end()) {
            not_found = false;
            RETURN_IF_ERROR(Expr::create_expr_tree(pool, it->second, &_probe_expr_ctx));
        }
    }

    if (not_found) {
        return Status::NotFound("plan node id not found. node_id = " + std::to_string(node_id));
    }
    return Status::OK();
}

Status RuntimeFilterProbeDescriptor::prepare(RuntimeState* state, const RowDescriptor& row_desc, MemTracker* tracker,
                                             RuntimeProfile* p) {
    if (_probe_expr_ctx != nullptr) {
        RETURN_IF_ERROR(_probe_expr_ctx->prepare(state, row_desc, tracker));
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
                                                          MemTracker* tracker, ExprContext* new_probe_expr_ctx) {
    // close old probe expr
    _probe_expr_ctx->close(state);
    // create new probe expr and open it.
    _probe_expr_ctx = state->obj_pool()->add(new ExprContext(new_probe_expr_ctx->root()));
    _probe_expr_ctx->prepare(state, row_desc, tracker);
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
          _selectivity(std::move(that._selectivity)),
          _input_chunk_nums(that._input_chunk_nums),
          _wait_timeout_ms(that._wait_timeout_ms) {}

Status RuntimeFilterProbeCollector::prepare(RuntimeState* state, const RowDescriptor& row_desc, MemTracker* tracker,
                                            RuntimeProfile* profile) {
    _runtime_profile = profile;
    for (auto& it : _descriptors) {
        RuntimeFilterProbeDescriptor* rf_desc = it.second;
        RETURN_IF_ERROR(rf_desc->prepare(state, row_desc, tracker, profile));
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

void RuntimeFilterProbeCollector::do_evaluate(vectorized::Chunk* chunk) {
    if ((_input_chunk_nums++ & 31) == 0) {
        update_selectivity(chunk);
        return;
    }
    if (!_selectivity.empty()) {
        for (auto& kv : _selectivity) {
            RuntimeFilterProbeDescriptor* rf_desc = kv.second;
            const JoinRuntimeFilter* filter = rf_desc->runtime_filter();
            if (filter == nullptr) continue;
            ColumnPtr column = rf_desc->probe_expr_ctx()->evaluate(chunk);
            vectorized::Column::Filter& selection = filter->evaluate(column.get(), rf_desc->runtime_filter_ctx());
            _run_filter_nums += 1;
            size_t true_count = SIMD::count_nonzero(selection);

            if (true_count == 0) {
                chunk->set_num_rows(0);
                return;
            } else {
                chunk->filter(selection);
            }
        }
    }
}

void RuntimeFilterProbeCollector::evaluate(vectorized::Chunk* chunk) {
    if (_descriptors.empty()) return;
    size_t before = chunk->num_rows();
    if (before == 0) return;

    if (_join_runtime_filter_timer == nullptr) {
        init_counter();
    }
    {
        SCOPED_TIMER(_join_runtime_filter_timer);
        _join_runtime_filter_input_counter->update(before);
        _run_filter_nums = 0;
        do_evaluate(chunk);
        size_t after = chunk->num_rows();
        _join_runtime_filter_output_counter->update(after);
        _join_runtime_filter_eval_counter->update(_run_filter_nums);
    }
}

void RuntimeFilterProbeCollector::update_selectivity(vectorized::Chunk* chunk) {
    _selectivity.clear();
    size_t chunk_size = chunk->num_rows();
    vectorized::Column::Filter* selection = nullptr;
    for (auto& it : _descriptors) {
        RuntimeFilterProbeDescriptor* rf_desc = it.second;
        const JoinRuntimeFilter* filter = rf_desc->runtime_filter();
        if (filter == nullptr) continue;
        ColumnPtr column = rf_desc->probe_expr_ctx()->evaluate(chunk);
        vectorized::Column::Filter& new_selection = filter->evaluate(column.get(), rf_desc->runtime_filter_ctx());
        _run_filter_nums += 1;
        size_t true_count = SIMD::count_nonzero(new_selection);
        double selectivity = true_count * 1.0 / chunk_size;
        if (selectivity <= 0.5) {     // useful filter
            if (selectivity < 0.05) { // very useful filter, could early return
                _selectivity.clear();
                _selectivity.emplace(selectivity, rf_desc);
                chunk->filter(new_selection);
                return;
            }

            // Only choose three most selective runtime filters
            if (_selectivity.size() < 3) {
                _selectivity.emplace(selectivity, rf_desc);
            } else {
                auto it = _selectivity.end();
                it--;
                if (selectivity < it->first) {
                    _selectivity.erase(it);
                    _selectivity.emplace(selectivity, rf_desc);
                }
            }

            if (selection == nullptr) {
                selection = &new_selection;
            } else {
                // Merge selection
                uint8_t* dest = selection->data();
                const uint8_t* src = new_selection.data();
                for (size_t j = 0; j < chunk_size; ++j) {
                    dest[j] = src[j] & dest[j];
                }
            }
        }
    }
    if (!_selectivity.empty()) {
        chunk->filter(*selection);
    }
}

void RuntimeFilterProbeCollector::push_down(RuntimeFilterProbeCollector* parent,
                                            const std::vector<TupleId>& tuple_ids) {
    if (this == parent) return;
    auto iter = parent->_descriptors.begin();
    while (iter != parent->_descriptors.end()) {
        RuntimeFilterProbeDescriptor* desc = iter->second;
        if (desc->is_bound(tuple_ids)) {
            add_descriptor(desc);
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

void RuntimeFilterProbeCollector::init_counter() {
    _join_runtime_filter_timer = ADD_TIMER(_runtime_profile, "JoinRuntimeFilterTime");
    _join_runtime_filter_input_counter = ADD_COUNTER(_runtime_profile, "JoinRuntimeFilterInputRows", TUnit::UNIT);
    _join_runtime_filter_output_counter = ADD_COUNTER(_runtime_profile, "JoinRuntimeFilterOutputRows", TUnit::UNIT);
    _join_runtime_filter_eval_counter = ADD_COUNTER(_runtime_profile, "JoinRuntimeFilterEvaluate", TUnit::UNIT);
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
    _shared_runtime_filter = rf;
    set_runtime_filter(_shared_runtime_filter.get());
}

} // namespace starrocks::vectorized