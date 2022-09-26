// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/lookupjoin/index_seek_operator.h"

#include "column/chunk.h"
#include "exec/pipeline/operator.h"
#include "exec/vectorized/olap_scan_node.h"
#include "exprs/vectorized/binary_predicate.h"
#include "exprs/vectorized/literal.h"
#include "lookup_join_context.h"
#include "storage/chunk_helper.h"
#include "storage/predicate_parser.h"
#include "storage/vectorized_column_predicate.h"
#include "util/runtime_profile.h"

namespace starrocks::pipeline {

IndexSeekOperator::IndexSeekOperator(OperatorFactory* factory, int32_t id,
                                     int32_t plan_node_id, const int32_t driver_sequence,
                                     const TOlapScanNode& olap_scan_node,
                                     const vectorized::RuntimeFilterProbeCollector& runtime_filter_collector,
                                     std::shared_ptr<LookupJoinContext> lookup_join_context)
        : SourceOperator(factory, id, "index_seek", plan_node_id, driver_sequence),
          _olap_scan_node(olap_scan_node),
          _runtime_filter_collector(runtime_filter_collector),
          _lookup_join_context(lookup_join_context) {
    DCHECK(_lookup_join_context);
    _lookup_join_context->ref();
    _join_key_descs = _lookup_join_context->join_key_descs();
}

IndexSeekOperator::~IndexSeekOperator() {
    if (_reader) {
        _reader.reset();
    }
    _predicate_free_pool.clear();
}

Status IndexSeekOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::prepare(state));
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_olap_scan_node.tuple_id);
    if (_tuple_desc == nullptr) {
        return Status::InternalError("Failed to get tuple descriptor.");
    }
    _init_row_desc();
    RETURN_IF_ERROR(_prepare_tablet_reader(state));
    return Status::OK();
}

Status IndexSeekOperator::_init_tablet_reader(RuntimeState* state,
                                              TabletReaderState& reader_params) {
    VLOG(1) << "do prepare with dependency, _row_id:"<< std::to_string(_row_id) <<
            ", _probe_chunk size:" << (_probe_chunk ? std::to_string(_probe_chunk->num_rows()) : "0");
    if (_lookup_join_context->is_finished()) {
        VLOG(1) << "lookup join context finished";
        set_finishing(state);
        return Status::EndOfFile("");
    }
    if (!_probe_chunk || _row_id == _probe_chunk->num_rows()) {
        _row_id = 0;
        _probe_chunk = std::make_shared<vectorized::Chunk>();
        if (!_lookup_join_context->blocking_get(&_probe_chunk)) {
            // Go to next turn.
            return Status::OK();
        }
        VLOG(1) << "blocking get lookup join context finished 2";
    }
    DCHECK_LT(_row_id, _probe_chunk->num_rows());

    // TODO: Find a better way to iterator probe's chunk. Maybe introduce ParameterExpr later.
    // Make a new conjunct ctx
    RETURN_IF_ERROR(_init_conjunct_ctxs(reader_params, _row_id));
    // init conjncts manager

    // Make sure clear states.
    RETURN_IF_ERROR(_init_conjuncts_manager(state, reader_params));
    // _init_reader_params
    RETURN_IF_ERROR(_init_reader_params(state, reader_params));

    // TODO: must renew a reader to seek again???
    _reader = std::make_shared<TabletReader>(_tablet, Version(0, _version), _child_schema);
    RETURN_IF_ERROR(_reader->prepare());
    RETURN_IF_ERROR(_reader->open(*(reader_params.params)));
    _row_id++;

    return Status::OK();
}

void IndexSeekOperator::_init_row_desc() {
    auto& lookup_join_params = _lookup_join_context->params();
    auto& left_row_desc = lookup_join_params.left_row_desc;
    auto& right_row_desc = lookup_join_params.right_row_desc;
    _left_column_count = 0;
    _right_column_count = 0;
    _col_types.clear();
    for (auto& tuple_desc : left_row_desc.tuple_descriptors()) {
        for (auto& slot : tuple_desc->slots()) {
            _col_types.emplace_back(slot);
            _left_column_count++;
        }
    }
    for (auto& tuple_desc : right_row_desc.tuple_descriptors()) {
        for (auto& slot : tuple_desc->slots()) {
            _col_types.emplace_back(slot);
            _right_column_count++;
        }
    }
}

Expr* IndexSeekOperator::_create_eq_conjunct_expr(ObjectPool* pool, PrimitiveType ptype) {
    TExprNode expr_node;
    expr_node.opcode = TExprOpcode::EQ;;
    expr_node.child_type = to_thrift(ptype);
    expr_node.node_type = TExprNodeType::BINARY_PRED;
    expr_node.num_children = 2;
    expr_node.__isset.opcode = true;
    expr_node.__isset.child_type = true;
    expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    return pool->add(vectorized::VectorizedBinaryPredicateFactory::from_thrift(expr_node));
}

// TODO: how to construct a const column from one row?
Expr* IndexSeekOperator::_create_literal_expr(ObjectPool* pool, const ColumnPtr& input_column, uint32_t row_id,
                                              const TypeDescriptor& type_desc) {
    auto column = vectorized::ColumnHelper::create_column(type_desc, true);
    column->append_datum(input_column->get(row_id));
    auto const_column = std::make_shared<vectorized::ConstColumn>(std::move(column));
    return pool->add(new vectorized::VectorizedLiteral(std::move(const_column), type_desc));
}

// Construct a new context by using the new probe chunk with the row_id-th.
Status IndexSeekOperator::_init_conjunct_ctxs(TabletReaderState& reader_params,
                                              uint32_t row_id) {
    auto& conjunct_ctxs = reader_params.conjunct_ctxs;
    auto* pool = &(reader_params.pool);
    DCHECK_EQ(conjunct_ctxs.size(), _join_key_descs.size());
    for (int i = 0; i < _join_key_descs.size(); i++) {
        auto& desc = _join_key_descs[i];
        auto left_join_key_column_ref = desc.left_column_ref;
        auto* left_join_key_type = desc.left_join_key_type;
        auto left_column = _probe_chunk->get_column_by_slot_id(left_join_key_column_ref->slot_id());

        Expr* expr = _create_eq_conjunct_expr(pool, left_join_key_type->type);
        expr->add_child(const_cast<vectorized::ColumnRef*>(desc.right_column_ref));
        expr->add_child(_create_literal_expr(pool, left_column, row_id, *left_join_key_type));
        VLOG(1)<<"expr:"<<expr->debug_string();

        conjunct_ctxs.emplace_back(pool->add(new ExprContext(expr)));
    };
    VLOG(1) << "conjunct contexts:"<<Expr::debug_string(conjunct_ctxs);
    return Status::OK();
}

Status IndexSeekOperator::_get_tablet(const TInternalScanRange* scan_range) {
    _version = strtoul(scan_range->version.c_str(), nullptr, 10);
    ASSIGN_OR_RETURN(_tablet, vectorized::OlapScanNode::get_tablet(scan_range));
    return Status::OK();
}

Status IndexSeekOperator::_init_conjuncts_manager(RuntimeState* state,
                                                  TabletReaderState& reader_params) {
    vectorized::OlapScanConjunctsManager* cm = &(reader_params.conjuncts_manager);
    cm->conjunct_ctxs_ptr = &(reader_params.conjunct_ctxs);
    cm->tuple_desc = _tuple_desc;
    cm->obj_pool = &_obj_pool;
    cm->key_column_names = &_olap_scan_node.key_column_name;
    cm->runtime_filters = &_runtime_filter_collector;
    cm->runtime_state = state;

    // Parse conjuncts via _conjuncts_manager.
    RETURN_IF_ERROR(cm->parse_conjuncts(true, config::max_scan_key_num, true));
    // Get key_ranges and not_push_down_conjuncts from _conjuncts_manager.
    RETURN_IF_ERROR(cm->get_key_ranges(&(reader_params.key_ranges)));
    cm->get_not_push_down_conjuncts(&(reader_params.not_push_down_conjuncts));
    VLOG(1) << "reader_params.conjunct_ctxs:" << Expr::debug_string(reader_params.conjunct_ctxs);
    VLOG(1) << "reader_params.not_push_down_conjuncts:" << Expr::debug_string(reader_params.not_push_down_conjuncts);

    return Status::OK();
}

Status IndexSeekOperator::_init_reader_params(RuntimeState* state,
                                              TabletReaderState& reader_params) {
    auto& params = *(reader_params.params);
    auto& conjuncts_manager = reader_params.conjuncts_manager;
    auto& not_push_down_predicates = reader_params.not_push_down_predicates;
    auto& key_ranges= reader_params.key_ranges;

    params.is_pipeline = true;
    params.chunk_size = state->chunk_size();
    params.reader_type = READER_QUERY;
    params.skip_aggregation = true;
    params.runtime_state = state;
    params.use_page_cache = !config::disable_storage_page_cache;

    PredicateParser parser(_tablet->tablet_schema());
    std::vector<PredicatePtr> preds;
    RETURN_IF_ERROR(conjuncts_manager.get_column_predicates(&parser, &preds));
    for (auto& p : preds) {
        if (parser.can_pushdown(p.get())) {
            params.predicates.push_back(p.get());
        } else {
            not_push_down_predicates.add(p.get());
        }
        _predicate_free_pool.emplace_back(std::move(p));
    }

    // Range
    for (const auto& key_range : key_ranges) {
        if (key_range->begin_scan_range.size() == 1 && key_range->begin_scan_range.get_value(0) == NEGATIVE_INFINITY) {
            continue;
        }

        params.range = key_range->begin_include ? TabletReaderParams::RangeStartOperation::GE
                                                 : TabletReaderParams::RangeStartOperation::GT;
        params.end_range = key_range->end_include ? TabletReaderParams::RangeEndOperation::LE
                                                   : TabletReaderParams::RangeEndOperation::LT;

        params.start_key.push_back(key_range->begin_scan_range);
        params.end_key.push_back(key_range->end_scan_range);
    }

    return Status::OK();
}

Status IndexSeekOperator::_init_scanner_columns(std::vector<uint32_t>& scanner_columns) {
    auto slots = _tuple_desc->slots();
    for (auto slot : slots) {
        DCHECK(slot->is_materialized());
        int32_t index = _tablet->field_index(slot->col_name());
        if (index < 0) {
            std::stringstream ss;
            ss << "invalid field name: " << slot->col_name();
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        scanner_columns.push_back(index);
    }
    if (scanner_columns.empty()) {
        return Status::InternalError("failed to build storage scanner, no materialized slot!");
    }
    return Status::OK();
}

Status IndexSeekOperator::_prepare_tablet_reader(RuntimeState* state) {
    // init tablet
    auto scan_ranges = morsel_queue()->olap_scan_ranges();
    VLOG(1) << "scan range size:" << scan_ranges.size() << ", _version:" << _version;

    TInternalScanRange* scan_range;
    // TODO: Throw exception here.
    if (scan_ranges.size() != 1) {
        VLOG(1) << "!!!! scan range size:" << scan_ranges.size() << ", _version:" << _version;
        return Status::Corruption("scan range's size illegal: " + std::to_string(scan_ranges.size()));
    }
    scan_range = scan_ranges[0];
    RETURN_IF_ERROR(_get_tablet(scan_range));
    RETURN_IF_ERROR(_init_scanner_columns(_scanner_columns));
    VLOG(1) << " _version:" << _version <<",  _scanner_columns size:" << _scanner_columns.size();
    _child_schema = ChunkHelper::convert_schema_to_format_v2(_tablet->tablet_schema(), _scanner_columns);

    return Status::OK();
}

void IndexSeekOperator::close(RuntimeState* state) {
    _lookup_join_context->unref(state);
    _predicate_free_pool.clear();
    if (_reader) {
        _reader.reset();
    }
    SourceOperator::close(state);
}

vectorized::ChunkPtr IndexSeekOperator::_init_ouput_chunk(RuntimeState* state) {
    ChunkPtr chunk = std::make_shared<vectorized::Chunk>();
    for (size_t i = 0; i < _col_types.size(); i++) {
        SlotDescriptor* slot = _col_types[i];
        ColumnPtr new_col = vectorized::ColumnHelper::create_column(slot->type(), slot->is_nullable());
        chunk->append_column(new_col, slot->id());
    }
    chunk->reserve(state->chunk_size());
    return chunk;
}

vectorized::ChunkPtr IndexSeekOperator::_permute_output_chunk(RuntimeState* state) {
    // TODO: Construct lookup join's result: how to decompose with IndexSeek?
    // TODO: add Accumulate Chunk later.
    auto output_chunk = _init_ouput_chunk(state);
    if (_cur_chunk->num_rows() == 0) {
        return output_chunk;
    }

    auto num_rows = _cur_chunk->num_rows();
    for (int i = 0; i < _left_column_count; i++) {
        SlotDescriptor* slot = _col_types[i];
        ColumnPtr& dst_col = output_chunk->get_column_by_slot_id(slot->id());
        ColumnPtr& src_col = _probe_chunk->get_column_by_slot_id(slot->id());
        dst_col->append_value_multiple_times(*src_col, (_row_id - 1), num_rows);
    }
    for (int i = 0; i < _right_column_count; i++) {
        SlotDescriptor* slot = _col_types[i + _left_column_count];
        ColumnPtr& dst_col = output_chunk->get_column_by_slot_id(slot->id());
        ColumnPtr& src_col = _cur_chunk->get_column_by_slot_id(slot->id());
        VLOG(1) << "src_col:" << src_col->debug_string() << ", slot->id:" << slot->id() << ", i:"<<i;
        dst_col->append(*src_col);
    }
    for (size_t i = 0; i < output_chunk->num_rows(); i++) {
        VLOG(2) << "output_chunk output: " << output_chunk->debug_row(i);
    }
    return output_chunk;
}

Status IndexSeekOperator::_seek_row(RuntimeState* state) {
    do {
        if (_cur_eos) {
            // step1: prepare if needed
            _reader_state = std::make_shared<TabletReaderState>();
            RETURN_IF_ERROR(_init_tablet_reader(state, *_reader_state));
            VLOG(1) << "_probe_chunk output: " << _probe_chunk->debug_row(_row_id - 1);
            _cur_eos = false;
        }
        _cur_chunk.reset(ChunkHelper::new_chunk_pooled(_reader->output_schema(), state->chunk_size(), true));

        DCHECK(_reader);
        // seek the data
        auto read_schema = _reader->output_schema();
        for (auto& name: read_schema.field_names()) {
            VLOG(1) << "name:"<< name;
        }

        VLOG(1) << "do index seek pull_chunk";
        auto status = _reader->get_next(_cur_chunk.get());
        if (status.is_end_of_file()) {
            _cur_eos = true;
            if (_lookup_join_context->is_finished()) {
                VLOG(1) << "index seek finished :" << _cur_chunk->num_rows();
                set_finishing(state);
                return status;
            } else {
                continue;
            }
        } else {
            RETURN_IF_ERROR(status);
        }

        VLOG(1) << "_cur_chunk size:" << _cur_chunk->num_rows();
        if (_cur_chunk->is_empty()) {
            continue;
        }

        if (!(_reader_state->not_push_down_conjuncts).empty()) {
            RETURN_IF_ERROR(ExecNode::eval_conjuncts(_reader_state->not_push_down_conjuncts, &(*_cur_chunk)));
        }
    } while (_cur_chunk->num_rows() == 0);
    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> IndexSeekOperator::pull_chunk(RuntimeState* state) {
    if (state->is_cancelled()) {
        return Status::Cancelled("canceled state");
    }

    // seek one row
    RETURN_IF_ERROR(_seek_row(state));

    DCHECK_LT(0, _cur_chunk->num_rows());
    for (size_t i = 0; i < _cur_chunk->num_rows(); i++) {
        VLOG(2) << "_cur_chunk output: " << _cur_chunk->debug_row(i);
    }

    // output result
    return _permute_output_chunk(state);
}

Status IndexSeekOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
    return Status::OK();
}

} // namespace starrocks::pipeline
