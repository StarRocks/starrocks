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

#include "exec/raw_values_node.h"

#include "column/column_helper.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "gen_cpp/PlanNodes_types.h"
#include "glog/logging.h"

namespace starrocks {

RawValuesNode::RawValuesNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs), _tuple_id(tnode.raw_values_node.tuple_id) {}

RawValuesNode::~RawValuesNode() {
    if (runtime_state() != nullptr) {
        close(runtime_state());
    }
}

Status RawValuesNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));

    const auto& raw_values_node = tnode.raw_values_node;
    _constant_type = TypeDescriptor::from_thrift(raw_values_node.constant_type);

    if (raw_values_node.__isset.long_values && !raw_values_node.long_values.empty()) {
        _long_values = raw_values_node.long_values;
        DCHECK(_constant_type.is_integer_type() || _constant_type.type == TYPE_BIGINT);
    } else if (raw_values_node.__isset.string_values && !raw_values_node.string_values.empty()) {
        _string_values = raw_values_node.string_values;
        DCHECK(_constant_type.is_string_type());
    } else {
        LOG(ERROR) << "RawValuesNode::init - ERROR: no valid typed values found!"
                   << " long_values isset: " << raw_values_node.__isset.long_values
                   << " string_values isset: " << raw_values_node.__isset.string_values;
        return Status::InternalError("RawValuesNode: no valid typed values found");
    }

    return Status::OK();
}

Status RawValuesNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));

    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    if (_tuple_desc == nullptr) {
        return Status::InternalError("RawValuesNode: failed to get tuple descriptor");
    }

    return Status::OK();
}

Status RawValuesNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::open(state));
    return Status::OK();
}

Status RawValuesNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    size_t total_rows = _long_values.empty() ? _string_values.size() : _long_values.size();

    if (_next_row_index >= total_rows) {
        *eos = true;
        return Status::OK();
    }

    *chunk = std::make_shared<Chunk>();
    size_t rows_count = std::min(static_cast<size_t>(state->chunk_size()), total_rows - _next_row_index);

    DCHECK(_tuple_desc->slots().size() == 1);
    const auto* dst_slot = _tuple_desc->slots()[0];

    MutableColumnPtr dst_column = ColumnHelper::create_column(dst_slot->type(), dst_slot->is_nullable());
    dst_column->reserve(rows_count);

    if (!_long_values.empty()) {
        for (size_t i = 0; i < rows_count; i++) {
            dst_column->append_datum(Datum(_long_values[_next_row_index + i]));
        }
    } else {
        for (size_t i = 0; i < rows_count; i++) {
            const std::string& value = _string_values[_next_row_index + i];
            dst_column->append_datum(Datum(Slice(value)));
        }
    }

    (*chunk)->append_column(std::move(dst_column), dst_slot->id());
    _next_row_index += rows_count;

    _num_rows_returned += (*chunk)->num_rows();
    if (reached_limit()) {
        int64_t num_rows_over = _num_rows_returned - _limit;
        DCHECK_GE((*chunk)->num_rows(), num_rows_over);
        (*chunk)->set_num_rows((*chunk)->num_rows() - num_rows_over);
        COUNTER_SET(_rows_returned_counter, _limit);
        *eos = true;
    } else {
        *eos = (_next_row_index >= total_rows);
    }

    DCHECK_CHUNK(*chunk);
    return Status::OK();
}

void RawValuesNode::close(RuntimeState* state) {
    if (is_closed()) {
        return;
    }
    ExecNode::close(state);
}

pipeline::OpFactories RawValuesNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;
    OpFactories operators;

    const auto& dst_tuple_desc =
            context->fragment_context()->runtime_state()->desc_tbl().get_tuple_descriptor(_tuple_id);
    const auto& dst_slots = dst_tuple_desc->slots();
    size_t total_rows = _long_values.empty() ? _string_values.size() : _long_values.size();

    // Create RawValuesSourceOperatorFactory
    auto raw_values_source_op = std::make_shared<RawValuesSourceOperatorFactory>(
            context->next_operator_id(), id(), dst_slots, _constant_type, std::move(_long_values),
            std::move(_string_values));

    // Calculate appropriate parallelism
    size_t parallelism = std::min(context->degree_of_parallelism(),
                                  (total_rows + runtime_state()->chunk_size() - 1) / runtime_state()->chunk_size());

    raw_values_source_op->set_degree_of_parallelism(parallelism);

    operators.emplace_back(std::move(raw_values_source_op));

    if (limit() != -1) {
        operators.emplace_back(std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }

    return operators;
}

} // namespace starrocks
