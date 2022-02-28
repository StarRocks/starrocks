#include "exec/vectorized/jdbc_scan_node.h"

#include "exec/pipeline/jdbc_scan_operator.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exprs/expr_context.h"

namespace starrocks::vectorized {

JDBCScanNode::JDBCScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ScanNode(pool, tnode, descs), _jdbc_scan_node(tnode.jdbc_scan_node) {}

Status JDBCScanNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ScanNode::prepare(state));
    return Status::NotSupported(
            "querying jdbc table is not supported in vectorized engine, please set enable_pipeline_engine=true");
}

Status JDBCScanNode::open(RuntimeState* state) {
    DCHECK(false) << "unreachable path";
    return Status::NotSupported("Not supported in vectorized engine");
}

Status JDBCScanNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    DCHECK(false) << "unreachable path";
    return Status::NotSupported("Not supported in vectorized engine");
}

Status JDBCScanNode::close(RuntimeState* state) {
    return ScanNode::close(state);
}

Status JDBCScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    DCHECK(false) << "unreachable path";
    return Status::NotSupported("Not supported in vectorized engine");
}

pipeline::OpFactories JDBCScanNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;
    OpFactories operators;
    auto jdbc_scan_operator = std::make_shared<JDBCScanOperatorFactory>(
            context->next_operator_id(), id(), _jdbc_scan_node, std::move(_conjunct_ctxs), limit());
    jdbc_scan_operator->set_degree_of_parallelism(context->get_dop_of_scan_node(this->id()));
    operators.emplace_back(std::move(jdbc_scan_operator));
    if (limit() != -1) {
        operators.emplace_back(std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }
    operators = context->maybe_interpolate_local_passthrough_exchange(context->fragment_context()->runtime_state(),
                                                                      operators, context->degree_of_parallelism());
    return operators;
}

void JDBCScanNode::debug_string(int indentation_level, std::stringstream* out) const {
    DCHECK(false) << "unreachable path";
}
} // namespace starrocks::vectorized