#include "exec/pipeline/bucket_process_operator.h"

#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace starrocks::pipeline {

Status BucketProcessContext::reset_operator_state(RuntimeState* state) {
    RETURN_IF_ERROR(source->reset_state(state, {}));
    RETURN_IF_ERROR(sink->reset_state(state, {}));
    return Status::OK();
}

Status BucketProcessSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    RETURN_IF_ERROR(_ctx->sink->prepare(state));
    return Status::OK();
}

void BucketProcessSinkOperator::close(RuntimeState* state) {
    _ctx->sink->close(state);
}

bool BucketProcessSinkOperator::need_input() const {
    if (_ctx->current_bucket_sink_finished) {
        return false;
    }
    return _ctx->sink->need_input();
}

bool BucketProcessSinkOperator::is_finished() const {
    return _ctx->all_input_finishing && _ctx->sink->is_finished();
}

Status BucketProcessSinkOperator::set_finishing(RuntimeState* state) {
    _ctx->all_input_finishing = true;
    bool token = _ctx->token;
    if (!token && _ctx->token.compare_exchange_strong(token, true)) {
        RETURN_IF_ERROR(_ctx->sink->set_finishing(state));
        _ctx->current_bucket_sink_finished = true;
    }

    return Status::OK();
}

Status BucketProcessSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    auto info = chunk->owner_info();
    if (!chunk->is_empty()) {
        RETURN_IF_ERROR(_ctx->sink->push_chunk(state, chunk));
    }
    // short-circuit case. such as group by limit
    if (_ctx->sink->is_finished()) {
        _ctx->all_input_finishing = true;
        return Status::OK();
    }
    if (info.is_last_chunk()) {
        RETURN_IF_ERROR(_ctx->sink->set_finishing(state));
        _ctx->current_bucket_sink_finished = true;
    }
    return Status::OK();
}

Status BucketProcessSourceOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    return _ctx->source->prepare(state);
}

bool BucketProcessSourceOperator::has_output() const {
    return _ctx->current_bucket_sink_finished && (_ctx->source->has_output() || _ctx->source->is_finished());
}
bool BucketProcessSourceOperator::is_finished() const {
    return _ctx->all_input_finishing && _ctx->source->is_finished();
}
Status BucketProcessSourceOperator::set_finished(RuntimeState* state) {
    _ctx->finished = true;
    RETURN_IF_ERROR(_ctx->source->set_finished(state));
    return Status::OK();
}
void BucketProcessSourceOperator::close(RuntimeState* state) {
    _ctx->source->close(state);
}

StatusOr<ChunkPtr> BucketProcessSourceOperator::pull_chunk(RuntimeState* state) {
    ChunkPtr chunk;
    if (_ctx->source->has_output()) {
        ASSIGN_OR_RETURN(chunk, _ctx->source->pull_chunk(state));
    }

    if (!_ctx->all_input_finishing && _ctx->source->is_finished()) {
        bool token = _ctx->token;
        if (!token && _ctx->token.compare_exchange_strong(token, true)) {
            RETURN_IF_ERROR(_ctx->reset_operator_state(state));
            if (_ctx->all_input_finishing) {
                RETURN_IF_ERROR(_ctx->sink->set_finishing(state));
                _ctx->current_bucket_sink_finished = true;
            } else {
                _ctx->current_bucket_sink_finished = false;
            }
            _ctx->token = false;
        }
    }

    return chunk;
}

BucketProcessSinkOperatorFactory::BucketProcessSinkOperatorFactory(
        int32_t id, int32_t plan_node_id, const BucketProcessContextFactoryPtr& context_factory,
        const OperatorFactoryPtr& factory)
        : OperatorFactory(id, "bucket_process_sink_factory", plan_node_id),
          _factory(factory),
          _ctx_factory(context_factory) {}

OperatorPtr BucketProcessSinkOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    auto ctx = _ctx_factory->get_or_create(driver_sequence);
    ctx->sink = _factory->create(degree_of_parallelism, driver_sequence);
    auto bucket_source_operator =
            std::make_shared<BucketProcessSinkOperator>(this, _id, _plan_node_id, driver_sequence, ctx);
    return bucket_source_operator;
}

Status BucketProcessSinkOperatorFactory::prepare(RuntimeState* state) {
    return _factory->prepare(state);
}

void BucketProcessSinkOperatorFactory::close(RuntimeState* state) {
    _factory->close(state);
}

BucketProcessSourceOperatorFactory::BucketProcessSourceOperatorFactory(
        int32_t id, int32_t plan_node_id, const BucketProcessContextFactoryPtr& context_factory,
        const OperatorFactoryPtr& factory)
        : SourceOperatorFactory(id, "bucket_process_factory", plan_node_id),
          _factory(factory),
          _ctx_factory(context_factory) {}

OperatorPtr BucketProcessSourceOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    auto ctx = _ctx_factory->get_or_create(driver_sequence);
    ctx->source = _factory->create(degree_of_parallelism, driver_sequence);
    auto bucket_source_operator =
            std::make_shared<BucketProcessSourceOperator>(this, _id, _plan_node_id, driver_sequence, ctx);
    return bucket_source_operator;
}

Status BucketProcessSourceOperatorFactory::prepare(RuntimeState* state) {
    return _factory->prepare(state);
}

void BucketProcessSourceOperatorFactory::close(RuntimeState* state) {
    _factory->close(state);
}

} // namespace starrocks::pipeline