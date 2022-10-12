// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <ctime>

#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/source_operator.h"
#include "gutil/strings/substitute.h"
namespace starrocks {
namespace pipeline {

class Pipeline;
using PipelinePtr = std::shared_ptr<Pipeline>;
using Pipelines = std::vector<PipelinePtr>;
class Pipeline {
public:
    Pipeline() = delete;
    Pipeline(uint32_t id, const OpFactories& op_factories) : _id(id), _op_factories(op_factories) {
        _runtime_profile = std::make_shared<RuntimeProfile>(strings::Substitute("Pipeline (id=$0)", _id));
    }

    uint32_t get_id() const { return _id; }

    OpFactories& get_op_factories() { return _op_factories; }

    void add_op_factory(const OpFactoryPtr& op) { _op_factories.emplace_back(op); }

    Operators create_operators(int32_t degree_of_parallelism, int32_t i) {
        Operators operators;
        for (const auto& factory : _op_factories) {
            operators.emplace_back(factory->create(degree_of_parallelism, i));
        }
        return operators;
    }

    SourceOperatorFactory* source_operator_factory() {
        DCHECK(!_op_factories.empty());
        return down_cast<SourceOperatorFactory*>(_op_factories[0].get());
    }

    RuntimeProfile* runtime_profile() { return _runtime_profile.get(); }

    Status prepare(RuntimeState* state) {
        for (auto& op : _op_factories) {
            RETURN_IF_ERROR(op->prepare(state));
        }
        return Status::OK();
    }

    void close(RuntimeState* state) {
        for (auto& op : _op_factories) {
            op->close(state);
        }
    }

    std::string to_readable_string() const {
        std::stringstream ss;
        ss << "operator-chain: [";
        for (size_t i = 0; i < _op_factories.size(); ++i) {
            if (i == 0) {
                ss << _op_factories[i]->get_name();
            } else {
                ss << " -> " << _op_factories[i]->get_name();
            }
        }
        ss << "]";
        return ss.str();
    }

private:
    uint32_t _id = 0;
    std::shared_ptr<RuntimeProfile> _runtime_profile = nullptr;
    OpFactories _op_factories;
};

} // namespace pipeline
} // namespace starrocks
