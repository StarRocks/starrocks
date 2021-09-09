// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/pipeline/operator.h"

namespace starrocks {
class MemTracker;
namespace pipeline {

class Pipeline;
using PipelinePtr = std::shared_ptr<Pipeline>;
using Pipelines = std::vector<PipelinePtr>;

class Pipeline {
public:
    Pipeline() = default;
    Pipeline(uint32_t id, uint32_t driver_instance_count, const OpFactories& op_factories)
            : _id(id), _driver_instance_count(driver_instance_count), _op_factories(std::move(op_factories)) {}

    uint32_t get_id() const { return _id; }

    uint32_t get_driver_instance_count() const { return _driver_instance_count; }

    OpFactories& get_op_factories() { return _op_factories; }

    void add_op_factory(const OpFactoryPtr& op) { _op_factories.emplace_back(op); }

private:
    uint32_t _id = 0;
    uint32_t _driver_instance_count = 1;
    OpFactories _op_factories;
};

} // namespace pipeline
} // namespace starrocks