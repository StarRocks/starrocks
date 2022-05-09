// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <utility>

#include "exec/pipeline/chunk_source.h"
#include "exec/pipeline/operator.h"

namespace starrocks {
namespace pipeline {
class SourceOperator;
using SourceOperatorPtr = std::shared_ptr<SourceOperator>;

class SourceOperator : public Operator {
public:
    SourceOperator(OperatorFactory* factory, int32_t id, std::string name, int32_t plan_node_id)
            : Operator(factory, id, name, plan_node_id) {}
    ~SourceOperator() override = default;

    bool need_input() const override { return false; }

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override {
        return Status::InternalError("Shouldn't push chunk to source operator");
    }

    virtual void add_morsel_queue(MorselQueue* morsel_queue) { _morsel_queue = morsel_queue; };

    const MorselQueue* morsel_queue() const { return _morsel_queue; }

    virtual int64_t get_last_scan_rows_num() {
        int64_t scan_rows_num = _last_scan_rows_num;
        _last_scan_rows_num = 0;
        return scan_rows_num;
    }

    virtual int64_t get_last_scan_bytes() {
        int64_t scan_rows_num = _last_scan_rows_num;
        _last_scan_rows_num = 0;
        return scan_rows_num;
    }

protected:
    MorselQueue* _morsel_queue = nullptr;

    int64_t _last_scan_rows_num = 0;
    int64_t _last_scan_bytes = 0;
};

class SourceOperatorFactory : public OperatorFactory {
public:
    SourceOperatorFactory(int32_t id, const std::string& name, int32_t plan_node_id)
            : OperatorFactory(id, name, plan_node_id) {}
    bool is_source() const override { return true; }
    // with_morsels returning true means that the SourceOperator needs attach to MorselQueue, only
    // OlapScanOperator needs to do so.
    virtual bool with_morsels() const { return false; }
    // Set the DOP(degree of parallelism) of the SourceOperator, SourceOperator's DOP determine the Pipeline's DOP.
    virtual void set_degree_of_parallelism(size_t degree_of_parallelism) {
        this->_degree_of_parallelism = degree_of_parallelism;
    }
    virtual size_t degree_of_parallelism() const { return _degree_of_parallelism; }

protected:
    size_t _degree_of_parallelism = 1;
};
} // namespace pipeline
} // namespace starrocks
