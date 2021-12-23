// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once
#include "exec/vectorized/analytor.h"
namespace starrocks {
namespace pipeline {
class AnalytorFactory;
using AnalytorFactoryPtr = std::shared_ptr<AnalytorFactory>;
class AnalytorFactory {
public:
    AnalytorFactory(size_t dop, const TPlanNode& tnode, const RowDescriptor& child_row_desc,
                    const TupleDescriptor* result_tuple_desc)
            : _analytors(dop), _tnode(tnode), _child_row_desc(child_row_desc), _result_tuple_desc(result_tuple_desc) {}
    AnalytorPtr create(int i);

private:
    Analytors _analytors;
    const TPlanNode& _tnode;
    const RowDescriptor& _child_row_desc;
    const TupleDescriptor* _result_tuple_desc;
};

} // namespace pipeline
} // namespace starrocks