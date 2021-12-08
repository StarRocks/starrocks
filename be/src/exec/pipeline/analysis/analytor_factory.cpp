// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/analysis/analytor_factory.h"

namespace starrocks {
namespace pipeline {
AnalytorPtr AnalytorFactory::create(int i) {
    if (!_analytors[i]) {
        _analytors[i] = std::make_shared<Analytor>(_tnode, _child_row_desc, _result_tuple_desc);
    }
    return _analytors[i];
}
} // namespace pipeline
} // namespace starrocks