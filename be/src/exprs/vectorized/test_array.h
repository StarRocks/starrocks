//
// Created by gubichev on 1/25/23.
//

#pragma once
#include "exprs/vectorized/function_helper.h"
#include "udf/udf.h"


namespace starrocks {
namespace vectorized {

class TestArray {
public:
    DEFINE_VECTORIZED_FN(test_array);
};

} // namespace vectorized
} // namespace starrocks

