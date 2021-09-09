// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exprs/percentile_function.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "percentile_function.h"

#include <udf/udf.h>

#include "util/tdigest.h"

namespace starrocks {

void PercentileFunctions::init() {}

StringVal PercentileFunctions::percentile_empty(FunctionContext* ctx) {
    PercentileValue percentile;
    StringVal result(ctx, percentile.serialize_size());
    percentile.serialize(result.ptr);
    return result;
}

StringVal PercentileFunctions::percentile_hash(FunctionContext* ctx, const DoubleVal& src) {
    PercentileValue percentile;
    if (!src.is_null) {
        percentile.add(src.val);
    }
    StringVal result(ctx, percentile.serialize_size());
    percentile.serialize(result.ptr);
    return result;
}

void PercentileFunctions::percentile_approx_update(FunctionContext* ctx, const StringVal& src,
                                                   const DoubleVal& quantile, StringVal* dst) {
    if (src.is_null) {
        return;
    }

    DCHECK(dst->ptr != NULL);

    PercentileApproxState* dst_percentile = reinterpret_cast<PercentileApproxState*>(dst->ptr);
    dst_percentile->targetQuantile = quantile.val;
    if (src.len == 0) {
        // zero size means the src input is a agg object
        dst_percentile->percentile->merge(reinterpret_cast<PercentileValue*>(src.ptr));
    } else {
        PercentileValue* percentile = new PercentileValue();
        percentile->deserialize((char*)src.ptr);
        dst_percentile->percentile->merge(percentile);
        delete percentile;
    }
}

DoubleVal PercentileFunctions::percentile_approx_raw(FunctionContext* ctx, const StringVal& src,
                                                     const DoubleVal& quantile) {
    if (src.is_null) {
        return DoubleVal::null();
    }

    PercentileValue percentile;
    percentile.deserialize((char*)src.ptr);
    return percentile.quantile(quantile.val);
}
} // namespace starrocks
