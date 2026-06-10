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

#pragma once

#include "exprs/function_context.h"
#include "exprs/function_helper.h"

namespace starrocks {

class DictFunctions {
public:
    // dict_encode(value, dict_slot_id): translate a constant to its global-dictionary code for the
    // column identified by dict_slot_id, resolved once from BE runtime state. Lets the
    // low-cardinality rewrite pre-encode a constant operand so a dict-aware comparison runs directly
    // on integer codes (e.g. array_contains(dict_array, dict_encode('foo', slot))).
    //
    //   value         VARCHAR or ARRAY<VARCHAR> constant.
    //   dict_slot_id  INT constant: the global-dict column id whose dictionary to use.
    //
    // A value (or element) absent from the dictionary encodes to a guaranteed-absent "no match"
    // code (size + 1). The global dict is complete for a dict-encoded column, so an absent value
    // cannot occur in the column -- this makes comparisons against it behave correctly (col = absent
    // -> false). All absent values share the same code (not distinguishable from one another), which
    // is fine since the code is only ever compared against the dict column. A NULL input value (or
    // NULL array element) encodes to NULL. A missing dictionary for dict_slot_id is an error.
    //
    // One implementation serves both overloads, dispatched on arg0's type:
    //   dict_encode(VARCHAR,        INT) -> INT          (scalar)
    //   dict_encode(ARRAY<VARCHAR>, INT) -> ARRAY<INT>   (each element encoded; NULL kept)
    DEFINE_VECTORIZED_FN(dict_encode);

    static Status dict_encode_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static Status dict_encode_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);
};

} // namespace starrocks
