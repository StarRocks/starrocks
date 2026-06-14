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

#include <functional>

#include "common/global_types.h"
#include "common/statusor.h"

namespace starrocks {

class Expr;

class DictMappingExprInterface {
public:
    virtual ~DictMappingExprInterface() = default;

    virtual SlotId dict_mapping_slot_id() const = 0;
    virtual Expr* dict_mapping_origin_expr() const = 0;
    virtual Status rewrite_dict_mapping_expr(const std::function<StatusOr<Expr*>()>& rewriter) = 0;
    virtual void set_dict_mapping_output_id(SlotId id) = 0;
    virtual void disable_dict_mapping_open_rewrite() = 0;
};

} // namespace starrocks
