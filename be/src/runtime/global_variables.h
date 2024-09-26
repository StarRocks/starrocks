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

#include "column/nullable_column.h"
#include "common/status.h"

namespace starrocks {

class GlobalVariables {
public:
    static GlobalVariables* GetInstance() {
        static GlobalVariables s_global_vars;
        return &s_global_vars;
    }

    GlobalVariables();
    ~GlobalVariables() { _is_init = false; }

    bool is_init() const { return _is_init; }

    const NullColumnPtr& one_size_not_null_column() const {
        DCHECK(_is_init);
        return _one_size_not_null_column;
    }
    const NullColumnPtr& one_size_null_column() const {
        DCHECK(_is_init);
        return _one_size_null_column;
    }

private:
    static bool _is_init;
    NullColumnPtr _one_size_not_null_column;
    NullColumnPtr _one_size_null_column;
};
} // namespace starrocks