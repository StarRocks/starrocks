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

#include "primary_key_ops.h"

namespace starrocks {
std::string PrimaryKeyOps::get_op_string(bool is_delete) {
    return is_delete ? "1" : "0";
}

uint8_t PrimaryKeyOps::get_op_int(bool is_delete) {
    return is_delete ? 1 : 0;
}
} // namespace starrocks