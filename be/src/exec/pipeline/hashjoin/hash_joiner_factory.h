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

#include <memory>
#include <vector>

#include "exec/hash_joiner.h"

namespace starrocks::pipeline {

using HashJoiner = starrocks::HashJoiner;
using HashJoinerPtr = std::shared_ptr<HashJoiner>;
using HashJoinerMap = std::unordered_map<int32_t, HashJoinerPtr>;
class HashJoinerFactory;
using HashJoinerFactoryPtr = std::shared_ptr<HashJoinerFactory>;

class HashJoinerFactory {
public:
    HashJoinerFactory(starrocks::HashJoinerParam& param) : _param(param) {}

    Status prepare(RuntimeState* state);
    void close(RuntimeState* state);

    /// We must guarantee that:
    /// 1. All the builders must be created earlier than prober.
    /// 2. prober_dop is a multiple of builder_dop.
    HashJoinerPtr create_builder(int32_t builder_dop, int32_t builder_driver_seq);
    HashJoinerPtr create_prober(int32_t prober_dop, int32_t prober_driver_seq);
    HashJoinerPtr get_builder(int32_t prober_dop, int32_t prober_driver_seq);

    const starrocks::HashJoinerParam& hash_join_param() { return _param; }

private:
    HashJoinerPtr _create_joiner(HashJoinerMap& joiner_map, int32_t driver_sequence);

    HashJoinerParam _param;
    HashJoinerMap _builder_map;
    HashJoinerMap _prober_map;

    int32_t _builder_dop = 0;
    int32_t _prober_dop = 0;
};

} // namespace starrocks::pipeline
