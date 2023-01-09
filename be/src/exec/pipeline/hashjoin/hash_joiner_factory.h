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
using HashJoiners = std::vector<HashJoinerPtr>;
class HashJoinerFactory;
using HashJoinerFactoryPtr = std::shared_ptr<HashJoinerFactory>;

class HashJoinerFactory {
public:
    HashJoinerFactory(starrocks::HashJoinerParam& param, int dop) : _param(param), _hash_joiners(dop) {}

    Status prepare(RuntimeState* state);
    void close(RuntimeState* state);

    HashJoinerPtr create_prober(int driver_sequence) {
        if (!_hash_joiners[driver_sequence]) {
            _param._is_buildable = is_buildable(driver_sequence);
            _hash_joiners[driver_sequence] = std::make_shared<HashJoiner>(_param, _read_only_probers);
        }

        if (!_hash_joiners[driver_sequence]->is_buildable()) {
            _read_only_probers.emplace_back(_hash_joiners[driver_sequence]);
        }

        return _hash_joiners[driver_sequence];
    }

    HashJoinerPtr create_builder(int driver_sequence) {
        if (_param._distribution_mode == TJoinDistributionMode::BROADCAST) {
            driver_sequence = BROADCAST_BUILD_DRIVER_SEQUENCE;
        }
        if (!_hash_joiners[driver_sequence]) {
            _param._is_buildable = true;
            _hash_joiners[driver_sequence] = std::make_shared<HashJoiner>(_param, _read_only_probers);
        }

        return _hash_joiners[driver_sequence];
    }

    bool is_buildable(int driver_sequence) const {
        return _param._distribution_mode != TJoinDistributionMode::BROADCAST ||
               driver_sequence == BROADCAST_BUILD_DRIVER_SEQUENCE;
    }

    const HashJoiners& get_read_only_probers() const { return _read_only_probers; }

private:
    // Broadcast join need only create one hash table, because all the HashJoinProbeOperators
    // use the same hash table with their own different probe states.
    static constexpr int BROADCAST_BUILD_DRIVER_SEQUENCE = 0;

    starrocks::HashJoinerParam _param;
    HashJoiners _hash_joiners;
    HashJoiners _read_only_probers;
};

} // namespace starrocks::pipeline
