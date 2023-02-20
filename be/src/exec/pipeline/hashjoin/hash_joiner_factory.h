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
    HashJoinerFactory(starrocks::HashJoinerParam& param, int dop)
            : _param(param), _hash_joiners(dop), _read_only_probers(dop) {}

    Status prepare(RuntimeState* state);
    void close(RuntimeState* state);

    HashJoinerPtr create_prober(int degree_of_parallelism, int driver_sequence) {
        const auto [num_build_drivers, num_probe_drivers_per_build] = _calc_build_and_probe_size(degree_of_parallelism);
        const auto [build_driver_idx, probe_driver_idx] =
                _calc_build_and_probe_idx(degree_of_parallelism, driver_sequence);
        DCHECK(_hash_joiners.size() > 0);
        if (num_probe_drivers_per_build > _hash_joiners[build_driver_idx].size()) {
            _hash_joiners[build_driver_idx].resize(num_probe_drivers_per_build);
        }

        auto& joiner = _hash_joiners[build_driver_idx][probe_driver_idx];
        if (!joiner) {
            _param._is_buildable = (probe_driver_idx == BUILD_JOINER_INDEX);
            joiner = std::make_shared<HashJoiner>(_param, _read_only_probers[build_driver_idx]);
        }

        if (!joiner->is_buildable()) {
            _read_only_probers[build_driver_idx].emplace_back(joiner);
        }
        return joiner;
    }

    HashJoinerPtr create_builder(int degree_of_parallelism, int driver_sequence) {
        DCHECK(_hash_joiners.size() > 0);
        const auto [_, num_probe_drivers_per_build] = _calc_build_and_probe_size(degree_of_parallelism);
        const auto [build_driver_idx, __] = _calc_build_and_probe_idx(degree_of_parallelism, driver_sequence);
        if (_hash_joiners[build_driver_idx].empty()) {
            _hash_joiners[build_driver_idx].resize(num_probe_drivers_per_build);
        }
        auto& joiner = _hash_joiners[build_driver_idx][BUILD_JOINER_INDEX];
        if (!joiner) {
            _param._is_buildable = true;
            joiner = std::make_shared<HashJoiner>(_param, _read_only_probers[build_driver_idx]);
        }
        return joiner;
    }

    const HashJoiners& get_read_only_probers(int driver_sequence) const { return _read_only_probers[driver_sequence]; }

private:
    // Broadcast join need only create one hash table, because all the HashJoinProbeOperators
    // use the same hash table with their own different probe states.
    static constexpr int BROADCAST_BUILD_DRIVER_SEQUENCE = 0;
    static constexpr int BUILD_JOINER_INDEX = 0;

    std::pair<int, int> _calc_build_and_probe_idx(int degree_of_parallelism, int driver_sequence) {
        const auto [num_build_drivers, num_probe_drivers_per_build] = _calc_build_and_probe_size(degree_of_parallelism);
        const auto build_driver_idx = driver_sequence / num_probe_drivers_per_build;
        const auto probe_driver_idx = driver_sequence % num_probe_drivers_per_build;
        return std::make_pair(build_driver_idx, probe_driver_idx);
    }

    std::pair<int, int> _calc_build_and_probe_size(int degree_of_parallelism) {
        if (_param._distribution_mode == TJoinDistributionMode::BROADCAST) {
            return std::make_pair(BROADCAST_BUILD_DRIVER_SEQUENCE, degree_of_parallelism);
        } else {
            int num_build_drivers = static_cast<int>(_hash_joiners.size());
            return std::make_pair(num_build_drivers, degree_of_parallelism / num_build_drivers);
        }
    }

    starrocks::HashJoinerParam _param;
    std::vector<HashJoiners> _hash_joiners;
    std::vector<HashJoiners> _read_only_probers;
};

} // namespace starrocks::pipeline
