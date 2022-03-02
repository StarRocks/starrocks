// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once
#include <memory>
#include <vector>

#include "exec/vectorized/hash_joiner.h"
namespace starrocks {
namespace pipeline {

using HashJoiner = starrocks::vectorized::HashJoiner;
using HashJoinerPtr = std::shared_ptr<HashJoiner>;
using HashJoiners = std::vector<HashJoinerPtr>;
class HashJoinerFactory;
using HashJoinerFactoryPtr = std::shared_ptr<HashJoinerFactory>;

class HashJoinerFactory {
public:
    HashJoinerFactory(starrocks::vectorized::HashJoinerParam& param, int dop) : _param(param), _hash_joiners(dop) {}

    Status prepare(RuntimeState* state);
    void close(RuntimeState* state);

    HashJoinerPtr create_probe_hash_joiner(int driver_sequence) {
        if (!_hash_joiners[driver_sequence]) {
            _param._is_buildable = is_buildable(driver_sequence);
            _hash_joiners[driver_sequence] = std::make_shared<HashJoiner>(_param);
        }

        if (!_hash_joiners[driver_sequence]->is_buildable()) {
            _only_probe_hash_joiners.emplace_back(_hash_joiners[driver_sequence]);
        }

        return _hash_joiners[driver_sequence];
    }

    HashJoinerPtr create_build_hash_joiner(int driver_sequence) {
        if (_param._distribution_mode == TJoinDistributionMode::BROADCAST) {
            driver_sequence = BROADCAST_BUILD_DRIVER_SEQUENCE;
        }
        if (!_hash_joiners[driver_sequence]) {
            _param._is_buildable = true;
            _hash_joiners[driver_sequence] = std::make_shared<HashJoiner>(_param);
        }

        return _hash_joiners[driver_sequence];
    }

    bool is_buildable(int driver_sequence) const {
        return _param._distribution_mode != TJoinDistributionMode::BROADCAST ||
               driver_sequence == BROADCAST_BUILD_DRIVER_SEQUENCE;
    }

    const HashJoiners& get_only_probe_hash_joiners() const { return _only_probe_hash_joiners; }

private:
    // Broadcast join need only create one hash table, because all the HashJoinProbeOperators
    // use the same hash table with their own different probe states.
    static constexpr int BROADCAST_BUILD_DRIVER_SEQUENCE = 0;

    starrocks::vectorized::HashJoinerParam _param;
    HashJoiners _hash_joiners;
    HashJoiners _only_probe_hash_joiners;
};

} // namespace pipeline
} // namespace starrocks