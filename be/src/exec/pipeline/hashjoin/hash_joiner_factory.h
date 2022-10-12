// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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

    starrocks::vectorized::HashJoinerParam _param;
    HashJoiners _hash_joiners;
    HashJoiners _read_only_probers;
};

} // namespace pipeline
} // namespace starrocks