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


package com.staros.shard;

import com.google.common.collect.ImmutableList;
import com.staros.proto.PlacementPolicy;
import com.staros.shard.policy.AbstractShardPlacementPolicy;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ShardPolicyFilter {
    private static class ExcludeShardPlacementPolicy extends AbstractShardPlacementPolicy {
        public ExcludeShardPlacementPolicy() {
            super(PlacementPolicy.EXCLUDE);
        }

        @Override
        public void filter(Collection<Long> constraints, Collection<Long> candidates) {
            if (constraints == null || candidates == null) {
                return;
            }
            // EXCLUDE policy, all the candidates that are in the constraints will be filtered out
            candidates.removeIf(constraints::contains);
        }
    }

    // The list should be ordered by PlacementPolicy precedence if multiple policies exists
    protected static List<AbstractShardPlacementPolicy> policies = ImmutableList.of(new ExcludeShardPlacementPolicy());

    /**
     * Filter out candidates according to the given constraints, a map of PlacementPolicy -> WorkerIdList
     * @param constraints constraints group by PlacementPolicy
     * @param candidates candidate worker id list
     */
    public static void filter(Map<PlacementPolicy, Collection<Long>> constraints, Collection<Long> candidates) {
        policies.forEach(p -> p.filter(constraints.get(p.getPolicy()), candidates));
    }
}
