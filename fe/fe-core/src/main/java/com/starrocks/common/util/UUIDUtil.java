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


package com.starrocks.common.util;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedEpochRandomGenerator;
import com.starrocks.thrift.TUniqueId;

import java.security.SecureRandom;
import java.util.Random;
import java.util.UUID;

public class UUIDUtil {
    // Prefer UUIDv7 values as they are ordered, and include timestamp.
    // Using ThreadLocal to avoid lock contention in multithread environment.
    // See https://github.com/StarRocks/starrocks/issues/58962 for more details.
    private static final SecureRandom TRUE_RANDOM = new SecureRandom();
    private static final ThreadLocal<TimeBasedEpochRandomGenerator> UUID7_FACTORY =
            new ThreadLocal<>() {
                @Override protected TimeBasedEpochRandomGenerator initialValue() {
                    // SecureRandom return truly random values, but it is 4x slower than Random.
                    // Random is pseudo-random, so it has to be initialized with a random seed
                    // to avoid producing same results in a different thread
                    long seed = TRUE_RANDOM.nextLong();
                    return Generators.timeBasedEpochRandomGenerator(new Random(seed));
                }
            };

    public static UUID genUUID() {
        return UUID7_FACTORY.get().generate();
    }

    public static UUID fromTUniqueid(TUniqueId id) {
        return new UUID(id.getHi(), id.getLo());
    }

    public static TUniqueId toTUniqueId(UUID uuid) {
        return new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    public static TUniqueId genTUniqueId() {
        return toTUniqueId(genUUID());
    }

}
