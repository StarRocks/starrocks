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

import java.util.UUID;

public class UUIDUtil {
    // Prefer UUIDv7 to UUIDv4, as UUIDv7 values are sortable, but UUIDv4 are not
    private static final TimeBasedEpochRandomGenerator UUID_GENERATOR = Generators.timeBasedEpochRandomGenerator();

    public static UUID genUUID() {
        return UUID_GENERATOR.generate();
    }

    public static UUID fromTUniqueid(TUniqueId id) {
        return new UUID(id.getHi(), id.getLo());
    }

    public static TUniqueId toTUniqueId(UUID uuid) {
        return new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    public static TUniqueId genTUniqueId() {
        return toTUniqueId(UUID_GENERATOR.generate());
    }

}
