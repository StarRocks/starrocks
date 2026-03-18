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

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ProfileKeyDictionaryTest {

    @Test
    public void rootAndTotalTimeHaveExpectedIds() {
        assertEquals(0, ProfileKeyDictionary.staticId(RuntimeProfile.ROOT_COUNTER));
        assertEquals(1, ProfileKeyDictionary.staticId(RuntimeProfile.TOTAL_TIME_COUNTER));
    }

    @Test
    public void unknownNameReturnsAbsent() {
        assertEquals(ProfileKeyDictionary.ABSENT,
                ProfileKeyDictionary.staticId("NoSuchCounter_xyz"));
    }

    @Test
    public void nameOfRoundTrips() {
        for (int i = 0; i < ProfileKeyDictionary.STATIC_SIZE; i++) {
            String name = ProfileKeyDictionary.STATIC_NAMES[i];
            assertEquals(i, ProfileKeyDictionary.staticId(name),
                    "staticId mismatch at index " + i);
            assertEquals(name, ProfileKeyDictionary.nameOf(i),
                    "nameOf mismatch at index " + i);
        }
    }

    @Test
    public void allStaticIdsAreUnique() {
        Set<Integer> ids = new HashSet<>();
        for (String name : ProfileKeyDictionary.STATIC_NAMES) {
            int id = ProfileKeyDictionary.staticId(name);
            assertNotEquals(ProfileKeyDictionary.ABSENT, id);
            ids.add(id);
        }
        assertEquals(ProfileKeyDictionary.STATIC_SIZE, ids.size(),
                "Every static name must have a distinct ID");
    }

    @Test
    public void nameOfOutOfRangeReturnsNull() {
        assertNull(ProfileKeyDictionary.nameOf(-1));
        assertNull(ProfileKeyDictionary.nameOf(ProfileKeyDictionary.STATIC_SIZE));
        assertNull(ProfileKeyDictionary.nameOf(Integer.MAX_VALUE));
    }
}
