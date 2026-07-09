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

package com.starrocks.context.allocator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ContextVersionAllocatorTest {

    @Test
    public void testPerEntityMonotonic() {
        ContextVersionAllocator allocator = new ContextVersionAllocator();
        Assertions.assertEquals(1L, allocator.next(101L));
        Assertions.assertEquals(2L, allocator.next(101L));
        Assertions.assertEquals(1L, allocator.next(202L));
        Assertions.assertEquals(3L, allocator.next(101L));
    }

    @Test
    public void testSeedOnlyIncreases() {
        ContextVersionAllocator allocator = new ContextVersionAllocator();
        allocator.seed(101L, 50L);
        Assertions.assertEquals(51L, allocator.next(101L));
        // Seeding with a smaller value must not decrease the counter.
        allocator.seed(101L, 10L);
        Assertions.assertEquals(52L, allocator.next(101L));
    }
}
