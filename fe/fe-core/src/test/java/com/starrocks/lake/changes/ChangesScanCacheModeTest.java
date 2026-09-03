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

package com.starrocks.lake.changes;

import com.starrocks.thrift.TChangesScanCacheMode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ChangesScanCacheModeTest {

    @Test
    public void testFromName() {
        assertEquals(ChangesScanCacheMode.AUTO, ChangesScanCacheMode.fromName("auto"));
        assertEquals(ChangesScanCacheMode.NEVER, ChangesScanCacheMode.fromName("NEVER"));
        assertEquals(ChangesScanCacheMode.ALWAYS, ChangesScanCacheMode.fromName("Always"));
        assertThrows(IllegalArgumentException.class, () -> ChangesScanCacheMode.fromName("sometimes"));
        assertThrows(IllegalArgumentException.class, () -> ChangesScanCacheMode.fromName(null));
    }

    /**
     * Pins what AUTO currently means. A later change that derives AUTO from the workload has to
     * update this expectation, which is the point: the backend never sees AUTO, so this mapping is
     * the only place its meaning is written down.
     */
    @Test
    public void testResolve() {
        assertEquals(TChangesScanCacheMode.ALWAYS, ChangesScanCacheMode.AUTO.resolve());
        assertEquals(TChangesScanCacheMode.ALWAYS, ChangesScanCacheMode.ALWAYS.resolve());
        assertEquals(TChangesScanCacheMode.NEVER, ChangesScanCacheMode.NEVER.resolve());
    }
}
