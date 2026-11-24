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

package com.starrocks.backup;

import com.starrocks.alter.RollupJobV2Test;
import com.starrocks.sql.analyzer.DDLTestBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class RestoreJobSyncMvAnalyzeTest extends DDLTestBase {
    @BeforeAll
    public static void beforeAll() {
        RollupJobV2Test.beforeAll();
    }

    @ParameterizedTest
    @ValueSource(strings = {"restore_live", "restore"})
    public void testRestoreAnalyzesSyncMv(String name) throws Exception {
        RollupJobV2Test.assertSyncMvReplay(starRocksAssert, name, "id, length(txt) v", null);
    }
}
