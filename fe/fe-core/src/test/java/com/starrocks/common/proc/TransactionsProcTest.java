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

package com.starrocks.common.proc;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.transaction.GlobalTransactionMgr;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TransactionsProcTest {
    @Test
    public void testFetchResult() throws AnalysisException {
        int before = Config.max_show_proc_transactions_entry;
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public List<List<String>> getDbTransInfo(long dbId, boolean running, int limit) throws AnalysisException {
                List<List<String>> l = new ArrayList<>();
                for (int i = 0; i < limit; ++i) {
                    l.add(new ArrayList<String>());
                }
                return l;
            }
        };
        Config.max_show_proc_transactions_entry = 11;
        ProcResult result = new TransProcDir(123L, "running").fetchResult();
        Assert.assertEquals(11, result.getRows().size());
        Config.max_show_proc_transactions_entry = before;
    }
}
