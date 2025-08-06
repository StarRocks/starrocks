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

package com.starrocks.transaction;

import com.starrocks.listener.LoadJobHistoryLogListener;
import com.starrocks.load.loadv2.BrokerLoadJob;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.LoadMgr;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class LoadJobHistoryLogListenerTest {

    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Mocked
    private LoadMgr loadMgr;

    @Test
    public void testHasUnfinishedTablet(@Mocked TransactionState transactionState) {
        LoadJobHistoryLogListener listener = new LoadJobHistoryLogListener();

        LoadJob loadJob1 = new BrokerLoadJob();
        loadJob1.setId(123);
        LoadJob loadJob2 = new BrokerLoadJob();
        loadJob2.setId(456);
        List<LoadJob> loadJobs = Arrays.asList(loadJob1, loadJob2);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getLoadMgr();
                minTimes = 0;
                result = loadMgr;

                loadMgr.getLoadJobs(anyString);
                minTimes = 0;
                result = loadJobs;

                transactionState.getLabel();
                minTimes = 0;
                result = "label1";
            }
        };

        listener.onLoadJobTransactionFinish(transactionState);
    }

}
