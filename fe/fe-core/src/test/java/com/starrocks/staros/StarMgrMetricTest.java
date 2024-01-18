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

package com.starrocks.staros;

import com.starrocks.cloudnative.staros.StarMgrServer;
import com.starrocks.journal.bdbje.BDBJEJournal;
import com.starrocks.metric.PrometheusMetricVisitor;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class StarMgrMetricTest {
    @Mocked
    private BDBJEJournal journal;

    @Test
    public void testStarMgrServer() throws Exception {
        StarMgrServer server = new StarMgrServer(journal);

        PrometheusMetricVisitor visitor = new PrometheusMetricVisitor("");
        server.visitMetrics(visitor);
        String s = visitor.build();
        Assert.assertFalse(s.isEmpty());
    }
}
