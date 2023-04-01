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

package com.starrocks.pseudocluster;

import java.sql.SQLException;

public class PseudoClusterUtils {
    public static Tablet triggerIncrementalCloneOnce(PseudoCluster cluster, long destBeId)
            throws SQLException, InterruptedException {
        PseudoBackend be = cluster.getBackend(destBeId);
        long tabletId = cluster.listTablets("test", "test").get(0);
        Tablet tablet = be.getTablet(tabletId);
        be.setWriteFailureRate(1.0f);
        try {
            // 2 replicas commit version 2
            cluster.runSql("test", "insert into test values (1,\"1\", 1), (2,\"2\",2), (3,\"3\",3);");
        } finally {
            be.setWriteFailureRate(0.0f);
        }
        // 3 replicas commit version 3
        cluster.runSql("test", "insert into test values (1,\"1\", 1), (2,\"2\",2), (3,\"3\",3);");
        while (true) {
            if (tablet.getCloneExecuted() == 1) {
                break;
            }
            System.out.printf("wait tablet %d to finish clone\n", tabletId);
            Thread.sleep(1000);
        }

        return tablet;
    }
}
