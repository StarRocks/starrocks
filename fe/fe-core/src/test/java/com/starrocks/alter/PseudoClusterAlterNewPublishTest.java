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

package com.starrocks.alter;

import com.starrocks.common.conf.Config;
import com.starrocks.pseudocluster.PseudoCluster;
import org.junit.BeforeClass;

public class PseudoClusterAlterNewPublishTest extends PseudoClusterAlterTest {
    @BeforeClass
    public static void setUp() throws Exception {
        Config.alter_scheduler_interval_millisecond = 10000;
        System.out.println("enable new publish for PseudoClusterAlterNewPublishTest");
        Config.enable_new_publish_mechanism = true;
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
        PseudoCluster.getInstance().runSql(null, "create database test");
    }

}
