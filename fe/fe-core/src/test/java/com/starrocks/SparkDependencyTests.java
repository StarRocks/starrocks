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

package com.starrocks;

import com.google.common.collect.Maps;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.util.SizeEstimator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class SparkDependencyTests {

    @Test
    public void testSizeEstimator() {
        try {
            Map<String, String> m = Maps.newHashMap();
            SizeEstimator.estimate(m);
        } catch (Throwable t) {
            t.printStackTrace();
            Assert.fail("Please to check spark dependency about SizeEstimator.");
        }
    }

    @Test
    public void testSparkLauncher() {
        try {
            SparkLauncher launcher = new SparkLauncher();
            launcher.setConf("spark.log.level", "INFO");
        } catch (Throwable t) {
            t.printStackTrace();
            Assert.fail("Please to check spark dependency about SparkLauncher.");
        }
    }
}
