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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/ResourceDescTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.collect.Maps;
import com.starrocks.catalog.ResourceMgr;
import com.starrocks.catalog.SparkResource;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.load.EtlJobType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ResourceDesc;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class ResourceDescTest {

    @Test
    public void testNormal(@Mocked GlobalStateMgr globalStateMgr, @Injectable ResourceMgr resourceMgr)
            throws AnalysisException, DdlException {
        String resourceName = "spark0";
        Map<String, String> properties = Maps.newHashMap();
        String key = "spark.executor.memory";
        String value = "2g";
        properties.put(key, value);
        ResourceDesc resourceDesc = new ResourceDesc(resourceName, properties);
        SparkResource resource = new SparkResource(resourceName);

        new Expectations() {
            {
                globalStateMgr.getResourceMgr();
                result = resourceMgr;
                resourceMgr.getResource(resourceName);
                result = resource;
            }
        };

        resourceDesc.analyze();
        Assert.assertEquals(resourceName, resourceDesc.getName());
        Assert.assertEquals(value, resourceDesc.getProperties().get(key));
        Assert.assertEquals(EtlJobType.SPARK, resourceDesc.getEtlJobType());
    }

    @Test(expected = AnalysisException.class)
    public void testNoResource(@Mocked GlobalStateMgr globalStateMgr, @Injectable ResourceMgr resourceMgr)
            throws AnalysisException {
        String resourceName = "spark1";
        ResourceDesc resourceDesc = new ResourceDesc(resourceName, null);

        new Expectations() {
            {
                globalStateMgr.getResourceMgr();
                result = resourceMgr;
                resourceMgr.getResource(resourceName);
                result = null;
            }
        };

        resourceDesc.analyze();
    }
}
