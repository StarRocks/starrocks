// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/ResourceGroupTest.java

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

package com.starrocks.catalog;

import com.starrocks.common.DdlException;
import com.starrocks.thrift.TResourceGroup;
import com.starrocks.thrift.TResourceType;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ResourceGroupTest {
    @Test
    public void testNormal() throws IOException, DdlException {
        ResourceGroup.Builder builder = ResourceGroup.builder();
        builder.cpuShare(100);
        ResourceGroup resource = builder.build();

        Assert.assertEquals("CPU_SHARE = 100", resource.toString());

        // To thrift
        TResourceGroup tResource = resource.toThrift();
        Assert.assertEquals(100, tResource.getResourceByType().get(TResourceType.TRESOURCE_CPU_SHARE).intValue());

        // Write edit log
        resource.updateByDesc("cpu_share", 150);
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(byteStream);
        resource.write(outputStream);
        outputStream.flush();
        DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(byteStream.toByteArray()));
        ResourceGroup newResource = ResourceGroup.readIn(inputStream);

        Assert.assertEquals(150, newResource.getByDesc("cpu_share"));
    }
}