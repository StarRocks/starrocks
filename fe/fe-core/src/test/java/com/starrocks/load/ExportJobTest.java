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

package com.starrocks.load;

import com.google.common.collect.Lists;
import com.starrocks.common.Pair;
import com.starrocks.thrift.TNetworkAddress;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ExportJobTest {

    @Test
    public void testExportUpdateInfo() {
        ExportJob.ExportUpdateInfo updateInfo = new ExportJob.ExportUpdateInfo();

        List<Pair<TNetworkAddress, String>> tList = Lists.newArrayList(
                Pair.create(new TNetworkAddress("host1", 1000), "path1"));
        List<Pair<ExportJob.NetworkAddress, String>> sList = Lists.newArrayList(
                Pair.create(new ExportJob.NetworkAddress("host1", 1000), "path1")
        );

        Assert.assertEquals(sList, updateInfo.serialize(tList));
        Assert.assertEquals(tList, updateInfo.deserialize(sList));
    }
}
