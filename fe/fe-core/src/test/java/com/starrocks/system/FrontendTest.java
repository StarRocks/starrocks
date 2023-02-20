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


package com.starrocks.system;

import com.starrocks.ha.FrontendNodeType;
import org.junit.Assert;
import org.junit.Test;

public class FrontendTest {
    
    @Test
    public void testFeUpdate() {
        Frontend fe = new Frontend(FrontendNodeType.FOLLOWER, "name", "testHost", 1110);
        fe.updateHostAndEditLogPort("modifiedHost", 2110);
        Assert.assertEquals("modifiedHost", fe.getHost());
        Assert.assertTrue(fe.getEditLogPort() == 2110);
    }

    @Test
    public void testHbStatusBadNeedSync() {
        FrontendHbResponse hbResponse = new FrontendHbResponse("BAD", "");
        
        Frontend fe = new Frontend(FrontendNodeType.FOLLOWER, "name", "testHost", 1110);
        boolean needSync = fe.handleHbResponse(hbResponse, true);
        Assert.assertTrue(needSync);
    }
}
