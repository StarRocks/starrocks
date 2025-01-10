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


package com.starrocks.analysis;

import com.starrocks.ha.FrontendNodeType;
import com.starrocks.sql.ast.ModifyFrontendAddressClause;
import org.junit.Assert;
import org.junit.Test;


public class ModifyFrontendAddressClauseTest {

    @Test
    public void testCreateClause() {    
        ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause("originalHost-test", "sandbox");
        Assert.assertEquals("sandbox", clause.getDestHost());
        Assert.assertEquals("originalHost-test", clause.getSrcHost());
    }

    @Test
    public void testNormal() {
        ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause("test:1000", FrontendNodeType.FOLLOWER);
        Assert.assertTrue(clause.getHostPort().equals("test:1000"));
    }
}