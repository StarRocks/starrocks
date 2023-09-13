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

package com.starrocks.http;

import com.starrocks.http.rest.RestBaseResult;
import org.junit.Assert;
import org.junit.Test;

public class RestBaseResultTest {
    @Test
    public void testToJson() {
        RestBaseResult result = new RestBaseResult();
        Assert.assertEquals(result.toJson(),
                "{\"status\":\"OK\",\"code\":\"0\",\"msg\":\"Success\",\"message\":\"OK\"}");
        Assert.assertEquals(result.toJsonString(), "{\"code\":\"0\",\"message\":\"OK\"}");

        RestBaseResult failedResult = new RestBaseResult("NPE");
        Assert.assertEquals(failedResult.toJson(),
                "{\"status\":\"FAILED\",\"code\":\"1\",\"msg\":\"NPE\",\"message\":\"NPE\"}");
        Assert.assertEquals(failedResult.toJsonString(), "{\"code\":\"1\",\"message\":\"NPE\"}");
    }
}
