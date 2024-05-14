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
import com.starrocks.http.rest.v2.RestBaseResultV2;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RestBaseResultTest {

    @Test
    public void testToJson() {
        {
            RestBaseResult resultV1 = new RestBaseResult();
            assertEquals(
                    "{\"status\":\"OK\",\"code\":\"0\",\"msg\":\"Success\",\"message\":\"OK\"}",
                    resultV1.toJson()
            );

            RestBaseResultV2<Object> resultV2 = new RestBaseResultV2<>();
            assertEquals(
                    "{\"code\":\"0\",\"message\":\"OK\"}",
                    resultV2.toJson()
            );
        }

        {
            RestBaseResult resultV1 = new RestBaseResult("NPE");
            assertEquals(
                    "{\"status\":\"FAILED\",\"code\":\"1\",\"msg\":\"NPE\",\"message\":\"NPE\"}",
                    resultV1.toJson()
            );

            RestBaseResultV2<Object> resultV2 = new RestBaseResultV2<>("NPE");
            assertEquals(
                    "{\"code\":\"1\",\"message\":\"NPE\"}",
                    resultV2.toJson()
            );
        }
    }

}
