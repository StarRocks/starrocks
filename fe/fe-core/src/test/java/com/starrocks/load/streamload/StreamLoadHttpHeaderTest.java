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

package com.starrocks.load.streamload;

import org.junit.Test;
import org.wildfly.common.Assert;

import java.lang.reflect.Field;
import java.util.List;

public class StreamLoadHttpHeaderTest {

    @Test
    public void testHttpHeaderList() throws Exception {
        Field[] fields = StreamLoadHttpHeader.class.getDeclaredFields();
        List<String> headerKeyList = StreamLoadHttpHeader.HTTP_HEADER_LIST;

        for (Field field : fields) {
            if (field.getName().equals("HTTP_HEADER_LIST")) {
                continue;
            }
            if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                String fieldValue = (String) field.get(null);
                Assert.assertTrue(headerKeyList.contains(fieldValue));
            }
        }
    }
}
