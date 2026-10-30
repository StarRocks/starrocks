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

package com.starrocks.http.action;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class WebBaseActionTest {

    private static final String PAYLOAD = "select 1 /*<img src=x onerror=\"alert(1)\">*/";

    @Test
    public void testAppendTableHeaderEncodesHtml() {
        WebBaseAction action = new WebBaseAction(null);
        StringBuilder buffer = new StringBuilder();
        action.appendTableHeader(buffer, Lists.newArrayList("Id", PAYLOAD));

        String html = buffer.toString();
        Assertions.assertTrue(html.contains("<th>Id</th>"), html);
        Assertions.assertFalse(html.contains("<img"), html);
        Assertions.assertTrue(html.contains("&lt;img src=x onerror=&#34;alert(1)&#34;&gt;"), html);
    }

    @Test
    public void testAppendTableBodyEncodesHtml() {
        WebBaseAction action = new WebBaseAction(null);
        StringBuilder buffer = new StringBuilder();
        List<List<String>> rows = Lists.newArrayList();
        rows.add(Lists.newArrayList("1", PAYLOAD));
        action.appendTableBody(buffer, rows);

        String html = buffer.toString();
        Assertions.assertTrue(html.contains("<td>1</td>"), html);
        Assertions.assertFalse(html.contains("<img"), html);
        Assertions.assertTrue(html.contains("&lt;img src=x onerror=&#34;alert(1)&#34;&gt;"), html);
    }
}
