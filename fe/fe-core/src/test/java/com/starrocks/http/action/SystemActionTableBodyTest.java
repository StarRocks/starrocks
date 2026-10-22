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

public class SystemActionTableBodyTest {

    private static final String PAYLOAD = "<img src=x onerror=\"alert(1)\">";
    private static final String PAYLOAD_HTML = "&lt;img src=x onerror=&#34;alert(1)&#34;&gt;";

    private static String render(boolean isDir, String path, List<String> row) {
        StringBuilder buffer = new StringBuilder();
        List<List<String>> rows = Lists.newArrayList();
        rows.add(row);
        new SystemAction(null).appendSystemTableBody(buffer, rows, isDir, path);
        return buffer.toString();
    }

    @Test
    public void testPlainCellIsHtmlEncoded() {
        // e.g. a table named with HTML in SHOW PROC '/dbs/<dbId>'
        String html = render(true, "/dbs/10001", Lists.newArrayList("10002", PAYLOAD));
        Assertions.assertFalse(html.contains(PAYLOAD), html);
        Assertions.assertTrue(html.contains("<td>" + PAYLOAD_HTML + "</td>"), html);
    }

    @Test
    public void testNewlineKeptAsLineBreakAfterEncoding() {
        String html = render(false, "/", Lists.newArrayList("line1\n<b>line2</b>"));
        Assertions.assertTrue(html.contains("<td>line1<br/>&lt;b&gt;line2&lt;/b&gt;</td>"), html);
    }

    @Test
    public void testDirLinkIsEncoded() {
        String html = render(true, "/dbs", Lists.newArrayList("10001"));
        Assertions.assertTrue(html.contains("<a href=\"?path=/dbs/10001\">10001</a>"), html);

        html = render(true, "/dbs", Lists.newArrayList("a\"" + PAYLOAD + "%"));
        Assertions.assertFalse(html.contains(PAYLOAD), html);
        Assertions.assertTrue(
                html.contains("<a href=\"?path=/dbs/a%22%3Cimg%20src%3Dx%20onerror%3D%22alert%281%29%22%3E%25\">"), html);
        Assertions.assertTrue(html.contains(">a&#34;" + PAYLOAD_HTML + "%</a>"), html);
    }

    @Test
    public void testUrlCellIsAttributeEncoded() {
        String html = render(false, "/", Lists.newArrayList("http://example.com/a?b=1&c=2"));
        Assertions.assertTrue(html.contains("<a href=\"http://example.com/a?b=1&amp;c=2\">URL</a>"), html);
    }
}
