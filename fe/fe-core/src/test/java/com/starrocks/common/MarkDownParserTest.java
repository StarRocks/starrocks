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

package com.starrocks.common;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class MarkDownParserTest {

    @Test
    public void testNormal() throws StarRocksException {
        List<String> lines = Lists.newArrayList();
        lines.add("# SHOW TABLES");
        lines.add("## name");
        lines.add("SHOW TABLES");
        lines.add("## description");
        lines.add("SYNTAX:");
        lines.add("\tSHOW TABLES [FROM] database");
        lines.add("## example");
        lines.add("show tables;");
        lines.add("## keyword");
        lines.add("SHOW, TABLES");
        lines.add("## url");
        lines.add("http://www.baidu.com");
        MarkDownParser parser = new MarkDownParser(lines);
        Map<String, Map<String, String>> map = parser.parse();
        Assertions.assertNotNull(map.get("show taBLES"));
        Assertions.assertEquals("SHOW TABLES\n", map.get("SHOW TABLES").get("name"));
        Assertions.assertEquals("SYNTAX:\n\tSHOW TABLES [FROM] database\n", map.get("SHOW TABLES").get("description"));
        Assertions.assertEquals("show tables;\n", map.get("SHOW TABLES").get("example"));
        Assertions.assertEquals("SHOW, TABLES\n", map.get("SHOW TABLES").get("keyword"));
        Assertions.assertEquals("http://www.baidu.com\n", map.get("SHOW TABLES").get("url"));
        for (Map.Entry<String, Map<String, String>> doc : map.entrySet()) {
            Assertions.assertEquals("SHOW TABLES\n", doc.getValue().get("NAme"));
        }
    }

    @Test
    public void testMultiDoc() throws StarRocksException {
        List<String> lines = Lists.newArrayList();
        lines.add(" name");
        lines.add("# SHOW TABLES");
        lines.add("## name");
        lines.add("SHOW TABLES");
        lines.add("## description");
        lines.add("SYNTAX:\n\tSHOW TABLES [FROM] database");
        lines.add("## example");
        lines.add("show tables;");
        lines.add("## keyword");
        lines.add("SHOW, TABLES");
        lines.add("## url");
        lines.add("http://www.baidu.com");
        lines.add("# SHOW DATABASES");
        lines.add("# DATABASES");
        MarkDownParser parser = new MarkDownParser(lines);
        Map<String, Map<String, String>> map = parser.parse();
        Assertions.assertNotNull(map.get("SHOW TABLES"));
        Assertions.assertEquals("SHOW TABLES\n", map.get("SHOW TABLES").get("name"));
        Assertions.assertEquals("SYNTAX:\n\tSHOW TABLES [FROM] database\n", map.get("SHOW TABLES").get("description"));
        Assertions.assertEquals("show tables;\n", map.get("SHOW TABLES").get("example"));
        Assertions.assertEquals("SHOW, TABLES\n", map.get("SHOW TABLES").get("keyword"));
        Assertions.assertEquals("http://www.baidu.com\n", map.get("SHOW TABLES").get("url"));
        Assertions.assertNotNull(map.get("SHOW DATABASES"));
        Assertions.assertNotNull(map.get("DATABASES"));
        Assertions.assertNull(map.get("DATABASES abc"));
    }

    @Test
    public void testNoDoc() throws StarRocksException {
        List<String> lines = Lists.newArrayList();
        lines.add(" SHOW TABLES");
        lines.add(" name");
        lines.add("SHOW TABLES");
        lines.add(" description");
        lines.add("SYNTAX:\n\tSHOW TABLES [FROM] database");
        lines.add(" example");
        lines.add("show tables;");
        lines.add(" keyword");
        lines.add("SHOW, TABLES");
        lines.add(" url");
        lines.add("http://www.baidu.com");
        lines.add(" SHOW DATABASES");
        lines.add(" DATABASES");
        MarkDownParser parser = new MarkDownParser(lines);
        Map<String, Map<String, String>> map = parser.parse();
        Assertions.assertNull(map.get("SHOW TABLES"));
        Assertions.assertNull(map.get("SHOW DATABASES"));
        Assertions.assertNull(map.get("DATABASES"));
        Assertions.assertNull(map.get("DATABASES abc"));
    }

    @Test
    public void testNoFirst() {
        assertThrows(StarRocksException.class, () -> {
            List<String> lines = Lists.newArrayList();
            lines.add("## SHOW TABLES");
            MarkDownParser parser = new MarkDownParser(lines);
            parser.parse();
            Assertions.fail("No exception throws.");
        });
    }

    //    When encounter a headlevel at 3 or greater, we ignore it rather than throw exception
    //    @Test(expected = UserException.class)
    //    public void testErrorState() throws UserException {
    //        List<String> lines = Lists.newArrayList();
    //        lines.add("# SHOW TABLES");
    //        lines.add("## name");
    //        lines.add("### name");
    //        MarkDownParser parser = new MarkDownParser(lines);
    //        Map<String, Map<String, String>> map = parser.parse();
    //        Assert.fail("No exception throws.");
    //    }

    @Test
    public void testMultiHeadLevel() throws StarRocksException {
        List<String> lines = Lists.newArrayList();
        lines.add("# SHOW TABLES");
        lines.add("## name");
        lines.add(" SHOW TABLES");
        lines.add("## description");
        lines.add("###Syntax");
        lines.add("SYNTAX:\n\tSHOW TABLES [FROM] database");
        lines.add("####Parameter");
        lines.add(">table_name");
        lines.add("## example");
        lines.add("show tables;");
        lines.add("### Exam1");
        lines.add("exam1");
        lines.add("## keyword");
        lines.add("SHOW, TABLES");
        lines.add("## url");
        lines.add("http://www.baidu.com");
        MarkDownParser parser = new MarkDownParser(lines);
        Map<String, Map<String, String>> map = parser.parse();
        Assertions.assertNotNull(map.get("SHOW TABLES"));
        Assertions.assertEquals(" SHOW TABLES\n", map.get("SHOW TABLES").get("name"));
        Assertions.assertEquals("Syntax\nSYNTAX:\n\tSHOW TABLES [FROM] database\nParameter\n>table_name\n",
                map.get("SHOW TABLES").get("description"));
        Assertions.assertEquals("show tables;\n Exam1\nexam1\n", map.get("SHOW TABLES").get("example"));
        Assertions.assertEquals("SHOW, TABLES\n", map.get("SHOW TABLES").get("keyword"));
        Assertions.assertEquals("http://www.baidu.com\n", map.get("SHOW TABLES").get("url"));
    }

    @Test
    public void testEmptyTitle() throws StarRocksException {
        List<String> lines = Lists.newArrayList();
        lines.add("#");
        lines.add("## ");
        lines.add("SHOW TABLES");
        lines.add("## ");
        lines.add("SYNTAX:\n\tSHOW TABLES [FROM] database");
        lines.add("## example");
        lines.add("show tables;");
        lines.add("## keyword");
        lines.add("SHOW, TABLES");
        lines.add("## url");
        lines.add("http://www.baidu.com");
        MarkDownParser parser = new MarkDownParser(lines);
        Map<String, Map<String, String>> map = parser.parse();
        Assertions.assertNotNull(map.get(""));
        Assertions.assertEquals("SYNTAX:\n\tSHOW TABLES [FROM] database\n", map.get("").get(""));
        Assertions.assertEquals("show tables;\n", map.get("").get("example"));
        Assertions.assertEquals("SHOW, TABLES\n", map.get("").get("keyword"));
        Assertions.assertEquals("http://www.baidu.com\n", map.get("").get("url"));
    }

    @Test
    public void testOneName() throws StarRocksException {
        List<String> lines = Lists.newArrayList();
        lines.add("# TABLES");
        lines.add("# TABLE");
        MarkDownParser parser = new MarkDownParser(lines);
        Map<String, Map<String, String>> map = parser.parse();
        Assertions.assertNotNull(map.get("TABLE"));
        Assertions.assertNotNull(map.get("TABLES"));
    }
}
