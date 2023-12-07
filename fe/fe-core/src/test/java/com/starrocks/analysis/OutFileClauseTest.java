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

import com.google.common.collect.Maps;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import org.junit.Assert;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.Map;

public class OutFileClauseTest {

    @Test
    public void testAnalyzeProperties() throws UnsupportedEncodingException, AnalysisException {
        Scope scope = new Scope(RelationId.anonymous(), new RelationFields());
        Map<String, String> properties = Maps.newHashMap();

        // default column separator and line delimiter
        properties.put("broker.name", "broker0");
        OutFileClause clause = new OutFileClause("file_path", "csv", properties);
        clause.analyze();
        Assert.assertEquals("\t", clause.getColumnSeparator());
        Assert.assertEquals("\n", clause.getRowDelimiter());

        // column separator: | and line delimiter: ,
        properties.clear();
        properties.put("broker.name", "broker0");
        properties.put("column_separator", "|");
        properties.put("line_delimiter", ",");
        clause = new OutFileClause("file_path", "csv", properties);
        clause.analyze();
        Assert.assertEquals("|", clause.getColumnSeparator());
        Assert.assertEquals(",", clause.getRowDelimiter());

        // invisible character column separator and line delimiter
        properties.clear();
        properties.put("broker.name", "broker0");
        properties.put("column_separator", "\\x01");
        properties.put("line_delimiter", "\\x02");
        clause = new OutFileClause("file_path", "csv", properties);
        clause.analyze();
        Assert.assertEquals(new String(new byte[] {1}, "UTF-8"), clause.getColumnSeparator());
        Assert.assertEquals(new String(new byte[] {2}, "UTF-8"), clause.getRowDelimiter());
    }
}
