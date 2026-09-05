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

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.TextAnalyzer;
import com.starrocks.common.io.Writable;

public class TextAnalyzerLog implements Writable {
    @SerializedName("dbId")
    private long dbId;
    @SerializedName("analyzer")
    private TextAnalyzer analyzer;
    @SerializedName("name")
    private String name;

    public TextAnalyzerLog() {
    }

    public TextAnalyzerLog(long dbId, TextAnalyzer analyzer) {
        this.dbId = dbId;
        this.analyzer = analyzer;
        this.name = analyzer.getName();
    }

    public TextAnalyzerLog(long dbId, String name) {
        this.dbId = dbId;
        this.name = name;
    }

    public long getDbId() {
        return dbId;
    }

    public TextAnalyzer getAnalyzer() {
        return analyzer;
    }

    public String getName() {
        return name;
    }

}
