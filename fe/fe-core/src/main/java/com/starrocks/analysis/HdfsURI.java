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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/HdfsURI.java

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

package com.starrocks.analysis;

import com.google.common.base.Preconditions;
import com.starrocks.common.AnalysisException;

/*
 * Represents an HDFS URI in a SQL statement.
 */
public class HdfsURI {
    private final String location;

    // Set during analysis
    // dhc to do
    // private Path uriPath;
    private String uriPath;

    public HdfsURI(String location) {
        Preconditions.checkNotNull(location);
        this.location = location.trim();
    }

    public String getPath() {
        Preconditions.checkNotNull(uriPath);
        return uriPath;
    }

    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (location.isEmpty()) {
            throw new AnalysisException("URI path cannot be empty.");
        }
        uriPath = new String(location);
    }

    @Override
    public String toString() {
        // If uriPath is null (this HdfsURI has not been analyzed yet) just return the raw
        // location string the caller passed in.
        return uriPath == null ? location : uriPath.toString();
    }

    public String getLocation() {
        return location;
    }
}
