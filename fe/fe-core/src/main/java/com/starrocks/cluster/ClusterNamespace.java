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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/cluster/ClusterNamespace.java

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

package com.starrocks.cluster;

import com.google.common.base.Strings;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.system.SystemInfoService;

/**
 * used to isolate the use for the database name and user name in the globalStateMgr,
 * all using the database name and user name place need to call the appropriate
 * method to makeup full name or get real name, full name is made up generally
 * in stmt's analyze.
 */

public class ClusterNamespace {

    public static final String CLUSTER_DELIMITER = ":";

    public static String getFullName(String name) {
        return linkString(SystemInfoService.DEFAULT_CLUSTER, name);
    }

    public static String getNameFromFullName(String fullName) {
        if (!checkName(fullName)) {
            return fullName;
        }
        return extract(fullName, 1);
    }

    private static boolean checkName(String str) {
        if (Strings.isNullOrEmpty(str)) {
            return false;
        }
        final String[] ele = str.split(CLUSTER_DELIMITER);
        return ele.length > 1;
    }

    private static String linkString(String cluster, String name) {
        if (Strings.isNullOrEmpty(cluster) || Strings.isNullOrEmpty(name)) {
            return null;
        }
        if (name.contains(CLUSTER_DELIMITER) || name.equalsIgnoreCase(AuthenticationMgr.ROOT_USER)) {
            return name;
        }
        return cluster + CLUSTER_DELIMITER + name;
    }

    private static String extract(String fullName, int index) {
        final String[] ele = fullName.split(CLUSTER_DELIMITER);
        return ele[index];
    }
}
