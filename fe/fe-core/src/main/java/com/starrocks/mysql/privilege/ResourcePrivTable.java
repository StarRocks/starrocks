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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/mysql/privilege/ResourcePrivTable.java

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

package com.starrocks.mysql.privilege;

import com.starrocks.common.io.Text;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/*
 * ResourcePrivTable saves all resources privs
 */
public class ResourcePrivTable extends PrivTable {
    private static final Logger LOG = LogManager.getLogger(ResourcePrivTable.class);

    /*
     * Return first priv which match the user@host on resourceName The returned priv will be
     * saved in 'savedPrivs'.
     */
    public void getPrivs(UserIdentity currentUser, String resourceName, PrivBitSet savedPrivs) {
        List<PrivEntry> userPrivEntryList = map.get(currentUser);
        if (userPrivEntryList == null) {
            return;
        }

        ResourcePrivEntry matchedEntry = null;
        for (PrivEntry entry : userPrivEntryList) {
            ResourcePrivEntry resourcePrivEntry = (ResourcePrivEntry) entry;

            // check resource
            if (!resourcePrivEntry.getResourcePattern().match(resourceName)) {
                continue;
            }

            matchedEntry = resourcePrivEntry;
            break;
        }
        if (matchedEntry == null) {
            return;
        }

        savedPrivs.or(matchedEntry.getPrivSet());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (!isClassNameWrote) {
            String className = ResourcePrivTable.class.getCanonicalName();
            Text.writeString(out, className);
            isClassNameWrote = true;
        }

        super.write(out);
    }
}
