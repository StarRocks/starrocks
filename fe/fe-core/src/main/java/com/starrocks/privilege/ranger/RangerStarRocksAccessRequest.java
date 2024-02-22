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

package com.starrocks.privilege.ranger;

import com.starrocks.common.Config;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public class RangerStarRocksAccessRequest extends RangerAccessRequestImpl {
    private static final Logger LOG = LogManager.getLogger(RangerStarRocksAccessRequest.class);

    private RangerStarRocksAccessRequest() {
    }

    public static RangerStarRocksAccessRequest createAccessRequest(RangerAccessResourceImpl resource, UserIdentity user,
                                                                   String accessType) {
        Set<String> userGroups = null;
        if (Config.ranger_user_ugi) {
            UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user.getUser());
            String[] groups = ugi.getGroupNames();

            if (groups != null && groups.length > 0) {
                userGroups = new HashSet<>(Arrays.asList(groups));
            }
        }

        RangerStarRocksAccessRequest request = new RangerStarRocksAccessRequest();
        request.setUser(user.getUser());
        request.setUserGroups(userGroups);
        request.setAccessType(accessType);
        request.setResource(resource);
        request.setClientIPAddress(user.getHost());
        request.setClientType("starrocks");
        request.setClusterName("starrocks");
        request.setAccessTime(new Date());

        LOG.debug("RangerStarRocksAccessRequest | " + request.toString());

        return request;
    }
}