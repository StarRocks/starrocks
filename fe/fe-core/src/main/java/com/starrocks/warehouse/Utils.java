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

package com.starrocks.warehouse;

import com.google.common.base.Strings;
import com.starrocks.authentication.UserProperty;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

public class Utils {
    private static final Logger LOG = LogManager.getLogger(Utils.class);

    private Utils() {
    }

    public static Optional<String> getUserDefaultWarehouse(UserIdentity userIdentity) {
        if (userIdentity == null || userIdentity.isEphemeral()) {
            return Optional.empty();
        }

        String user = userIdentity.getUser();
        if (Strings.isNullOrEmpty(user)) {
            return Optional.empty();
        }

        try {
            UserProperty userProperty = GlobalStateMgr.getCurrentState().getAuthenticationMgr().getUserProperty(user);
            String userWarehouse = userProperty.getSessionVariables().get(SessionVariable.WAREHOUSE_NAME);
            if (!Strings.isNullOrEmpty(userWarehouse)) {
                return Optional.of(userWarehouse);
            }
        } catch (SemanticException e) {
            LOG.warn("Failed to get user property. user: {}", user, e);
        }

        return Optional.empty();
    }
}
