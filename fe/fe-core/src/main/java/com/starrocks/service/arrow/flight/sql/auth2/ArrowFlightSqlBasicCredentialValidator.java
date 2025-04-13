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

package com.starrocks.service.arrow.flight.sql.auth2;

import com.starrocks.service.arrow.flight.sql.session.ArrowFlightSqlSessionManager;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;

public class ArrowFlightSqlBasicCredentialValidator implements BasicCallHeaderAuthenticator.CredentialValidator {

    private final ArrowFlightSqlSessionManager sessionManager;

    public ArrowFlightSqlBasicCredentialValidator(ArrowFlightSqlSessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    @Override
    public CallHeaderAuthenticator.AuthResult validate(String username, String password) throws Exception {
        // TODO: validate remoteHost.
        UserIdentity user = null;
        //        GlobalStateMgr.getCurrentState().getAuthenticationMgr().checkPlainPassword(username, "0.0.0.0", password);
        if (user == null) {
            throw CallStatus.UNAUTHENTICATED.withDescription("Access denied for " + username).toRuntimeException();
        }

        String bearerToken = sessionManager.initializeSession(user);
        return () -> bearerToken;
    }
}