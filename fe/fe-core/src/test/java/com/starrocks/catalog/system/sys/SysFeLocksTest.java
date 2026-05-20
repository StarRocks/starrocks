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

package com.starrocks.catalog.system.sys;

import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TFeLocksReq;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SysFeLocksTest {

    @Test
    public void testListLocks() throws TException {
        TFeLocksReq req = new TFeLocksReq();
        TAuthInfo auth = new TAuthInfo();
        auth.setUser("root");
        auth.setUser_ip("127.0.0.1");
        req.setAuth_info(auth);

        var res = SysFeLocks.listLocks(req, false);
        assertTrue(StringUtils.isNotEmpty(res.toString()));
    }

    @Test
    public void testListLocksAccessDeniedSurfacesMessage() {
        TFeLocksReq req = new TFeLocksReq();
        TAuthInfo auth = new TAuthInfo();
        auth.setUser("nopriv");
        auth.setUser_ip("127.0.0.1");
        req.setAuth_info(auth);

        new MockUp<Authorizer>() {
            @Mock
            public void checkSystemAction(ConnectContext context, PrivilegeType privilegeType)
                    throws AccessDeniedException {
                throw new AccessDeniedException();
            }
        };

        TException ex = assertThrows(TException.class, () -> SysFeLocks.listLocks(req, true));
        assertNotNull(ex.getMessage(), "AccessDenied message should not be null");
        assertTrue(ex.getMessage().contains("Access denied"), "Should match canonical format: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("OPERATE"), "Should mention OPERATE: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("SYSTEM"), "Should mention SYSTEM: " + ex.getMessage());
    }

}
