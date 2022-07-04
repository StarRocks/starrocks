// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.alter;


import java.util.ArrayList;
import java.util.List;


import com.starrocks.analysis.AlterClause;

import com.starrocks.analysis.ModifyBackendAddressClause;
import com.starrocks.analysis.ModifyFrontendAddressClause;

import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;


import org.junit.Before;
import org.junit.Test;

public class SystemHandlerTest {
    
    SystemHandler systemHandler;

    @Before
    public void setUp() {
        systemHandler = new SystemHandler();
    }

    @Test(expected = DdlException.class)
    public void testModifyBackendAddressLogic() throws UserException {
        ModifyBackendAddressClause clause = new ModifyBackendAddressClause("127.0.0.1", "sandbox-fqdn");
        List<AlterClause> clauses = new ArrayList<>();
        clauses.add(clause);
        systemHandler.process(clauses, null, null);
    }

    @Test(expected = NullPointerException.class)
    public void testModifyFrontendAddressLogic() throws UserException {
        ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause("127.0.0.1", "sandbox-fqdn");
        List<AlterClause> clauses = new ArrayList<>();
        clauses.add(clause);
        systemHandler.process(clauses, null, null);
    }
}
