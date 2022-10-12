// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.alter;

import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.ModifyBackendAddressClause;
import com.starrocks.sql.ast.ModifyFrontendAddressClause;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

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
