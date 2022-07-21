// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/SetExecutor.java

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

package com.starrocks.qe;

import com.starrocks.analysis.SetNamesVar;
import com.starrocks.analysis.SetPassVar;
import com.starrocks.analysis.SetStmt;
import com.starrocks.analysis.SetTransaction;
import com.starrocks.analysis.SetVar;
import com.starrocks.common.DdlException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// Set executor
public class SetExecutor {
    private static final Logger LOG = LogManager.getLogger(SetExecutor.class);

    private ConnectContext ctx;
    private SetStmt stmt;

    public SetExecutor(ConnectContext ctx, SetStmt stmt) {
        this.ctx = ctx;
        this.stmt = stmt;
    }

    private void setVariablesOfAllType(SetVar var) throws DdlException {
        if (var instanceof SetPassVar) {
            // Set password
            SetPassVar setPassVar = (SetPassVar) var;
            ctx.getGlobalStateMgr().getAuth().setPassword(setPassVar);
        } else if (var instanceof SetNamesVar) {
            // do nothing
            return;
        } else if (var instanceof SetTransaction) {
            // do nothing
            return;
        } else {
            ctx.modifySessionVariable(var, false);
        }
    }

    private boolean isSessionVar(SetVar var) {
        return !(var instanceof SetPassVar
                || var instanceof SetNamesVar
                || var instanceof SetTransaction);
    }

    /**
     * SetExecutor will set the session variables and password
     *
     * @throws DdlException
     */
    public void execute() throws DdlException {
        for (SetVar var : stmt.getSetVars()) {
            setVariablesOfAllType(var);
        }
    }

    public void setSessionVars() throws DdlException {
        for (SetVar var : stmt.getSetVars()) {
            if (isSessionVar(var)) {
                ctx.modifySessionVariable(var, true);
            }
        }
    }
}
