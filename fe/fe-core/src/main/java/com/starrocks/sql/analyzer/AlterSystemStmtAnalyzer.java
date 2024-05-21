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

package com.starrocks.sql.analyzer;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.ast.AddBackendClause;
import com.starrocks.sql.ast.AddComputeNodeClause;
import com.starrocks.sql.ast.AddFollowerClause;
import com.starrocks.sql.ast.AddObserverClause;
import com.starrocks.sql.ast.AlterSystemStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.BackendClause;
import com.starrocks.sql.ast.CancelAlterSystemStmt;
import com.starrocks.sql.ast.CleanTabletSchedQClause;
import com.starrocks.sql.ast.ComputeNodeClause;
import com.starrocks.sql.ast.CreateImageClause;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.ast.FrontendClause;
import com.starrocks.sql.ast.ModifyBackendAddressClause;
import com.starrocks.sql.ast.ModifyBrokerClause;
import com.starrocks.sql.ast.ModifyFrontendAddressClause;
import com.starrocks.system.SystemInfoService;
import org.apache.commons.validator.routines.InetAddressValidator;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;

public class AlterSystemStmtAnalyzer extends AstVisitor<Void, ConnectContext> {

    public void analyze(DdlStmt ddlStmt, ConnectContext session) {
        if (ddlStmt instanceof AlterSystemStmt) {
            visit(((AlterSystemStmt) ddlStmt).getAlterClause(), session);
        } else if (ddlStmt instanceof CancelAlterSystemStmt) {
            CancelAlterSystemStmt stmt = (CancelAlterSystemStmt) ddlStmt;
            try {
                for (String hostPort : stmt.getHostPorts()) {
                    Pair<String, Integer> pair = SystemInfoService.validateHostAndPort(hostPort, false);
                    stmt.getHostPortPairs().add(pair);
                }
            } catch (AnalysisException e) {
                throw new SemanticException(PARSER_ERROR_MSG.invalidHostOrPort("FRONTEND", e.getMessage()));
            }
        }
    }

    @Override
    public Void visitComputeNodeClause(ComputeNodeClause computeNodeClause, ConnectContext context) {
        try {
            for (String hostPort : computeNodeClause.getHostPorts()) {
                Pair<String, Integer> pair = SystemInfoService.validateHostAndPort(hostPort,
                        computeNodeClause instanceof AddComputeNodeClause && !FrontendOptions.isUseFqdn());
                computeNodeClause.getHostPortPairs().add(pair);
            }
            Preconditions.checkState(!computeNodeClause.getHostPortPairs().isEmpty());
        } catch (AnalysisException e) {
            throw new SemanticException(PARSER_ERROR_MSG.invalidHostOrPort("COMPUTE NODE", e.getMessage()));
        }
        return null;
    }

    @Override
    public Void visitBackendClause(BackendClause backendClause, ConnectContext context) {
        try {
            for (String hostPort : backendClause.getHostPorts()) {
                Pair<String, Integer> pair = SystemInfoService.validateHostAndPort(hostPort,
                        backendClause instanceof AddBackendClause && !FrontendOptions.isUseFqdn());
                backendClause.getHostPortPairs().add(pair);
            }
            Preconditions.checkState(!backendClause.getHostPortPairs().isEmpty());
        } catch (AnalysisException e) {
            throw new SemanticException(PARSER_ERROR_MSG.invalidHostOrPort("BACKEND", e.getMessage()));
        }
        return null;
    }

    @Override
    public Void visitFrontendClause(FrontendClause frontendClause, ConnectContext context) {
        try {
            Pair<String, Integer> pair = SystemInfoService
                    .validateHostAndPort(frontendClause.getHostPort(),
                            (frontendClause instanceof AddFollowerClause
                                    || frontendClause instanceof AddObserverClause)
                                    && !FrontendOptions.isUseFqdn());
            frontendClause.setHost(pair.first);
            frontendClause.setPort(pair.second);
            Preconditions.checkState(!Strings.isNullOrEmpty(frontendClause.getHost()));
        } catch (AnalysisException e) {
            throw new SemanticException(PARSER_ERROR_MSG.invalidHostOrPort("FRONTEND", e.getMessage()));
        }
        return null;
    }

    @Override
    public Void visitCreateImageClause(CreateImageClause createImageClause, ConnectContext context) {
        return null;
    }

    @Override
    public Void visitCleanTabletSchedQClause(CleanTabletSchedQClause clause, ConnectContext context) {
        return null;
    }

    @Override
    public Void visitModifyFrontendHostClause(ModifyFrontendAddressClause clause, ConnectContext context) {
        checkModifyHostClause(clause.getSrcHost(), clause.getDestHost());
        return null;
    }

    @Override
    public Void visitModifyBackendHostClause(ModifyBackendAddressClause clause, ConnectContext context) {
        checkModifyHostClause(clause.getSrcHost(), clause.getDestHost());
        return null;
    }

    private void checkModifyHostClause(String srcHost, String destHost) {
        try {
            boolean srcHostIsIP = InetAddressValidator.getInstance().isValidInet4Address(srcHost);
            boolean destHostIsIP = InetAddressValidator.getInstance().isValidInet4Address(destHost);
            if (srcHostIsIP && destHostIsIP) {
                throw new SemanticException("Can't change ip to ip");
            }
            // If can't get an ip through the srcHost/destHost, will throw UnknownHostException
            if (!srcHostIsIP) {
                InetAddress.getByName(srcHost);
            }
            if (!destHostIsIP) {
                InetAddress.getByName(destHost);
            }
        } catch (UnknownHostException e) {
            throw new SemanticException("unknown host " + e.getMessage());
        }
    }

    @Override
    public Void visitModifyBrokerClause(ModifyBrokerClause clause, ConnectContext context) {
        validateBrokerName(clause);
        try {
            if (clause.getOp() != ModifyBrokerClause.ModifyOp.OP_DROP_ALL) {
                for (String hostPort : clause.getHostPorts()) {
                    Pair<String, Integer> pair = SystemInfoService.validateHostAndPort(hostPort, false);
                    clause.getHostPortPairs().add(pair);
                }
                Preconditions.checkState(!clause.getHostPortPairs().isEmpty());
            }
        } catch (AnalysisException e) {
            throw new SemanticException(PARSER_ERROR_MSG.invalidHostOrPort("BROKER", e.getMessage()));
        }
        return null;
    }

    private void validateBrokerName(ModifyBrokerClause clause) {
        try {
            clause.validateBrokerName();
        } catch (AnalysisException e) {
            throw new SemanticException(e.getMessage());
        }
    }
}
