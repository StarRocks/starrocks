// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.starrocks.analysis.AlterSystemStmt;
import com.starrocks.analysis.BackendClause;
import com.starrocks.analysis.ComputeNodeClause;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.FrontendClause;
import com.starrocks.analysis.ModifyBackendAddressClause;
import com.starrocks.analysis.ModifyFrontendAddressClause;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
<<<<<<< HEAD
=======
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.ast.AddBackendClause;
import com.starrocks.sql.ast.AddComputeNodeClause;
import com.starrocks.sql.ast.AddFollowerClause;
import com.starrocks.sql.ast.AddObserverClause;
import com.starrocks.sql.ast.AlterSystemStmt;
>>>>>>> 82f880c6e ([BugFix] Fix drop FE failed when the target FE is using FQDN bug the leader node is not (#25575))
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.system.SystemInfoService;
import org.apache.commons.validator.routines.InetAddressValidator;

import java.net.InetAddress;
import java.net.UnknownHostException;


public class AlterSystemStmtAnalyzer {

    public static void analyze(DdlStmt ddlStmt, ConnectContext session) {
<<<<<<< HEAD
        new AlterSystemStmtAnalyzer.AlterSystemStmtAnalyzerVisitor().analyze((AlterSystemStmt) ddlStmt, session);
=======
        if (ddlStmt instanceof AlterSystemStmt) {
            new AlterSystemStmtAnalyzer.AlterSystemStmtAnalyzerVisitor().analyze((AlterSystemStmt) ddlStmt, session);
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
>>>>>>> 82f880c6e ([BugFix] Fix drop FE failed when the target FE is using FQDN bug the leader node is not (#25575))
    }

    static class AlterSystemStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(AlterSystemStmt statement, ConnectContext session) {
            visit(statement.getAlterClause(), session);
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
                throw new SemanticException("compute node host or port is wrong!");
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
                throw new SemanticException("backend host or port is wrong!");
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
                throw new SemanticException("frontend host or port is wrong!");
            }
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
<<<<<<< HEAD
=======

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
>>>>>>> 82f880c6e ([BugFix] Fix drop FE failed when the target FE is using FQDN bug the leader node is not (#25575))
    }
}
