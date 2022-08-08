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
import com.starrocks.analysis.ModifyBrokerClause;
import com.starrocks.analysis.ModifyFrontendAddressClause;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.system.SystemInfoService;
import org.apache.commons.validator.routines.InetAddressValidator;

import java.net.InetAddress;
import java.net.UnknownHostException;


public class AlterSystemStmtAnalyzer {

    public static void analyze(DdlStmt ddlStmt, ConnectContext session) {
        new AlterSystemStmtAnalyzer.AlterSystemStmtAnalyzerVisitor().analyze((AlterSystemStmt) ddlStmt, session);
    }

    static class AlterSystemStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(AlterSystemStmt statement, ConnectContext session) {
            visit(statement.getAlterClause(), session);
        }

        @Override
        public Void visitComputeNodeClause(ComputeNodeClause computeNodeClause, ConnectContext context) {
            try {
                for (String hostPort : computeNodeClause.getHostPorts()) {
                    Pair<String, Integer> pair = SystemInfoService.validateHostAndPort(hostPort);
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
                    Pair<String, Integer> pair = SystemInfoService.validateHostAndPort(hostPort);
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
                Pair<String, Integer> pair = SystemInfoService.validateHostAndPort(frontendClause.getHostPort());
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
                if (!InetAddressValidator.getInstance().isValidInet4Address(srcHost)) {
                    InetAddress.getByName(srcHost);
                }
                // if destHost is a domain name need to determine whether it is a legitimate domain name
                if (InetAddressValidator.getInstance().isValidInet4Address(destHost)) {
                    throw new SemanticException("the host you want to set could't be an ip");
                }
                InetAddress.getByName(destHost);
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
                        Pair<String, Integer> pair = SystemInfoService.validateHostAndPort(hostPort);
                        clause.getHostPortPairs().add(pair);
                    }
                    Preconditions.checkState(!clause.getHostPortPairs().isEmpty());
                }
            } catch (AnalysisException e) {
                throw new SemanticException("broker host or port is wrong!");
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
}
