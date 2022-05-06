// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.starrocks.analysis.AlterSystemStmt;
import com.starrocks.analysis.BackendClause;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.FrontendClause;
import com.starrocks.analysis.ModifyBackendAddressClause;
import com.starrocks.analysis.ModifyFrontendAddressClause;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.NetUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.system.SystemInfoService;

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
        public Void visitBackendClause(BackendClause backendClause, ConnectContext context) {
            try {
                for (String hostPort : backendClause.getHostPorts()) {
                    Pair<String, Integer> pair = SystemInfoService.validateHostAndPort(hostPort, true);
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
                Pair<String, Integer> pair = SystemInfoService.validateHostAndPort(frontendClause.getHostPort(), true);
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
            try {
                Pair<String, Integer> pair = SystemInfoService.validateHostAndPort(clause.getWantToModifyHostPort(), false);
                Preconditions.checkState(!Strings.isNullOrEmpty(pair.first));
                clause.setWantToModifyHostPortPair(pair);
                String fqdn = clause.getFqdn();
                // fqdn is now allowed to be a legitimate ip, 
                // if it is a domain name need to determine whether it is a legitimate domain name
                if (!NetUtils.validIPAddress(fqdn)) {
                    InetAddress.getByName(fqdn);
                }
            } catch (AnalysisException e) {
                throw new SemanticException("target front host or port was wrong!");
            } catch (UnknownHostException e) {
                throw new SemanticException("unknown host " + e.getMessage());
            }
            return null;
        }

        @Override
        public Void visitModifyBackendHostClause(ModifyBackendAddressClause clause, ConnectContext context) {
            try {
                Pair<String, Integer> pair = SystemInfoService.validateHostAndPort(clause.getWantToModifyHostPort(), false);
                Preconditions.checkState(!Strings.isNullOrEmpty(pair.first));
                clause.setWantToModifyHostPortPair(pair);
                String fqdn = clause.getFqdn();
                // fqdn is now allowed to be a legitimate ip, 
                // if it is a domain name need to determine whether it is a legitimate domain name
                if (!NetUtils.validIPAddress(fqdn)) {
                    InetAddress.getByName(fqdn);
                }
            } catch (AnalysisException e) {
                throw new SemanticException("target backend host or port was wrong!");
            } catch (UnknownHostException e) {
                throw new SemanticException("unknown host" + e.getMessage());
            }
            return null;
        }
    }
}
