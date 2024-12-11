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
<<<<<<< HEAD
=======
import com.starrocks.common.util.PropertyAnalyzer;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
import com.starrocks.sql.ast.ModifyBackendAddressClause;
=======
import com.starrocks.sql.ast.ModifyBackendClause;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.starrocks.sql.ast.ModifyBrokerClause;
import com.starrocks.sql.ast.ModifyFrontendAddressClause;
import com.starrocks.system.SystemInfoService;
import org.apache.commons.validator.routines.InetAddressValidator;

import java.net.InetAddress;
import java.net.UnknownHostException;
<<<<<<< HEAD

import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;

public class AlterSystemStmtAnalyzer extends AstVisitor<Void, ConnectContext> {
=======
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class AlterSystemStmtAnalyzer implements AstVisitor<Void, ConnectContext> {
    public static final String PROP_KEY_LOCATION = PropertyAnalyzer.PROPERTIES_LABELS_LOCATION;
    private static final Set<String> PROPS_SUPPORTED = new HashSet<>();

    static {
        PROPS_SUPPORTED.add(PROP_KEY_LOCATION);
    }
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

    public void analyze(DdlStmt ddlStmt, ConnectContext session) {
        if (ddlStmt instanceof AlterSystemStmt) {
            visit(((AlterSystemStmt) ddlStmt).getAlterClause(), session);
        } else if (ddlStmt instanceof CancelAlterSystemStmt) {
            CancelAlterSystemStmt stmt = (CancelAlterSystemStmt) ddlStmt;
<<<<<<< HEAD
            try {
                for (String hostPort : stmt.getHostPorts()) {
                    Pair<String, Integer> pair = SystemInfoService.validateHostAndPort(hostPort, false);
                    stmt.getHostPortPairs().add(pair);
                }
            } catch (AnalysisException e) {
                throw new SemanticException(PARSER_ERROR_MSG.invalidHostOrPort("FRONTEND", e.getMessage()));
=======
            for (String hostPort : stmt.getHostPorts()) {
                Pair<String, Integer> pair = SystemInfoService.validateHostAndPort(hostPort, false);
                stmt.getHostPortPairs().add(pair);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            }
        }
    }

    @Override
    public Void visitComputeNodeClause(ComputeNodeClause computeNodeClause, ConnectContext context) {
<<<<<<< HEAD
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
=======
        for (String hostPort : computeNodeClause.getHostPorts()) {
            Pair<String, Integer> pair = SystemInfoService.validateHostAndPort(hostPort,
                    computeNodeClause instanceof AddComputeNodeClause && !FrontendOptions.isUseFqdn());
            computeNodeClause.getHostPortPairs().add(pair);
        }
        Preconditions.checkState(!computeNodeClause.getHostPortPairs().isEmpty());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        return null;
    }

    @Override
    public Void visitBackendClause(BackendClause backendClause, ConnectContext context) {
<<<<<<< HEAD
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
=======
        List<Pair<String, Integer>> hostPortPairs = new ArrayList<>();
        for (String hostPort : backendClause.getHostPortsUnResolved()) {
            Pair<String, Integer> pair = SystemInfoService.validateHostAndPort(hostPort,
                    backendClause instanceof AddBackendClause && !FrontendOptions.isUseFqdn());
            hostPortPairs.add(pair);
        }
        backendClause.setHostPortPairs(hostPortPairs);
        Preconditions.checkState(!hostPortPairs.isEmpty());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        return null;
    }

    @Override
    public Void visitFrontendClause(FrontendClause frontendClause, ConnectContext context) {
<<<<<<< HEAD
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
=======
        Pair<String, Integer> pair = SystemInfoService
                .validateHostAndPort(frontendClause.getHostPort(),
                        (frontendClause instanceof AddFollowerClause
                                || frontendClause instanceof AddObserverClause)
                                && !FrontendOptions.isUseFqdn());
        frontendClause.setHost(pair.first);
        frontendClause.setPort(pair.second);
        Preconditions.checkState(!Strings.isNullOrEmpty(frontendClause.getHost()));
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
    public Void visitModifyBackendHostClause(ModifyBackendAddressClause clause, ConnectContext context) {
        checkModifyHostClause(clause.getSrcHost(), clause.getDestHost());
        return null;
    }

=======
    public Void visitModifyBackendClause(ModifyBackendClause clause, ConnectContext context) {
        if (clause.getBackendHostPort() == null) {
            checkModifyHostClause(clause.getSrcHost(), clause.getDestHost());
        } else {
            SystemInfoService.validateHostAndPort(clause.getBackendHostPort(), false);
            analyzeBackendProperties(clause.getProperties());
        }
        return null;
    }

    private void analyzeBackendProperties(Map<String, String> properties) {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String propKey = entry.getKey();
            if (!PROPS_SUPPORTED.contains(propKey)) {
                throw new SemanticException("unsupported property: " + propKey);
            }
            if (propKey.equals(PROP_KEY_LOCATION)) {
                String propVal = entry.getValue();
                if (propVal.isEmpty()) {
                    continue;
                }
                // Support single level location label for now
                String regex = "(\\s*[a-z_0-9]+\\s*:\\s*[a-z_0-9]+\\s*)";
                if (!Pattern.compile(regex).matcher(propVal).matches()) {
                    throw new SemanticException("invalid location format: " + propVal +
                            ", should be like: 'key:val'");
                }
            }
        }
    }

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    private void checkModifyHostClause(String srcHost, String destHost) {
        try {
            boolean srcHostIsIP = InetAddressValidator.getInstance().isValidInet4Address(srcHost);
            boolean destHostIsIP = InetAddressValidator.getInstance().isValidInet4Address(destHost);
            if (srcHostIsIP && destHostIsIP) {
                throw new SemanticException("Can't change ip to ip");
            }
<<<<<<< HEAD
            // If can't get an ip through the srcHost/destHost, will throw UnknownHostException
=======
            // Can't get an ip through the srcHost/destHost, will throw UnknownHostException
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
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
=======
        if (clause.getOp() != ModifyBrokerClause.ModifyOp.OP_DROP_ALL) {
            for (String hostPort : clause.getHostPorts()) {
                Pair<String, Integer> pair = SystemInfoService.validateHostAndPort(hostPort, false);
                clause.getHostPortPairs().add(pair);
            }
            Preconditions.checkState(!clause.getHostPortPairs().isEmpty());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
