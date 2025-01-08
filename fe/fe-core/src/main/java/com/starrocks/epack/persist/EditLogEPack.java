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
package com.starrocks.epack.persist;

import com.starrocks.epack.authentication.AuthenticationMgrEPack;
import com.starrocks.journal.JournalEntity;
import com.starrocks.journal.JournalInconsistentException;
import com.starrocks.journal.JournalTask;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.automv.lifecycle.MVChangeLog;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class EditLogEPack extends EditLog {
    public EditLogEPack(BlockingQueue<JournalTask> journalQueue) {
        super(journalQueue);
    }

    public void logCreateSecurityIntegration(String name, Map<String, String> propertyMap) {
        SecurityIntegrationPersistInfo info = new SecurityIntegrationPersistInfo(name, propertyMap);
        logEdit(OperationTypeEPack.OP_CREATE_SECURITY_INTEGRATION, info);
    }

    public void logAlterSecurityIntegration(String name, Map<String, String> alterProps) {
        SecurityIntegrationPersistInfo info = new SecurityIntegrationPersistInfo(name, alterProps);
        logEdit(OperationTypeEPack.OP_ALTER_SECURITY_INTEGRATION, info);
    }

    public void logDropSecurityIntegration(String name) {
        SecurityIntegrationPersistInfo info = new SecurityIntegrationPersistInfo(name, null);
        logEdit(OperationTypeEPack.OP_DROP_SECURITY_INTEGRATION, info);
    }

    public void logCreateRoleMapping(String name, Map<String, String> propertyMap) {
        RoleMappingPersistInfo info = new RoleMappingPersistInfo(name, propertyMap);
        logEdit(OperationTypeEPack.OP_CREATE_ROLE_MAPPING, info);
    }

    public void logAlterRoleMapping(String name, Map<String, String> alterProps) {
        RoleMappingPersistInfo info = new RoleMappingPersistInfo(name, alterProps);
        logEdit(OperationTypeEPack.OP_ALTER_ROLE_MAPPING, info);
    }

    public void logDropRoleMapping(String name) {
        RoleMappingPersistInfo info = new RoleMappingPersistInfo(name, null);
        logEdit(OperationTypeEPack.OP_DROP_ROLE_MAPPING, info);
    }

    public void logMVChangeLog(MVChangeLog mvChangeLog) {
        logEdit(OperationTypeEPack.OP_MV_CHANGE, mvChangeLog);
    }

    public void logCreatePasswordPolicy(CreatePasswordPolicyLog createPasswordPolicyLog) {
        logEdit(OperationTypeEPack.OP_CREATE_PASSWORD_POLICY, createPasswordPolicyLog);
    }

    public void logDropPasswordPolicy(DropPasswordPolicyLog dropPasswordPolicyLog) {
        logEdit(OperationTypeEPack.OP_DROP_PASSWORD_POLICY, dropPasswordPolicyLog);
    }

    public void logSetGlobalPasswordPolicy(SetPasswordPolicyLog setPasswordPolicyLog) {
        logEdit(OperationTypeEPack.OP_SET_PASSWORD_POLICY, setPasswordPolicyLog);
    }

    public void logUnsetGlobalPasswordPolicy(UnsetPasswordPolicyLog unsetPasswordPolicyLog) {
        logEdit(OperationTypeEPack.OP_UNSET_PASSWORD_POLICY, unsetPasswordPolicyLog);
    }

    @Override
    public void loadJournal(GlobalStateMgr globalStateMgr, JournalEntity journal)
            throws JournalInconsistentException {

        short opCode = journal.opCode();
        try {
            switch (opCode) {
                case OperationTypeEPack.OP_CREATE_SECURITY_INTEGRATION: {
                    SecurityIntegrationPersistInfo info = (SecurityIntegrationPersistInfo) journal.data();
                    AuthenticationMgrEPack authenticationMgr =
                            (AuthenticationMgrEPack) GlobalStateMgr.getCurrentState().getAuthenticationMgr();
                    authenticationMgr.replayCreateSecurityIntegration(
                            info.name, info.propertyMap);
                    break;
                }
                case OperationTypeEPack.OP_ALTER_SECURITY_INTEGRATION: {
                    SecurityIntegrationPersistInfo info = (SecurityIntegrationPersistInfo) journal.data();
                    AuthenticationMgrEPack authenticationMgr =
                            (AuthenticationMgrEPack) GlobalStateMgr.getCurrentState().getAuthenticationMgr();
                    authenticationMgr.replayAlterSecurityIntegration(
                            info.name, info.propertyMap);
                    break;
                }
                case OperationTypeEPack.OP_DROP_SECURITY_INTEGRATION: {
                    SecurityIntegrationPersistInfo info = (SecurityIntegrationPersistInfo) journal.data();
                    AuthenticationMgrEPack authenticationMgr =
                            (AuthenticationMgrEPack) GlobalStateMgr.getCurrentState().getAuthenticationMgr();
                    authenticationMgr.replayDropSecurityIntegration(info.name);
                    break;
                }
                case OperationTypeEPack.OP_CREATE_ROLE_MAPPING: {
                    RoleMappingPersistInfo info = (RoleMappingPersistInfo) journal.data();
                    globalStateMgr.getAuthorizationMgr().getRoleMappingMetaMgr().replayCreateRoleMapping(
                            info.name, info.propertyMap);
                    break;
                }
                case OperationTypeEPack.OP_ALTER_ROLE_MAPPING: {
                    RoleMappingPersistInfo info = (RoleMappingPersistInfo) journal.data();
                    globalStateMgr.getAuthorizationMgr().getRoleMappingMetaMgr().replayAlterRoleMapping(
                            info.name, info.propertyMap);
                    break;
                }
                case OperationTypeEPack.OP_DROP_ROLE_MAPPING: {
                    RoleMappingPersistInfo info = (RoleMappingPersistInfo) journal.data();
                    globalStateMgr.getAuthorizationMgr().getRoleMappingMetaMgr().replayDropRoleMapping(info.name);
                    break;
                }
                case OperationTypeEPack.OP_CREATE_PASSWORD_POLICY: {
                    CreatePasswordPolicyLog createPasswordPolicyLog = (CreatePasswordPolicyLog) journal.data();
                    globalStateMgr.getSecurityPolicyManager().doCreatePasswordPolicy(createPasswordPolicyLog);
                    break;
                }
                case OperationTypeEPack.OP_DROP_PASSWORD_POLICY: {
                    DropPasswordPolicyLog dropPasswordPolicyLog = (DropPasswordPolicyLog) journal.data();
                    globalStateMgr.getSecurityPolicyManager().doDropPasswordPolicy(dropPasswordPolicyLog);
                    break;
                }
                case OperationTypeEPack.OP_SET_PASSWORD_POLICY: {
                    SetPasswordPolicyLog setPasswordPolicyLog = (SetPasswordPolicyLog) journal.data();
                    globalStateMgr.getSecurityPolicyManager().setGlobalPasswordPolicy(setPasswordPolicyLog.getPasswordPolicyId());
                    break;
                }
                case OperationTypeEPack.OP_UNSET_PASSWORD_POLICY: {
                    globalStateMgr.getSecurityPolicyManager().setGlobalPasswordPolicy(-1);
                    break;
                }
                default: {
                    super.loadJournal(globalStateMgr, journal);
                }
            }
        } catch (Exception e) {
            JournalInconsistentException exception =
                    new JournalInconsistentException(opCode, "failed to load journal type " + opCode);
            exception.initCause(e);
            throw exception;
        }
    }
}
