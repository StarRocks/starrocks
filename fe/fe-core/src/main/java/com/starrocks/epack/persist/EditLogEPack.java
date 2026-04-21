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

import com.starrocks.epack.system.SystemInfo;
import com.starrocks.journal.JournalEntity;
import com.starrocks.journal.JournalInconsistentException;
import com.starrocks.journal.JournalTask;
import com.starrocks.lake.snapshot.ClusterSnapshotMgrEPack;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.WALApplier;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.automv.lifecycle.MVChangeLog;
import com.starrocks.sql.automv.qe.RecommendationsTaskStatus;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class EditLogEPack extends EditLog {
    public EditLogEPack(BlockingQueue<JournalTask> journalQueue) {
        super(journalQueue);
    }

    public void logCreateRoleMapping(String name, Map<String, String> propertyMap) {
        RoleMappingPersistInfo info = new RoleMappingPersistInfo(name, propertyMap);
        logJsonObject(OperationTypeEPack.OP_CREATE_ROLE_MAPPING, info);
    }

    public void logAlterRoleMapping(String name, Map<String, String> alterProps) {
        RoleMappingPersistInfo info = new RoleMappingPersistInfo(name, alterProps);
        logJsonObject(OperationTypeEPack.OP_ALTER_ROLE_MAPPING, info);
    }

    public void logDropRoleMapping(String name) {
        RoleMappingPersistInfo info = new RoleMappingPersistInfo(name, null);
        logJsonObject(OperationTypeEPack.OP_DROP_ROLE_MAPPING, info);
    }

    public void logMVChangeLog(MVChangeLog mvChangeLog) {
        logJsonObject(OperationTypeEPack.OP_MV_CHANGE, mvChangeLog);
    }

    public void logRecommendationsTaskStatusChange(RecommendationsTaskStatus taskStatus) {
        logJsonObject(OperationTypeEPack.OP_RECOMMENDATIONS_TASK_STATUS_CHANGE, taskStatus);
    }

    public void logCreatePasswordPolicy(CreatePasswordPolicyLog createPasswordPolicyLog) {
        logJsonObject(OperationTypeEPack.OP_CREATE_PASSWORD_POLICY, createPasswordPolicyLog);
    }

    public void logDropPasswordPolicy(DropPasswordPolicyLog dropPasswordPolicyLog) {
        logJsonObject(OperationTypeEPack.OP_DROP_PASSWORD_POLICY, dropPasswordPolicyLog);
    }

    public void logSetGlobalPasswordPolicy(SetPasswordPolicyLog setPasswordPolicyLog) {
        logJsonObject(OperationTypeEPack.OP_SET_PASSWORD_POLICY, setPasswordPolicyLog);
    }

    public void logUnsetGlobalPasswordPolicy(UnsetPasswordPolicyLog unsetPasswordPolicyLog) {
        logJsonObject(OperationTypeEPack.OP_UNSET_PASSWORD_POLICY, unsetPasswordPolicyLog);
    }

    public void logManualClusterSnapshotLog(ManualClusterSnapshotLog info, WALApplier walApplier) {
        logJsonObject(OperationTypeEPack.OP_MANUAL_CLUSTER_SNAPSHOT_LOG, info, walApplier);
    }

    public void logInitSystemInfo(SystemInfo info, WALApplier applier) {
        logJsonObject(OperationTypeEPack.OP_INIT_SYSTEM_INFO, info, applier);
    }

    public void logRegisterLicense(RegisterLicenseLog log, WALApplier applier) {
        logJsonObject(OperationTypeEPack.OP_REGISTER_LICENSE, log, applier);
    }

    public void logUpdateScaleOutLicenseFreeStartTime(ScaleOutLicenseFreeStartTimeLog log, WALApplier applier) {
        logJsonObject(OperationTypeEPack.OP_UPDATE_SCALE_OUT_LICENSE_FREE_START_TIME, log, applier);
    }

    @Override
    public void loadJournal(GlobalStateMgr globalStateMgr, JournalEntity journal)
            throws JournalInconsistentException {

        short opCode = journal.opCode();
        try {
            switch (opCode) {
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
                case OperationTypeEPack.OP_MANUAL_CLUSTER_SNAPSHOT_LOG: {
                    ManualClusterSnapshotLog log = (ManualClusterSnapshotLog) journal.data();
                    ClusterSnapshotMgrEPack clusterSnapshotMgrEpack =
                                    (ClusterSnapshotMgrEPack) globalStateMgr.getClusterSnapshotMgr();
                    clusterSnapshotMgrEpack.replayManualLog(log);
                    break;
                }
                case OperationTypeEPack.OP_INIT_SYSTEM_INFO: {
                    globalStateMgr.getLicenseMgr().applyInitSystemInfo((SystemInfo) journal.data());
                    break;
                }
                case OperationTypeEPack.OP_REGISTER_LICENSE: {
                    RegisterLicenseLog log = (RegisterLicenseLog) journal.data();
                    globalStateMgr.getLicenseMgr().applyRegisterLicense(log);
                    break;
                }
                case OperationTypeEPack.OP_UPDATE_SCALE_OUT_LICENSE_FREE_START_TIME: {
                    ScaleOutLicenseFreeStartTimeLog log = (ScaleOutLicenseFreeStartTimeLog) journal.data();
                    globalStateMgr.getLicenseMgr().applyScaleOutLicenseFreeStartTime(log);
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
