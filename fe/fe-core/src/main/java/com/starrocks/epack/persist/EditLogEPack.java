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

import com.starrocks.context.ai.AIProvider;
import com.starrocks.epack.authorization.DbUID;
import com.starrocks.epack.authorization.Policy;
import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.epack.system.SystemInfo;
import com.starrocks.journal.JournalEntity;
import com.starrocks.journal.JournalInconsistentException;
import com.starrocks.journal.JournalTask;
import com.starrocks.lake.bookmark.BookmarkLogEntry;
import com.starrocks.lake.restore.SnapshotRestoreJob;
import com.starrocks.lake.snapshot.ClusterSnapshotMgrEPack;
import com.starrocks.persist.ContextOpLog;
import com.starrocks.persist.DropAIProviderLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.SetDefaultAIProviderLog;
import com.starrocks.persist.WALApplier;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.PolicyName;
import com.starrocks.sql.automv.lifecycle.MVChangeLog;
import com.starrocks.sql.automv.qe.RecommendationsTaskStatus;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class EditLogEPack extends EditLog {
    public EditLogEPack(BlockingQueue<JournalTask> journalQueue) {
        super(journalQueue);
    }

    public EditLogEPack(BlockingQueue<JournalTask> journalQueue, boolean gateOpen) {
        super(journalQueue, gateOpen);
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

    public void logUpdateLicenseUsage(LicenseUsageLog log, WALApplier applier) {
        logJsonObject(OperationTypeEPack.OP_UPDATE_LICENSE_USAGE, log, applier);
    }

    public void logBookmarkEntry(BookmarkLogEntry entry, WALApplier applier) {
        logJsonObject(OperationTypeEPack.OP_BOOKMARK_LOG, entry, applier);
    }

    public void logCreateContextBase(ContextOpLog log, WALApplier walApplier) {
        logJsonObject(OperationTypeEPack.OP_CREATE_CONTEXTBASE, log, walApplier);
    }

    public void logAlterContextBase(ContextOpLog log, WALApplier walApplier) {
        logJsonObject(OperationTypeEPack.OP_ALTER_CONTEXTBASE, log, walApplier);
    }

    public void logRenameContextBase(ContextOpLog log, WALApplier walApplier) {
        logJsonObject(OperationTypeEPack.OP_RENAME_CONTEXTBASE, log, walApplier);
    }

    public void logDropContextBase(ContextOpLog log, WALApplier walApplier) {
        logJsonObject(OperationTypeEPack.OP_DROP_CONTEXTBASE, log, walApplier);
    }

    public void logCreateContextCollection(ContextOpLog log, WALApplier walApplier) {
        logJsonObject(OperationTypeEPack.OP_CREATE_CONTEXT_COLLECTION, log, walApplier);
    }

    public void logDropContextCollection(ContextOpLog log, WALApplier walApplier) {
        logJsonObject(OperationTypeEPack.OP_DROP_CONTEXT_COLLECTION, log, walApplier);
    }

    public void logCreateContextWorkspace(ContextOpLog log, WALApplier walApplier) {
        logJsonObject(OperationTypeEPack.OP_CREATE_CONTEXT_WORKSPACE, log, walApplier);
    }

    public void logDropContextWorkspace(ContextOpLog log, WALApplier walApplier) {
        logJsonObject(OperationTypeEPack.OP_DROP_CONTEXT_WORKSPACE, log, walApplier);
    }

    public void logCreateContextRetrievalProfile(ContextOpLog log, WALApplier walApplier) {
        logJsonObject(OperationTypeEPack.OP_CREATE_CONTEXT_RETRIEVAL_PROFILE, log, walApplier);
    }

    public void logDropContextRetrievalProfile(ContextOpLog log, WALApplier walApplier) {
        logJsonObject(OperationTypeEPack.OP_DROP_CONTEXT_RETRIEVAL_PROFILE, log, walApplier);
    }

    public void logCreateAIProvider(AIProvider provider, WALApplier walApplier) {
        logJsonObject(OperationTypeEPack.OP_CREATE_AI_PROVIDER, provider, walApplier);
    }

    public void logAlterAIProvider(AIProvider provider, WALApplier walApplier) {
        logJsonObject(OperationTypeEPack.OP_ALTER_AI_PROVIDER, provider, walApplier);
    }

    public void logDropAIProvider(DropAIProviderLog log, WALApplier walApplier) {
        logJsonObject(OperationTypeEPack.OP_DROP_AI_PROVIDER, log, walApplier);
    }

    public void logSetDefaultAIProvider(SetDefaultAIProviderLog log, WALApplier walApplier) {
        logJsonObject(OperationTypeEPack.OP_SET_DEFAULT_AI_PROVIDER, log, walApplier);
    }

    public void logCreateMaskingPolicy(Policy policy) {
        CreatePolicyLog createPolicyInfo = new CreatePolicyLog(policy);
        logJsonObject(OperationTypeEPack.OP_CREATE_MASKING_POLICY, createPolicyInfo);
    }

    public void logCreateRowAccessPolicy(Policy policy) {
        CreatePolicyLog createPolicyInfo = new CreatePolicyLog(policy);
        logJsonObject(OperationTypeEPack.OP_CREATE_ROW_ACCESS_POLICY, createPolicyInfo);
    }

    public void logDropPolicy(PolicyName policyName, DbUID db, Policy policy) {
        DropPolicyLog dropPolicyLog = new DropPolicyLog(policy.getPolicyType(), policy.getPolicyId(), db,
                policyName.getName());
        logJsonObject(OperationTypeEPack.OP_DROP_POLICY, dropPolicyLog);
    }

    public void logAlterPolicySetBody(PolicyName policyName, PolicyType policyType, String policyBody) {
        AlterPolicyLog alterPolicyInfo = new AlterPolicyLog(policyName, policyType,
                new AlterPolicyLog.PolicySetBodyInfo(policyBody));
        logJsonObject(OperationTypeEPack.OP_ALTER_POLICY_SET_BODY, alterPolicyInfo);
    }

    public void logAlterPolicySetComment(PolicyName policyName, PolicyType policyType, String comment) {
        AlterPolicyLog alterPolicyInfo = new AlterPolicyLog(policyName, policyType,
                new AlterPolicyLog.PolicySetCommentInfo(comment));
        logJsonObject(OperationTypeEPack.OP_ALTER_POLICY_SET_COMMENT, alterPolicyInfo);
    }

    public void logAlterPolicyRename(PolicyName policyName, PolicyType policyType, String newPolicyName) {
        AlterPolicyLog alterPolicyInfo = new AlterPolicyLog(policyName, policyType,
                new AlterPolicyLog.PolicyRenameInfo(newPolicyName));
        logJsonObject(OperationTypeEPack.OP_ALTER_POLICY_RENAME, alterPolicyInfo);
    }

    public void logApplyMaskingPolicy(ApplyOrRevokeMaskingPolicyLog applyMaskingPolicyInfo) {
        logJsonObject(OperationTypeEPack.OP_APPLY_MASKING_POLICY, applyMaskingPolicyInfo);
    }

    public void logApplyRowAccessPolicy(ApplyOrRevokeRowAccessPolicyLog applyMaskingPolicyInfo) {
        logJsonObject(OperationTypeEPack.OP_APPLY_ROW_ACCESS_POLICY, applyMaskingPolicyInfo);
    }

    public void logRevokeMaskingPolicy(ApplyOrRevokeMaskingPolicyLog applyMaskingPolicyInfo) {
        logJsonObject(OperationTypeEPack.OP_REVOKE_MASKING_POLICY, applyMaskingPolicyInfo);
    }

    public void logRevokeRowAccessPolicy(ApplyOrRevokeRowAccessPolicyLog applyMaskingPolicyInfo) {
        logJsonObject(OperationTypeEPack.OP_REVOKE_ROW_ACCESS_POLICY, applyMaskingPolicyInfo);
    }

    // failover group
    public void logCreateFailoverGroup(FailoverGroup failoverGroup) {
        CreateFailoverGroupLog createFailoverGroupLog = new CreateFailoverGroupLog(failoverGroup);
        logJsonObject(OperationTypeEPack.OP_CREATE_FAILOVER_GROUP, createFailoverGroupLog);
    }

    public void logDropFailoverGroup(long failoverGroupId) {
        DropFailoverGroupLog dropFailoverGroupLog = new DropFailoverGroupLog(failoverGroupId);
        logJsonObject(OperationTypeEPack.OP_DROP_FAILOVER_GROUP, dropFailoverGroupLog);
    }

    public void logUpdateFailoverGroup(FailoverGroup failoverGroup) {
        UpdateFailoverGroupLog updateFailoverGroupLog = new UpdateFailoverGroupLog(failoverGroup);
        logJsonObject(OperationTypeEPack.OP_UPDATE_FAILOVER_GROUP, updateFailoverGroupLog);
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
                case OperationTypeEPack.OP_RESTORE_FROM_SNAPSHOT: {
                    SnapshotRestoreJob job = (SnapshotRestoreJob) journal.data();
                    GlobalStateMgr.getCurrentState().getBackupHandler().replayAddJob(job);
                    break;
                }
                case OperationTypeEPack.OP_MV_CHANGE: {
                    MVChangeLog mvChangeLog = (MVChangeLog) journal.data();
                    globalStateMgr.getMVLifecycleManager().replayMVChangeLog(mvChangeLog);
                    break;
                }
                case OperationTypeEPack.OP_RECOMMENDATIONS_TASK_STATUS_CHANGE: {
                    RecommendationsTaskStatus taskStatus = (RecommendationsTaskStatus) journal.data();
                    globalStateMgr.getRecommendationsTaskMgr().applyLogEntry(taskStatus);
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
                case OperationTypeEPack.OP_UPDATE_LICENSE_USAGE: {
                    LicenseUsageLog log = (LicenseUsageLog) journal.data();
                    globalStateMgr.getLicenseMgr().applyUpdateLicenseUsage(log);
                    break;
                }
                case OperationTypeEPack.OP_BOOKMARK_LOG: {
                    BookmarkLogEntry entry = (BookmarkLogEntry) journal.data();
                    globalStateMgr.getBookmarkManager().replay(entry);
                    break;
                }
                case OperationTypeEPack.OP_CREATE_CONTEXTBASE: {
                    globalStateMgr.getContextMgr().replayCreateContextBase((ContextOpLog) journal.data());
                    break;
                }
                case OperationTypeEPack.OP_ALTER_CONTEXTBASE: {
                    globalStateMgr.getContextMgr().replayAlterContextBase((ContextOpLog) journal.data());
                    break;
                }
                case OperationTypeEPack.OP_RENAME_CONTEXTBASE: {
                    globalStateMgr.getContextMgr().replayRenameContextBase((ContextOpLog) journal.data());
                    break;
                }
                case OperationTypeEPack.OP_DROP_CONTEXTBASE: {
                    globalStateMgr.getContextMgr().replayDropContextBase((ContextOpLog) journal.data());
                    break;
                }
                case OperationTypeEPack.OP_CREATE_CONTEXT_COLLECTION: {
                    globalStateMgr.getContextMgr().replayCreateCollection((ContextOpLog) journal.data());
                    break;
                }
                case OperationTypeEPack.OP_DROP_CONTEXT_COLLECTION: {
                    globalStateMgr.getContextMgr().replayDropCollection((ContextOpLog) journal.data());
                    break;
                }
                case OperationTypeEPack.OP_CREATE_CONTEXT_WORKSPACE: {
                    globalStateMgr.getContextMgr().replayCreateWorkspace((ContextOpLog) journal.data());
                    break;
                }
                case OperationTypeEPack.OP_DROP_CONTEXT_WORKSPACE: {
                    globalStateMgr.getContextMgr().replayDropWorkspace((ContextOpLog) journal.data());
                    break;
                }
                case OperationTypeEPack.OP_CREATE_CONTEXT_RETRIEVAL_PROFILE: {
                    globalStateMgr.getContextMgr().replayCreateRetrievalProfile((ContextOpLog) journal.data());
                    break;
                }
                case OperationTypeEPack.OP_DROP_CONTEXT_RETRIEVAL_PROFILE: {
                    globalStateMgr.getContextMgr().replayDropRetrievalProfile((ContextOpLog) journal.data());
                    break;
                }
                case OperationTypeEPack.OP_CREATE_AI_PROVIDER: {
                    AIProvider provider = (AIProvider) journal.data();
                    globalStateMgr.getAIProviderMgr().replayCreateProvider(provider);
                    break;
                }
                case OperationTypeEPack.OP_ALTER_AI_PROVIDER: {
                    AIProvider provider = (AIProvider) journal.data();
                    globalStateMgr.getAIProviderMgr().replayAlterProvider(provider);
                    break;
                }
                case OperationTypeEPack.OP_DROP_AI_PROVIDER: {
                    DropAIProviderLog log = (DropAIProviderLog) journal.data();
                    globalStateMgr.getAIProviderMgr().replayDropProvider(log);
                    break;
                }
                case OperationTypeEPack.OP_SET_DEFAULT_AI_PROVIDER: {
                    SetDefaultAIProviderLog log = (SetDefaultAIProviderLog) journal.data();
                    globalStateMgr.getAIProviderMgr().replaySetDefaultProvider(log);
                    break;
                }
                case OperationTypeEPack.OP_CREATE_MASKING_POLICY:
                case OperationTypeEPack.OP_CREATE_ROW_ACCESS_POLICY: {
                    CreatePolicyLog policy = (CreatePolicyLog) journal.data();
                    globalStateMgr.getSecurityPolicyManager().replayCreatePolicy(policy);
                    break;
                }
                case OperationTypeEPack.OP_DROP_POLICY: {
                    DropPolicyLog policy = (DropPolicyLog) journal.data();
                    globalStateMgr.getSecurityPolicyManager().replayDropPolicy(policy);
                    break;
                }
                case OperationTypeEPack.OP_ALTER_POLICY_SET_BODY:
                case OperationTypeEPack.OP_ALTER_POLICY_SET_COMMENT:
                case OperationTypeEPack.OP_ALTER_POLICY_RENAME: {
                    AlterPolicyLog alterPolicyInfo = (AlterPolicyLog) journal.data();
                    globalStateMgr.getSecurityPolicyManager().replayAlterPolicy(alterPolicyInfo);
                    break;
                }
                case OperationTypeEPack.OP_APPLY_MASKING_POLICY: {
                    ApplyOrRevokeMaskingPolicyLog applyMaskingPolicyInfo = (ApplyOrRevokeMaskingPolicyLog) journal
                            .data();
                    globalStateMgr.getSecurityPolicyManager().registerMaskingPolicyContext(applyMaskingPolicyInfo);
                    break;
                }
                case OperationTypeEPack.OP_APPLY_ROW_ACCESS_POLICY: {
                    ApplyOrRevokeRowAccessPolicyLog applyRowAccessPolicyInfo = (ApplyOrRevokeRowAccessPolicyLog) journal
                            .data();
                    globalStateMgr.getSecurityPolicyManager().registerRowAccessPolicyContext(applyRowAccessPolicyInfo);
                    break;
                }
                case OperationTypeEPack.OP_REVOKE_MASKING_POLICY: {
                    ApplyOrRevokeMaskingPolicyLog applyMaskingPolicyInfo = (ApplyOrRevokeMaskingPolicyLog) journal
                            .data();
                    globalStateMgr.getSecurityPolicyManager().replayRevokeMaskingPolicyContext(applyMaskingPolicyInfo);
                    break;
                }
                case OperationTypeEPack.OP_REVOKE_ROW_ACCESS_POLICY: {
                    ApplyOrRevokeRowAccessPolicyLog applyRowAccessPolicyInfo = (ApplyOrRevokeRowAccessPolicyLog) journal
                            .data();
                    globalStateMgr.getSecurityPolicyManager()
                            .replayRevokeRowAccessPolicyContext(applyRowAccessPolicyInfo);
                    break;
                }
                case OperationTypeEPack.OP_CREATE_FAILOVER_GROUP: {
                    CreateFailoverGroupLog createFailoverGroupLog = (CreateFailoverGroupLog) journal.data();
                    globalStateMgr.getFailoverGroupMgr()
                            .replayCreateFailoverGroup(createFailoverGroupLog.getFailoverGroup());
                    break;
                }
                case OperationTypeEPack.OP_DROP_FAILOVER_GROUP: {
                    DropFailoverGroupLog dropFailoverGroupLog = (DropFailoverGroupLog) journal.data();
                    globalStateMgr.getFailoverGroupMgr()
                            .replayDropFailoverGroup(dropFailoverGroupLog.getFailoverGroupId());
                    break;
                }
                case OperationTypeEPack.OP_UPDATE_FAILOVER_GROUP: {
                    UpdateFailoverGroupLog updateFailoverGroupLog = (UpdateFailoverGroupLog) journal.data();
                    globalStateMgr.getFailoverGroupMgr()
                            .replayUpdateFailoverGroup(updateFailoverGroupLog.getFailoverGroup());
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
