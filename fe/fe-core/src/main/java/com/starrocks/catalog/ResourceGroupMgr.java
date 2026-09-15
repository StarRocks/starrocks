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

package com.starrocks.catalog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.authorization.RolePrivilegeCollectionV2;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.AlterResourceGroupLog;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.ResourceGroupOpEntry;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.ResourceGroupAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterResourceGroupStmt;
import com.starrocks.sql.ast.CreateResourceGroupStmt;
import com.starrocks.sql.ast.DropResourceGroupStmt;
import com.starrocks.sql.ast.ShowResourceGroupStmt;
import com.starrocks.sql.optimizer.cost.feature.CostPredictor;
import com.starrocks.system.BackendResourceStat;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TWorkGroup;
import com.starrocks.thrift.TWorkGroupOp;
import com.starrocks.thrift.TWorkGroupOpType;
import com.starrocks.thrift.TWorkGroupType;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static com.starrocks.server.WarehouseManager.DEFAULT_WAREHOUSE_ID;

// WorkGroupMgr is employed by GlobalStateMgr to manage WorkGroup in FE.
public class ResourceGroupMgr implements Writable {
    private static final Logger LOG = LogManager.getLogger(ResourceGroupMgr.class);

    public static final String SHORT_QUERY_SET_EXCLUSIVE_CPU_CORES_ERR_MSG =
            "'short_query' ResourceGroup cannot set 'exclusive_cpu_cores', " +
                    "since it use 'cpu_weight' as 'exclusive_cpu_cores'";

    // ---------------------------------------------------------------------------
    // Immutable holder for all three CopyOnWrite index maps.
    // A single write to the AtomicReference provides an atomic, consistent view
    // of every index — eliminating the window where three separate writes
    // could be observed in a torn order by a lock-free reader (Issue 2).
    // ---------------------------------------------------------------------------
    static final class ResourceGroupSnapshot {
        static final ResourceGroupSnapshot EMPTY = new ResourceGroupSnapshot(
                Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), null);

        /** Groups keyed by name. */
        final Map<String, ResourceGroup>         byName;
        /** Groups keyed by ID. */
        final Map<Long, ResourceGroup>           byId;
        /** All classifiers keyed by classifier ID, across all groups. */
        final Map<Long, ResourceGroupClassifier> byClassifier;
        /** The short_query resource group (if any). */
        final ResourceGroup                      shortQueryResourceGroup;

        ResourceGroupSnapshot(Map<String, ResourceGroup> byName,
                              Map<Long, ResourceGroup> byId,
                              Map<Long, ResourceGroupClassifier> byClassifier,
                              ResourceGroup shortQueryResourceGroup) {
            this.byName                  = Collections.unmodifiableMap(byName);
            this.byId                    = Collections.unmodifiableMap(byId);
            this.byClassifier            = Collections.unmodifiableMap(byClassifier);
            this.shortQueryResourceGroup = shortQueryResourceGroup;
        }
    }

    // AtomicReference replaces volatile for SonarCloud S3077 compliance.
    // Writers replace the entire holder via set(); readers capture via get()
    // once and access all three maps without any lock.
    // Because ResourceGroup objects are deep-copied on every alter (Issue 1 fix),
    // holders of an older snapshot always see a consistent pre-alter view.
    private final AtomicReference<ResourceGroupSnapshot> snapshot =
            new AtomicReference<>(ResourceGroupSnapshot.EMPTY);

    // Package-private test hooks: used by unit tests to construct and inject
    // snapshot state with strong typing and without reflection.
    @VisibleForTesting
    static ResourceGroupSnapshot newSnapshotForTest(
            Map<String, ResourceGroup> byName,
            Map<Long, ResourceGroup> byId,
            Map<Long, ResourceGroupClassifier> byClassifier,
            ResourceGroup shortQueryResourceGroup) {
        return new ResourceGroupSnapshot(
                byName, byId, byClassifier, shortQueryResourceGroup);
    }

    @VisibleForTesting
    void setSnapshotForTest(ResourceGroupSnapshot snap) {
        this.snapshot.set(snap);
    }

    @VisibleForTesting
    ResourceGroupSnapshot getSnapshotForTest() {
        return this.snapshot.get();
    }

    private final List<TWorkGroupOp> resourceGroupOps = new ArrayList<>();
    private final Map<Long, Map<Long, TWorkGroup>> activeResourceGroupsPerBe = new HashMap<>();
    private final Map<Long, Long> minVersionPerBe = new HashMap<>();
    // Write lock provides mutual exclusion for DDL (create/alter/drop) operations only.
    // Read operations (chooseResourceGroup, getResourceGroup, etc.) are lock-free.
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile boolean hasCreatedDefaultResourceGroups = false;


    @VisibleForTesting
    void writeLock() {
        lock.writeLock().lock();
    }

    @VisibleForTesting
    void writeUnlock() {
        lock.writeLock().unlock();
    }

    // readLock/readUnlock are retained for getResourceGroupsNeedToDeliver, which protects
    // the non-volatile resourceGroupOps and activeResourceGroupsPerBe fields.
    // Classification hot-path methods (chooseResourceGroup, getResourceGroup, etc.) do NOT
    // use these — they read the volatile snapshot fields lock-free.
    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    public void createResourceGroup(CreateResourceGroupStmt stmt) throws DdlException {
        writeLock();
        try {
            ResourceGroup wg = ResourceGroupBuilder.buildFromStmt(stmt);
            boolean needReplace = false;
            if (snapshot.get().byName.containsKey(wg.getName())) {
                if (stmt.isReplaceIfExists()) {
                    needReplace = true;
                } else if (!stmt.isIfNotExists()) {
                    throw new DdlException(String.format("RESOURCE_GROUP(%s) already exists", wg.getName()));
                } else {
                    return;
                }
            }

            ResourceGroup sqrg = snapshot.get().shortQueryResourceGroup;
            if (wg.getResourceGroupType() == TWorkGroupType.WG_SHORT_QUERY && sqrg != null
                    && !(needReplace && sqrg.getName().equals(wg.getName()))) {
                throw new DdlException(
                        String.format("There can be only one short_query RESOURCE_GROUP (%s)",
                                sqrg.getName()));
            }

            if (wg.getClassifiers() != null && !wg.getClassifiers().isEmpty() &&
                    wg.getResourceGroupType().equals(TWorkGroupType.WG_MV)) {
                throw new DdlException("MV Resource Group not support classifiers.");
            }

            if ((wg.getClassifiers() == null || wg.getClassifiers().isEmpty()) &&
                    !ResourceGroup.BUILTIN_WG_NAMES.contains(wg.getName()) &&
                    !wg.getResourceGroupType().equals(TWorkGroupType.WG_MV)) {
                throw new DdlException("This type Resource Group need define classifiers.");
            }

            validateExclusiveCpuCoresInlock(
                    wg.getNormalizedExclusiveCpuCores(), wg.getExclusiveCpuPercent(), wg.getWarehouses(), wg);

            if (!wg.hasDefaultMemPool() && !resourceGroupInMemPoolHaveSameMemLimit(wg)) {
                throw new DdlException(
                        "Property `mem_limit` must be equal for all resource groups using the mem_pool [" +
                                wg.getMemPool() + "].");
            }

            wg.normalizeCpuWeight();

            ResourceGroup oldWg = needReplace ? snapshot.get().byName.get(wg.getName()) : null;
            if (oldWg != null) {
                wg.setId(oldWg.getId());
            } else if (ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME.equals(wg.getName())) {
                wg.setId(ResourceGroup.DEFAULT_WG_ID);
            } else if (ResourceGroup.DEFAULT_MV_RESOURCE_GROUP_NAME.equals(wg.getName())) {
                wg.setId(ResourceGroup.DEFAULT_MV_WG_ID);
            } else {
                wg.setId(GlobalStateMgr.getCurrentState().getNextId());
            }

            wg.setVersion(GlobalStateMgr.getCurrentState().getNextId());
            if (wg.getClassifiers() != null) {
                for (ResourceGroupClassifier classifier : wg.getClassifiers()) {
                    classifier.setResourceGroupId(wg.getId());
                    classifier.setId(GlobalStateMgr.getCurrentState().getNextId());
                }
            }

            TWorkGroupOpType opType = (oldWg != null)
                    ? TWorkGroupOpType.WORKGROUP_OP_ALTER
                    : TWorkGroupOpType.WORKGROUP_OP_CREATE;
            ResourceGroupOpEntry workGroupOp = new ResourceGroupOpEntry(opType, wg);
            final boolean replacing = (oldWg != null);
            final String replacedName = wg.getName();
            GlobalStateMgr.getCurrentState().getEditLog().logResourceGroupOp(workGroupOp, wal -> {
                if (replacing) {
                    // Single volatile write: atomically replaces old entry with new entry.
                    replaceResourceGroupInternal(replacedName, wg);
                } else {
                    addResourceGroupInternal(wg);
                }
            });
            resourceGroupOps.add(workGroupOp.toThrift());
        } finally {
            writeUnlock();
        }
    }

    public List<List<String>> showResourceGroup(ShowResourceGroupStmt stmt) {
        String name = stmt.getName();
        if (name != null) {
            ResourceGroup rg = snapshot.get().byName.get(name);
            if (rg == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERROR_NO_RG_ERROR, name);
            }
            return rg.show(stmt.isVerbose());
        } else {
            return showAllResourceGroups(ConnectContext.get(), stmt.isVerbose(), stmt.isListAll());
        }
    }

    public List<Long> getResourceGroupIds() {
        // Lock-free: capture volatile snapshot once.
        return new ArrayList<>(snapshot.get().byId.keySet());
    }

    private boolean resourceGroupInMemPoolHaveSameMemLimit(ResourceGroup wg) {
        if (wg.hasDefaultMemPool()) {
            return true;
        }
        return snapshot.get().byName.entrySet().stream().allMatch(entry ->
                !Objects.equals(wg.getMemPool(), entry.getValue().getMemPool()) ||
                entry.getKey().equals(wg.getName()) ||
                Objects.equals(wg.getMemLimit(), entry.getValue().getMemLimit()));
    }

    private String getUnqualifiedUser(ConnectContext ctx) {
        Preconditions.checkArgument(ctx != null);
        String qualifiedUser = ctx.getQualifiedUser();
        //default_cluster:test
        String[] userParts = qualifiedUser.split(":");
        return userParts[userParts.length - 1];
    }

    private List<String> getUnqualifiedRole(ConnectContext ctx) {
        Preconditions.checkArgument(ctx != null);

        try {
            AuthorizationMgr manager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
            List<String> validRoles = new ArrayList<>();

            Set<Long> activeRoles = ctx.getCurrentRoleIds();
            if (activeRoles == null) {
                activeRoles = manager.getRoleIdsByUser(ctx.getCurrentUserIdentity());
            }

            for (Long roleId : activeRoles) {
                RolePrivilegeCollectionV2 rolePrivilegeCollection =
                        manager.getRolePrivilegeCollectionUnlocked(roleId, false);
                if (rolePrivilegeCollection != null) {
                    validRoles.add(rolePrivilegeCollection.getName());
                }
            }

            return validRoles.stream().filter(r -> !PrivilegeBuiltinConstants.BUILT_IN_ROLE_NAMES.contains(r))
                    .collect(Collectors.toList());
        } catch (PrivilegeException e) {
            LOG.info("getUnqualifiedRole failed for resource group, error message: " + e.getMessage());
            return null;
        }
    }

    public List<List<String>> showAllResourceGroups(ConnectContext ctx, boolean verbose, boolean isListAll) {
        // Lock-free: capture snapshot once — all three indexes are consistent.
        List<ResourceGroup> resourceGroupList = new ArrayList<>(snapshot.get().byName.values());
        if (isListAll || ConnectContext.get() == null) {
            resourceGroupList.sort(Comparator.comparing(ResourceGroup::getName));
            return resourceGroupList.stream()
                    .map(rg -> rg.show(verbose))
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
        } else {
            String user = getUnqualifiedUser(ctx);
            List<String> activeRoles = getUnqualifiedRole(ctx);
            String remoteIp = ctx.getRemoteIP();
            return resourceGroupList.stream()
                    .map(rg -> rg.showVisible(user, activeRoles, remoteIp, verbose))
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
        }
    }

    public List<List<String>> showOneResourceGroup(String name, boolean verbose) {
        // Lock-free: capture snapshot once.
        ResourceGroup rg = this.snapshot.get().byName.get(name);
        if (rg == null) {
            return Collections.emptyList();
        } else {
            return rg.show(verbose);
        }
    }

    public Set<String> getAllResourceGroupNames() {
        // Lock-free: defensive copy of snapshot keyset.
        return new HashSet<>(snapshot.get().byName.keySet());
    }

    private void replayAddResourceGroup(ResourceGroup workgroup) {
        addResourceGroupInternal(workgroup);
        ResourceGroupOpEntry op = new ResourceGroupOpEntry(TWorkGroupOpType.WORKGROUP_OP_CREATE, workgroup);
        resourceGroupOps.add(op.toThrift());
    }

    public ResourceGroup getResourceGroup(String name) {
        // Lock-free: AtomicReference read gives a consistent snapshot.
        return snapshot.get().byName.getOrDefault(name, null);
    }

    public ResourceGroup getResourceGroup(long id) {
        // Lock-free: AtomicReference read gives a consistent snapshot.
        return snapshot.get().byId.getOrDefault(id, null);
    }

    private int getExclusiveCpuCores(Integer exclusiveCpuCores, Integer exclusiveCpuPercent, int minCoreNum) {
        if (exclusiveCpuCores != null && exclusiveCpuCores > 0) {
            return exclusiveCpuCores;
        } else if (exclusiveCpuPercent != null && exclusiveCpuPercent > 0) {
            return minCoreNum * exclusiveCpuPercent / 100;
        } else {
            return 0;
        }
    }

    private static class WarehouseCoresInfo {
        private final int minCores;
        private int sumExclusiveCpuCores = 0;

        private WarehouseCoresInfo(int minCores) {
            this.minCores = minCores;
        }
    }

    /**
     * For each warehouse, the sum of the exclusive CPU cores of all effective resource groups on that warehouse plus one must
     * not exceed the number of CPU cores of the smallest BE in that resource group.
     *
     * <p> The resource groups that are effective on a warehouse are defined as follows:
     * - For a warehouse with bound resource groups: both the resource groups bound to that warehouse and the resource groups not
     * bound to any warehouse.
     * - For a warehouse without bound resource groups: the resource groups not bound to any warehouse.
     */
    private void validateExclusiveCpuCoresInlock(Integer exclusiveCpuCores, Integer exclusiveCpuPercent, List<String> warehouses,
                                                 ResourceGroup wg)
            throws DdlException {
        Set<Long> boundWhIds = Sets.newHashSet();
        Map<String, WarehouseCoresInfo> boundWhToItem = new HashMap<>();

        List<ResourceGroup> groups = new ArrayList<>(snapshot.get().byName.values());
        if (!snapshot.get().byName.containsKey(wg.getName())) {
            groups.add(wg);
        }

        // First, iterate over the resource groups that are bound to a warehouse to determine
        // which warehouses have resource groups bound to them.
        for (ResourceGroup group : groups) {
            if (group.getWarehouses() == null || group.getWarehouses().isEmpty()) {
                continue;
            }

            Integer curExclusiveCpuCores;
            Integer curExclusiveCpuPercent;
            List<String> curWarehouses;
            if (group.getName().equals(wg.getName())) {
                curExclusiveCpuCores = exclusiveCpuCores;
                curExclusiveCpuPercent = exclusiveCpuPercent;
                curWarehouses = warehouses;
            } else {
                curExclusiveCpuCores = group.getNormalizedExclusiveCpuCores();
                curExclusiveCpuPercent = group.getExclusiveCpuPercent();
                curWarehouses = group.getWarehouses();
            }

            for (String warehouseName : curWarehouses) {
                WarehouseCoresInfo item = boundWhToItem.get(warehouseName);
                if (item == null) {
                    Warehouse wh = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(warehouseName);
                    if (wh == null) {
                        continue;
                    }
                    boundWhIds.add(wh.getId());
                    int minCores = BackendResourceStat.getInstance().getMinNumCoresOfBe(wh.getId());
                    item = new WarehouseCoresInfo(minCores);
                    boundWhToItem.put(warehouseName, item);
                }

                int exclusiveCores = getExclusiveCpuCores(curExclusiveCpuCores, curExclusiveCpuPercent, item.minCores);
                item.sumExclusiveCpuCores += exclusiveCores;
            }
        }

        // Then, iterate over the resource groups that are not bound to any warehouse.
        final int nonBoundWhMinCores = BackendResourceStat.getInstance().getMinNumCoresOfBeExceptWarehouses(boundWhIds);
        int nonBoundWhSumExclusiveCores = 0;
        for (ResourceGroup group : groups) {
            if (group.getWarehouses() != null && !group.getWarehouses().isEmpty()) {
                continue;
            }

            Integer curExclusiveCpuCores;
            Integer curExclusiveCpuPercent;
            if (group.getName().equals(wg.getName())) {
                curExclusiveCpuCores = exclusiveCpuCores;
                curExclusiveCpuPercent = exclusiveCpuPercent;
            } else {
                curExclusiveCpuCores = group.getNormalizedExclusiveCpuCores();
                curExclusiveCpuPercent = group.getExclusiveCpuPercent();
            }

            int nonUsedWhExclusiveCores = getExclusiveCpuCores(curExclusiveCpuCores, curExclusiveCpuPercent, nonBoundWhMinCores);
            nonBoundWhSumExclusiveCores += nonUsedWhExclusiveCores;

            for (WarehouseCoresInfo item : boundWhToItem.values()) {
                int exclusiveCores = getExclusiveCpuCores(curExclusiveCpuCores, curExclusiveCpuPercent, item.minCores);
                item.sumExclusiveCpuCores += exclusiveCores;
            }
        }

        if (nonBoundWhSumExclusiveCores + 1 > nonBoundWhMinCores) {
            throw new DdlException(String.format("The effective exclusive CPU allocation (%d) exceeds the available cores " +
                    "(%d, that is, total cores minus one reserved for non-exclusive groups) on the smallest BE " +
                    "not assigned to any warehouse.", nonBoundWhSumExclusiveCores, nonBoundWhMinCores - 1));
        }
        for (Map.Entry<String, WarehouseCoresInfo> entry : boundWhToItem.entrySet()) {
            String warehouseName = entry.getKey();
            WarehouseCoresInfo item = entry.getValue();
            if (item.sumExclusiveCpuCores + 1 > item.minCores) {
                throw new DdlException(String.format("The effective exclusive CPU allocation (%d) exceeds the available cores " +
                        "(%d, that is, total cores minus one reserved for non-exclusive groups) on the smallest BE " +
                        "of warehouse %s.", item.sumExclusiveCpuCores, item.minCores - 1, warehouseName));
            }
        }
    }

    public void alterResourceGroup(AlterResourceGroupStmt stmt) throws DdlException {
        writeLock();
        try {
            String name = stmt.getName();
            if (!snapshot.get().byName.containsKey(name)) {
                throw new DdlException("RESOURCE_GROUP(" + name + ") does not exist");
            }
            ResourceGroup wg = snapshot.get().byName.get(name);
            AlterResourceGroupLog alterResourceGroupLog = new AlterResourceGroupLog();
            alterResourceGroupLog.setName(name);
            AlterResourceGroupStmt.SubCommand cmd = stmt.getCmd();
            if (wg.getResourceGroupType() == TWorkGroupType.WG_MV &&
                    !(cmd instanceof AlterResourceGroupStmt.AlterProperties)) {
                throw new DdlException("MV Resource Group not support classifiers.");
            }

            if (cmd instanceof AlterResourceGroupStmt.AddClassifiers) {
                // Build new classifiers using ResourceGroupBuilder instead of getting from stmt
                List<ResourceGroupClassifier> newAddedClassifiers;
                try {
                    newAddedClassifiers = ResourceGroupBuilder.buildAddedClassifiersFromStmt(stmt);
                } catch (SemanticException e) {
                    throw new DdlException(e.getMessage());
                }
                for (ResourceGroupClassifier classifier : newAddedClassifiers) {
                    classifier.setResourceGroupId(wg.getId());
                    classifier.setId(GlobalStateMgr.getCurrentState().getNextId());
                }
                List<ResourceGroupClassifier> classifiers = new ArrayList<>(wg.getClassifiers());
                classifiers.addAll(newAddedClassifiers);
                alterResourceGroupLog.setClassifiers(classifiers);
            } else if (cmd instanceof AlterResourceGroupStmt.AlterProperties) {
                // Build changed properties using ResourceGroupBuilder instead of getting from stmt
                ResourceGroup changedProperties;
                try {
                    changedProperties = ResourceGroupBuilder.buildChangedPropertiesFromStmt(stmt);
                } catch (SemanticException e) {
                    throw new DdlException(e.getMessage());
                }

                Integer cpuWeight = changedProperties.getRawCpuWeight();
                if (cpuWeight == null) {
                    cpuWeight = wg.getNormalizedCpuWeight();
                }
                Integer cpuWeightPercent = changedProperties.getCpuWeightPercent();
                if (cpuWeightPercent == null) {
                    cpuWeightPercent = wg.getCpuWeightPercent();
                }
                Integer exclusiveCpuCores = changedProperties.getExclusiveCpuCores();
                if (exclusiveCpuCores == null) {
                    exclusiveCpuCores = wg.getExclusiveCpuCores();
                }
                Integer exclusiveCpuPercent = changedProperties.getExclusiveCpuPercent();
                if (exclusiveCpuPercent == null) {
                    exclusiveCpuPercent = wg.getExclusiveCpuPercent();
                }
                List<String> warehouses = changedProperties.getWarehouses();
                if (warehouses == null) {
                    warehouses = wg.getWarehouses();
                }
                Integer maxCpuCores = changedProperties.getMaxCpuCores();
                if (maxCpuCores == null) {
                    maxCpuCores = wg.getMaxCpuCores();
                }
                ResourceGroup.validateCpuParameters(cpuWeight, cpuWeightPercent,
                        exclusiveCpuCores, exclusiveCpuPercent, maxCpuCores, wg.getResourceGroupType(), warehouses);
                if ((exclusiveCpuCores != null && exclusiveCpuCores > 0) ||
                        (exclusiveCpuPercent != null && exclusiveCpuPercent > 0)) {
                    validateExclusiveCpuCoresInlock(exclusiveCpuCores, exclusiveCpuPercent, warehouses, wg);
                    if (wg.getResourceGroupType() == TWorkGroupType.WG_SHORT_QUERY) {
                        throw new SemanticException(SHORT_QUERY_SET_EXCLUSIVE_CPU_CORES_ERR_MSG);
                    }
                }

                String memPool = wg.getMemPool();
                if (wg.hasDefaultMemPool()) {
                    memPool = ResourceGroup.DEFAULT_MEM_POOL;
                }
                if (changedProperties.getMemPool() != null && !changedProperties.getMemPool().equals(memPool)) {
                    throw new DdlException("Property `mem_pool` cannot be altered [" + wg.getMemPool() + "].");
                }
                if (!wg.hasDefaultMemPool() &&
                        changedProperties.getMemLimit() != null &&
                        !wg.getMemLimit().equals(changedProperties.getMemLimit())) {
                    throw new DdlException(
                            "Property `mem_limit` cannot be altered for resource groups with mem_pool [" +
                                    wg.getMemPool() + "].");
                }

                // NOTE that validate parameters should be called before setting properties.

                cpuWeightPercent = changedProperties.getCpuWeightPercent();
                if (cpuWeightPercent != null) {
                    alterResourceGroupLog.setCpuWeightPercent(cpuWeightPercent);
                }
                cpuWeight = changedProperties.getRawCpuWeight();
                if (cpuWeight != null) {
                    alterResourceGroupLog.setCpuWeight(cpuWeight);
                }
                exclusiveCpuCores = changedProperties.getExclusiveCpuCores();
                if (exclusiveCpuCores != null) {
                    alterResourceGroupLog.setExclusiveCpuCores(exclusiveCpuCores);
                }
                exclusiveCpuPercent = changedProperties.getExclusiveCpuPercent();
                if (exclusiveCpuPercent != null) {
                    alterResourceGroupLog.setExclusiveCpuPercent(exclusiveCpuPercent);
                }

                maxCpuCores = changedProperties.getMaxCpuCores();
                if (maxCpuCores != null) {
                    alterResourceGroupLog.setMaxCpuCores(maxCpuCores);
                }

                Double memLimit = changedProperties.getMemLimit();
                if (memLimit != null) {
                    alterResourceGroupLog.setMemLimit(memLimit);
                }

                Long bigQueryMemLimit = changedProperties.getBigQueryMemLimit();
                if (bigQueryMemLimit != null) {
                    alterResourceGroupLog.setBigQueryMemLimit(bigQueryMemLimit);
                }

                Long bigQueryScanRowsLimit = changedProperties.getBigQueryScanRowsLimit();
                if (bigQueryScanRowsLimit != null) {
                    alterResourceGroupLog.setBigQueryScanRowsLimit(bigQueryScanRowsLimit);
                }

                Long bigQueryCpuCoreSecondLimit = changedProperties.getBigQueryCpuSecondLimit();
                if (bigQueryCpuCoreSecondLimit != null) {
                    alterResourceGroupLog.setBigQueryCpuSecondLimit(bigQueryCpuCoreSecondLimit);
                }

                Integer concurrentLimit = changedProperties.getConcurrencyLimit();
                if (concurrentLimit != null) {
                    alterResourceGroupLog.setConcurrencyLimit(concurrentLimit);
                }

                Double spillMemLimitThreshold = changedProperties.getSpillMemLimitThreshold();
                if (spillMemLimitThreshold != null) {
                    alterResourceGroupLog.setSpillMemLimitThreshold(spillMemLimitThreshold);
                }

                Double memUsedPctLimit = changedProperties.getMemUsedPctLimit();
                if (memUsedPctLimit != null) {
                    alterResourceGroupLog.setMemUsedPctLimit(memUsedPctLimit);
                }

                warehouses = changedProperties.getWarehouses();
                if (warehouses != null) {
                    alterResourceGroupLog.setWarehouses(warehouses);
                }

                // Type is guaranteed to be immutable during the analyzer phase.
                TWorkGroupType workGroupType = changedProperties.getResourceGroupType();
                Preconditions.checkState(workGroupType == null);
            } else if (cmd instanceof AlterResourceGroupStmt.DropClassifiers dropClassifiers) {
                Set<Long> classifierToDrop = new HashSet<>(dropClassifiers.getClassifierIds());
                List<ResourceGroupClassifier> classifiers = new ArrayList<>(wg.getClassifiers());
                classifiers.removeIf(classifier -> classifierToDrop.contains(classifier.getId()));
                alterResourceGroupLog.setClassifiers(classifiers);
            } else if (cmd instanceof AlterResourceGroupStmt.DropAllClassifiers) {
                alterResourceGroupLog.setClassifiers(Collections.emptyList());
            }

            // only when changing properties, version is required to update. because changing classifiers needs not
            // propagate to BE.
            if (cmd instanceof AlterResourceGroupStmt.AlterProperties) {
                alterResourceGroupLog.setVersion(GlobalStateMgr.getCurrentState().getNextId());
            }
            // Pre-compute the altered ResourceGroup copy before logging to EditLog.
            // This ensures all object creation/deep-copying happens before the durable WAL write,
            // so the WAL applier callback only performs an infallible in-memory snapshot update.
            ResourceGroup alteredWg = applyAlterToResourceGroup(wg, alterResourceGroupLog);
            GlobalStateMgr.getCurrentState().getEditLog().logAlterResourceGroup(
                    alterResourceGroupLog, wal -> replaceResourceGroupInternal(name, alteredWg));
            resourceGroupOps.add(new ResourceGroupOpEntry(TWorkGroupOpType.WORKGROUP_OP_ALTER, alteredWg).toThrift());
        } finally {
            writeUnlock();
        }
    }

    /**
     * Applies the alter log to a deep copy of {@code wg}.
     *
     * <p>The deep copy ensures that threads holding a snapshot captured before
     * this call continue to observe the pre-alter {@link ResourceGroup} object unmodified.
     *
     * @return the new, post-alter {@link ResourceGroup}.
     */
    private ResourceGroup applyAlterToResourceGroup(ResourceGroup wg, AlterResourceGroupLog log) {
        ResourceGroup newWg = wg.copy();

        if (log.getClassifiers() != null) {
            newWg.setClassifiers(log.getClassifiers());
        }
        if (log.getCpuWeight() != null) {
            newWg.setCpuWeight(log.getCpuWeight());
            newWg.normalizeCpuWeight();
        }
        if (log.getCpuWeightPercent() != null) {
            newWg.setCpuWeightPercent(log.getCpuWeightPercent());
        }
        if (log.getExclusiveCpuCores() != null) {
            newWg.setExclusiveCpuCores(log.getExclusiveCpuCores());
        }
        if (log.getExclusiveCpuPercent() != null) {
            newWg.setExclusiveCpuPercent(log.getExclusiveCpuPercent());
        }
        if (log.getMaxCpuCores() != null) {
            newWg.setMaxCpuCores(log.getMaxCpuCores());
        }
        if (log.getMemLimit() != null) {
            newWg.setMemLimit(log.getMemLimit());
        }
        if (log.getBigQueryMemLimit() != null) {
            newWg.setBigQueryMemLimit(log.getBigQueryMemLimit());
        }
        if (log.getBigQueryScanRowsLimit() != null) {
            newWg.setBigQueryScanRowsLimit(log.getBigQueryScanRowsLimit());
        }
        if (log.getBigQueryCpuSecondLimit() != null) {
            newWg.setBigQueryCpuSecondLimit(log.getBigQueryCpuSecondLimit());
        }
        if (log.getConcurrencyLimit() != null) {
            newWg.setConcurrencyLimit(log.getConcurrencyLimit());
        }
        if (log.getSpillMemLimitThreshold() != null) {
            newWg.setSpillMemLimitThreshold(log.getSpillMemLimitThreshold());
        }
        if (log.getMemUsedPctLimit() != null) {
            newWg.setMemUsedPctLimit(log.getMemUsedPctLimit());
        }
        if (log.getWarehouses() != null) {
            newWg.setWarehouses(log.getWarehouses());
        }
        if (log.getVersion() != 0) {
            newWg.setVersion(log.getVersion());
        }

        return newWg;
    }

    public void dropResourceGroup(DropResourceGroupStmt stmt) throws DdlException {
        writeLock();
        try {
            String name = stmt.getName();
            if (!snapshot.get().byName.containsKey(name)) {
                if (!stmt.isIfExists()) {
                    throw new DdlException("RESOURCE_GROUP(" + name + ") does not exist");
                }
                return;
            }
            dropResourceGroupUnlocked(name);
        } finally {
            writeUnlock();
        }
    }

    public void dropResourceGroupUnlocked(String name) {
        ResourceGroup wg = snapshot.get().byName.get(name);
        // Deep-copy before setting version so that snapshot holders see the pre-drop version.
        ResourceGroup wgForOp = wg.copy();
        wgForOp.setVersion(GlobalStateMgr.getCurrentState().getNextId());
        ResourceGroupOpEntry workGroupOp = new ResourceGroupOpEntry(TWorkGroupOpType.WORKGROUP_OP_DELETE, wgForOp);
        GlobalStateMgr.getCurrentState().getEditLog()
                .logResourceGroupOp(workGroupOp, wal -> removeResourceGroupInternal(name));
        resourceGroupOps.add(workGroupOp.toThrift());
    }

    public void replayResourceGroupOp(ResourceGroupOpEntry entry) {
        writeLock();
        try {
            ResourceGroup workgroup = entry.getResourceGroup();
            TWorkGroupOpType opType = entry.getOpType();
            switch (opType) {
                case WORKGROUP_OP_CREATE:
                    addResourceGroupInternal(workgroup);
                    break;
                case WORKGROUP_OP_DELETE:
                    removeResourceGroupInternal(workgroup.getName());
                    break;
                case WORKGROUP_OP_ALTER:
                    // Single atomic write — no transient absence window for lock-free readers.
                    replaceResourceGroupInternal(workgroup.getName(), workgroup);
                    break;
            }
            resourceGroupOps.add(entry.toThrift());
        } finally {
            writeUnlock();
        }
    }

    public void replayAlterResourceGroup(AlterResourceGroupLog log) {
        writeLock();
        try {
            ResourceGroup wg = snapshot.get().byName.get(log.getName());
            if (wg == null) {
                return;
            }
            ResourceGroup alteredWg = applyAlterToResourceGroup(wg, log);
            replaceResourceGroupInternal(log.getName(), alteredWg);
        } finally {
            writeUnlock();
        }
    }

    @VisibleForTesting
    void removeResourceGroupInternal(String name) {
        // CopyOnWrite: build new snapshot from old then atomically publish via set().
        ResourceGroupSnapshot old = this.snapshot.get();
        ResourceGroup wg = old.byName.get(name);
        if (wg == null) {
            return;
        }
        Map<String, ResourceGroup> newByName = new HashMap<>(old.byName);
        newByName.remove(name);
        Map<Long, ResourceGroup> newById = new HashMap<>(old.byId);
        newById.remove(wg.getId());
        Map<Long, ResourceGroupClassifier> newByClassifier = new HashMap<>(old.byClassifier);
        for (ResourceGroupClassifier classifier : wg.classifiers) {
            newByClassifier.remove(classifier.getId());
        }
        ResourceGroup shortQuery = (wg.getResourceGroupType() == TWorkGroupType.WG_SHORT_QUERY)
                ? null : old.shortQueryResourceGroup;
        // Single atomic write — readers always see all indexes updated atomically.
        this.snapshot.set(new ResourceGroupSnapshot(newByName, newById, newByClassifier, shortQuery));
    }

    @VisibleForTesting
    void addResourceGroupInternal(ResourceGroup wg) {
        // CopyOnWrite: build new snapshot from old then atomically publish via set().
        ResourceGroupSnapshot old = this.snapshot.get();
        Map<String, ResourceGroup> newByName = new HashMap<>(old.byName);
        newByName.put(wg.getName(), wg);
        Map<Long, ResourceGroup> newById = new HashMap<>(old.byId);
        newById.put(wg.getId(), wg);
        Map<Long, ResourceGroupClassifier> newByClassifier = new HashMap<>(old.byClassifier);
        if (wg.getClassifiers() != null) {
            for (ResourceGroupClassifier classifier : wg.getClassifiers()) {
                newByClassifier.put(classifier.getId(), classifier);
            }
        }
        ResourceGroup shortQuery = (wg.getResourceGroupType() == TWorkGroupType.WG_SHORT_QUERY)
                ? wg : old.shortQueryResourceGroup;
        // Single atomic write — readers always see all indexes updated atomically.
        this.snapshot.set(new ResourceGroupSnapshot(newByName, newById, newByClassifier, shortQuery));
        if (ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME.equals(wg.getName())) {
            hasCreatedDefaultResourceGroups = true;
        }
    }


    /**
     * Atomically replaces {@code oldName} with {@code newWg} in the snapshot using a single volatile write.
     * Builds all three indexes in local maps (single pass: remove old entries, add new entries) before
     * publishing, so lock-free readers never observe a transient window where the group is absent.
     *
     * <p>Must be called under {@link #writeLock()}.
     */
    @VisibleForTesting
    void replaceResourceGroupInternal(String oldName, ResourceGroup newWg) {
        ResourceGroupSnapshot old = this.snapshot.get();
        ResourceGroup oldWg = old.byName.get(oldName);

        // Single pass: build all three local maps before any volatile write.
        Map<String, ResourceGroup>         newByName       = new HashMap<>(old.byName);
        Map<Long, ResourceGroup>           newById         = new HashMap<>(old.byId);
        Map<Long, ResourceGroupClassifier> newByClassifier = new HashMap<>(old.byClassifier);

        // Remove old entries (no-op if the group did not exist under oldName).
        if (oldWg != null) {
            newByName.remove(oldName);
            newById.remove(oldWg.getId());
            if (oldWg.getClassifiers() != null) {
                for (ResourceGroupClassifier c : oldWg.getClassifiers()) {
                    newByClassifier.remove(c.getId());
                }
            }
        }

        // Add new entries.
        newByName.put(newWg.getName(), newWg);
        newById.put(newWg.getId(), newWg);
        if (newWg.getClassifiers() != null) {
            for (ResourceGroupClassifier c : newWg.getClassifiers()) {
                newByClassifier.put(c.getId(), c);
            }
        }

        // Determine the short_query group for the new snapshot.
        ResourceGroup shortQuery;
        if (newWg.getResourceGroupType() == TWorkGroupType.WG_SHORT_QUERY) {
            shortQuery = newWg;
        } else if (old.shortQueryResourceGroup != null && old.shortQueryResourceGroup.getName().equals(oldName)) {
            shortQuery = null;
        } else {
            shortQuery = old.shortQueryResourceGroup;
        }

        // Single atomic write — constructor wraps maps with Collections.unmodifiableMap.
        this.snapshot.set(new ResourceGroupSnapshot(newByName, newById, newByClassifier, shortQuery));
    }

    /**
     * If a resource group is bound to specific warehouses, and the warehouse that the current BE belongs to is not among those
     * bound warehouses, then the TWorkGroupOp sent to that BE will be marked as inactive, meaning this resource group will not
     * take effect on that BE.
     *
     * <p> We separate the logic of pushing resource groups to BEs from the logic of deciding whether they should be inactive,
     * to avoid complicated handling when the set of warehouses bound to a resource group changes.
     *
     * @param op            the resource group operation will be sent to this BE
     * @param warehouseName the warehouse name that the current BE belongs to
     * @return the TWorkGroupOp that may be marked as inactive
     */
    private TWorkGroupOp setInactiveOp(TWorkGroupOp op, String warehouseName) {
        if (ResourceGroup.BUILTIN_WG_NAMES.contains(op.getWorkgroup().getName())) {
            return op;
        }
        List<String> warehouses = op.getWorkgroup().getWarehouses();
        if (warehouseName == null || warehouses == null || warehouses.isEmpty() || warehouses.contains(warehouseName)) {
            return op;
        }

        // Only when we need to set `inactive` do we create a copied instance.
        // In all other cases, all BEs share the same TWorkGroupOp instance.
        TWorkGroupOp newOp = op.deepCopy();
        newOp.getWorkgroup().setInactive(true);
        return newOp;
    }

    public List<TWorkGroupOp> getResourceGroupsNeedToDeliver(Long beId) {
        ComputeNode computeNode = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(beId);
        String warehouseName = null;
        if (computeNode != null) {
            Warehouse wh = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(computeNode.getWarehouseId());
            if (wh != null) {
                warehouseName = wh.getName();
            }
        }

        // resourceGroupOps and activeResourceGroupsPerBe are not on the classification hot path
        // and remain protected by the read lock for consistent iteration.
        readLock();
        try {
            List<TWorkGroupOp> currentResourceGroupOps = new ArrayList<>();
            if (!activeResourceGroupsPerBe.containsKey(beId)) {
                for (TWorkGroupOp op : resourceGroupOps) {
                    currentResourceGroupOps.add(setInactiveOp(op, warehouseName));
                }
                return currentResourceGroupOps;
            }

            Long minVersion = minVersionPerBe.get(beId);
            Map<Long, TWorkGroup> activeResourceGroup = activeResourceGroupsPerBe.get(beId);
            // Use AtomicReference snapshot to avoid holding both lock types simultaneously.
            Map<Long, ResourceGroup> idSnapshot = this.snapshot.get().byId;
            for (TWorkGroupOp op : resourceGroupOps) {
                TWorkGroup twg = op.getWorkgroup();
                if (twg.getVersion() < minVersion) {
                    continue;
                }

                boolean active = activeResourceGroup.containsKey(twg.getId());
                if ((!active && idSnapshot.containsKey(twg.getId())) ||
                        (active && twg.getVersion() > activeResourceGroup.get(twg.getId()).getVersion())) {
                    currentResourceGroupOps.add(setInactiveOp(op, warehouseName));
                }
            }

            return currentResourceGroupOps;
        } finally {
            readUnlock();
        }
    }

    public void saveActiveResourceGroupsForBe(Long beId, List<TWorkGroup> workGroups) {
        writeLock();
        try {
            Map<Long, TWorkGroup> workGroupOnBe = new HashMap<>();
            Long minVersion = Long.MAX_VALUE;
            for (TWorkGroup workgroup : workGroups) {
                workGroupOnBe.put(workgroup.getId(), workgroup);
                if (workgroup.getVersion() < minVersion) {
                    minVersion = workgroup.getVersion();
                }
            }
            activeResourceGroupsPerBe.put(beId, workGroupOnBe);
            minVersionPerBe.put(beId, minVersion == Long.MAX_VALUE ? Long.MIN_VALUE : minVersion);
        } finally {
            writeUnlock();
        }
    }

    private boolean isResourceGroupMatchWarehouse(ResourceGroup rg, String warehouseName) {
        if (ResourceGroup.BUILTIN_WG_NAMES.contains(rg.getName())) {
            return true;
        }
        List<String> warehouses = rg.getWarehouses();
        return warehouses == null || warehouses.isEmpty() || warehouses.contains(warehouseName);
    }

    public TWorkGroup chooseResourceGroupByName(ConnectContext ctx, String wgName) {
        // Lock-free: single snapshot read — consistent with byId/byClassifier.
        ResourceGroup rg = snapshot.get().byName.get(wgName);
        if (rg == null) {
            return null;
        }
        if (ctx != null && !isResourceGroupMatchWarehouse(rg, ctx.getCurrentWarehouseName())) {
            return null;
        }
        return rg.toThrift();
    }

    public TWorkGroup chooseResourceGroupByID(ConnectContext ctx, long wgID) {
        // Lock-free: single snapshot read — consistent with byName/byClassifier.
        ResourceGroup rg = snapshot.get().byId.get(wgID);
        if (rg == null) {
            return null;
        }
        if (ctx != null && !isResourceGroupMatchWarehouse(rg, ctx.getCurrentWarehouseName())) {
            return null;
        }
        return rg.toThrift();
    }

    public TWorkGroup chooseResourceGroup(ConnectContext ctx, ResourceGroupClassifier.QueryType queryType, Set<Long> databases) {
        List<String> activeRoles = getUnqualifiedRole(ctx);

        // CopyOnWrite: capture the snapshot holder once — a single get()
        // guarantees all three indexes (byName, byId, byClassifier) are mutually consistent.
        // Writers publish them atomically by replacing the single ResourceGroupSnapshot reference.
        ResourceGroupSnapshot snap = this.snapshot.get();
        ResourceGroup sqrg        = snap.shortQueryResourceGroup;

        Map<String, ResourceGroup>         rgSnapshot  = snap.byName;
        Map<Long, ResourceGroup>           idSnapshot  = snap.byId;
        Map<Long, ResourceGroupClassifier> clsSnapshot = snap.byClassifier;

        String user          = getUnqualifiedUser(ctx);
        String remoteIp      = ctx.getRemoteIP();
        String warehouseName = ctx.getCurrentWarehouseName();
        final double planCpuCost = ctx.getAuditEventBuilder().build().planCpuCosts;
        final double planMemCost = CostPredictor.getServiceBasedCostPredictor().isAvailable() ?
                ctx.getAuditEventBuilder().build().predictMemBytes :
                ctx.getAuditEventBuilder().build().planMemCosts;

        // Build candidate group IDs with a plain loop — avoids Stream/lambda allocation on the
        // query hot path (called on every incoming query).
        Set<Long> candidateGroupIds = new HashSet<>();
        for (ResourceGroup rg : rgSnapshot.values()) {
            if (isResourceGroupMatchWarehouse(rg, warehouseName)) {
                candidateGroupIds.add(rg.getId());
            }
        }

        // Check short_query group first.
        if (sqrg != null) {
            List<ResourceGroupClassifier> shortQueryClassifierList = sqrg.classifiers.stream()
                    .filter(f -> f.isSatisfied(candidateGroupIds, user, activeRoles, queryType, remoteIp,
                            databases, planCpuCost, planMemCost))
                    .sorted(Comparator.comparingDouble(ResourceGroupClassifier::weight))
                    .collect(Collectors.toList());
            if (!shortQueryClassifierList.isEmpty()) {
                return sqrg.toThrift();
            }
        }

        List<ResourceGroupClassifier> classifierList =
                clsSnapshot.values().stream()
                        .filter(f -> f.isSatisfied(candidateGroupIds, user, activeRoles, queryType,
                                remoteIp, databases, planCpuCost, planMemCost))
                        .sorted(Comparator.comparingDouble(ResourceGroupClassifier::weight))
                        .collect(Collectors.toList());
        if (classifierList.isEmpty()) {
            return null;
        } else {
            ResourceGroup rg =
                    idSnapshot.get(classifierList.get(classifierList.size() - 1).getResourceGroupId());
            if (rg == null) {
                return null;
            }
            return rg.toThrift();
        }
    }

    public void createBuiltinResourceGroupsIfNotExist() {
        try {
            if (hasCreatedDefaultResourceGroups) {
                return;
            }

            // Create default resource groups only when there are BEs.
            // Otherwise, we cannot get the number of cores of BE as `cpu_weight`.
            if (BackendResourceStat.getInstance().getNumBes(DEFAULT_WAREHOUSE_ID) <= 0) {
                return;
            }

            ResourceGroup defaultWg = getResourceGroup(ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME);
            if (defaultWg != null) {
                return;
            }

            Map<String, String> defaultWgProperties = ImmutableMap.of(
                    ResourceGroup.CPU_WEIGHT_PERCENT, "100",
                    ResourceGroup.MEM_LIMIT, "1.0"
            );
            CreateResourceGroupStmt defaultWgStmt = new CreateResourceGroupStmt(ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME,
                    true, false, Collections.emptyList(), defaultWgProperties);
            ResourceGroupAnalyzer.analyzeCreateResourceGroupStmt(defaultWgStmt);
            createResourceGroup(defaultWgStmt);

            Map<String, String> defaultMvWgProperties = ImmutableMap.of(
                    ResourceGroup.CPU_WEIGHT_PERCENT, "1",
                    ResourceGroup.MEM_LIMIT, "0.8",
                    ResourceGroup.SPILL_MEM_LIMIT_THRESHOLD, "0.8"
            );
            CreateResourceGroupStmt defaultMvWgStmt = new CreateResourceGroupStmt(ResourceGroup.DEFAULT_MV_RESOURCE_GROUP_NAME,
                    true, false, Collections.emptyList(), defaultMvWgProperties);
            ResourceGroupAnalyzer.analyzeCreateResourceGroupStmt(defaultMvWgStmt);
            createResourceGroup(defaultMvWgStmt);
        } catch (Exception e) {
            LOG.warn("failed to create builtin resource groups", e);
        }
    }

    private static class SerializeData {
        @SerializedName("WorkGroups")
        public List<ResourceGroup> resourceGroups;
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        Map<String, ResourceGroup> byName = snapshot.get().byName;
        int numJson = 1 + byName.size();
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.RESOURCE_GROUP_MGR, numJson);
        writer.writeInt(byName.size());
        for (ResourceGroup resourceGroup : byName.values()) {
            writer.writeJson(resourceGroup);
        }

        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        List<ResourceGroup> resourceGroups = new ArrayList<>();
        reader.readCollection(ResourceGroup.class, resourceGroups::add);
        resourceGroups.sort(Comparator.comparing(ResourceGroup::getVersion));
        resourceGroups.forEach(this::replayAddResourceGroup);
    }
}
