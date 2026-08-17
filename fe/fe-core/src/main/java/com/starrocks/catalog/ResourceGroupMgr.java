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
import com.starrocks.persist.gson.GsonUtils;
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
import java.util.Set;
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
    // A single volatile write of this object provides an atomic, consistent view
    // of every index — eliminating the window where three separate volatile writes
    // could be observed in a torn order by a lock-free reader (Issue 2).
    // ---------------------------------------------------------------------------
    private static final class ResourceGroupSnapshot {
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

    // Single volatile field: writers replace the entire holder; readers capture
    // the reference once and access all three maps without any lock.
    // Because ResourceGroup objects are deep-copied on every alter (Issue 1 fix),
    // holders of an older snapshot always see a consistent pre-alter view.
    private volatile ResourceGroupSnapshot snapshot = ResourceGroupSnapshot.EMPTY;


    private final List<TWorkGroupOp> resourceGroupOps = new ArrayList<>();
    private final Map<Long, Map<Long, TWorkGroup>> activeResourceGroupsPerBe = new HashMap<>();
    private final Map<Long, Long> minVersionPerBe = new HashMap<>();
    // Write lock provides mutual exclusion for DDL (create/alter/drop) operations only.
    // Read operations (chooseResourceGroup, getResourceGroup, etc.) are lock-free.
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile boolean hasCreatedDefaultResourceGroups = false;


    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
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
            if (snapshot.byName.containsKey(wg.getName())) {
                if (stmt.isReplaceIfExists()) {
                    needReplace = true;
                } else if (!stmt.isIfNotExists()) {
                    throw new DdlException(String.format("RESOURCE_GROUP(%s) already exists", wg.getName()));
                } else {
                    return;
                }
            }

            ResourceGroup sqrg = snapshot.shortQueryResourceGroup;
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

            if (needReplace) {
                // Log a DELETE WAL entry so BEs and journal-replay followers learn the old version
                // is gone, but use a no-op snapshot callback — the snapshot update will be done
                // atomically together with the CREATE entry below (single volatile write).
                ResourceGroup oldWg = snapshot.byName.get(wg.getName());
                ResourceGroup oldWgForOp =
                        GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(oldWg), ResourceGroup.class);
                oldWgForOp.setVersion(GlobalStateMgr.getCurrentState().getNextId());
                ResourceGroupOpEntry deleteOp =
                        new ResourceGroupOpEntry(TWorkGroupOpType.WORKGROUP_OP_DELETE, oldWgForOp);
                GlobalStateMgr.getCurrentState().getEditLog()
                        .logResourceGroupOp(deleteOp, wal -> { /* snapshot updated atomically below */ });
                resourceGroupOps.add(deleteOp.toThrift());
            }

            wg.normalizeCpuWeight();

            if (ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME.equals(wg.getName())) {
                wg.setId(ResourceGroup.DEFAULT_WG_ID);
            } else if (ResourceGroup.DEFAULT_MV_RESOURCE_GROUP_NAME.equals(wg.getName())) {
                wg.setId(ResourceGroup.DEFAULT_MV_WG_ID);
            } else {
                wg.setId(GlobalStateMgr.getCurrentState().getNextId());
            }

            wg.setVersion(wg.getId());
            for (ResourceGroupClassifier classifier : wg.getClassifiers()) {
                classifier.setResourceGroupId(wg.getId());
                classifier.setId(GlobalStateMgr.getCurrentState().getNextId());
            }

            if (!wg.hasDefaultMemPool() && !resourceGroupInMemPoolHaveSameMemLimit(wg)) {
                throw new DdlException(
                        "Property `mem_limit` must be equal for all resource groups using the mem_pool [" +
                                wg.getMemPool() + "].");
            }

            ResourceGroupOpEntry workGroupOp = new ResourceGroupOpEntry(TWorkGroupOpType.WORKGROUP_OP_CREATE, wg);
            final boolean replacing = needReplace;
            final String replacedName = wg.getName();
            GlobalStateMgr.getCurrentState().getEditLog().logResourceGroupOp(workGroupOp, wal -> {
                if (replacing) {
                    // Single volatile write: atomically removes old entry and adds new one.
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
        if (stmt.getName() != null && !snapshot.byName.containsKey(stmt.getName())) {
            ErrorReport.reportSemanticException(ErrorCode.ERROR_NO_RG_ERROR, stmt.getName());
        }

        List<List<String>> rows;
        if (stmt.getName() != null) {
            rows = showOneResourceGroup(stmt.getName(), stmt.isVerbose());
        } else {
            rows = showAllResourceGroups(ConnectContext.get(), stmt.isVerbose(), stmt.isListAll());
        }
        return rows;
    }

    public List<Long> getResourceGroupIds() {
        // Lock-free: capture volatile snapshot once.
        return new ArrayList<>(snapshot.byId.keySet());
    }

    private boolean resourceGroupInMemPoolHaveSameMemLimit(ResourceGroup wg) {
        if (wg.hasDefaultMemPool()) {
            return true;
        }
        return snapshot.byName.entrySet().stream().allMatch(entry -> !wg.getMemPool().equals(entry.getValue().getMemPool()) ||
                wg.getMemLimit().equals(entry.getValue().getMemLimit()));
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
        // Lock-free: capture volatile snapshot once — all three indexes are consistent.
        List<ResourceGroup> resourceGroupList = new ArrayList<>(snapshot.byName.values());
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
        // Lock-free: capture volatile snapshot once.
        Map<String, ResourceGroup> snap = this.snapshot.byName;
        if (!snap.containsKey(name)) {
            return Collections.emptyList();
        } else {
            return snap.get(name).show(verbose);
        }
    }

    public Set<String> getAllResourceGroupNames() {
        // Lock-free: defensive copy of volatile snapshot keyset.
        return new HashSet<>(snapshot.byName.keySet());
    }

    private void replayAddResourceGroup(ResourceGroup workgroup) {
        addResourceGroupInternal(workgroup);
        ResourceGroupOpEntry op = new ResourceGroupOpEntry(TWorkGroupOpType.WORKGROUP_OP_CREATE, workgroup);
        resourceGroupOps.add(op.toThrift());
    }

    public ResourceGroup getResourceGroup(String name) {
        // Lock-free: volatile read gives a consistent snapshot.
        return snapshot.byName.getOrDefault(name, null);
    }

    public ResourceGroup getResourceGroup(long id) {
        // Lock-free: volatile read gives a consistent snapshot.
        return snapshot.byId.getOrDefault(id, null);
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

        List<ResourceGroup> groups = new ArrayList<>(snapshot.byName.values());
        if (!snapshot.byName.containsKey(wg.getName())) {
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
            if (!snapshot.byName.containsKey(name)) {
                throw new DdlException("RESOURCE_GROUP(" + name + ") does not exist");
            }
            ResourceGroup wg = snapshot.byName.get(name);
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
            // updateResourceGroup returns the new ResourceGroup (deep copy with mutations applied).
            // Capture it via an array to cross the lambda boundary, then use it for resourceGroupOps
            // so BEs receive the post-alter state rather than the pre-alter state.
            ResourceGroup[] newWgHolder = new ResourceGroup[] {wg};
            GlobalStateMgr.getCurrentState().getEditLog().logAlterResourceGroup(
                    alterResourceGroupLog, wal -> newWgHolder[0] = updateResourceGroup(wg, alterResourceGroupLog));
            resourceGroupOps.add(new ResourceGroupOpEntry(TWorkGroupOpType.WORKGROUP_OP_ALTER, newWgHolder[0]).toThrift());
        } finally {
            writeUnlock();
        }
    }

    /**
     * Applies the alter log to a deep copy of {@code wg} and atomically publishes the result
     * via {@link #removeResourceGroupInternal}/{@link #addResourceGroupInternal}.
     *
     * <p>The deep copy (Issue 1 fix) ensures that threads holding a snapshot captured before
     * this call continue to observe the pre-alter {@link ResourceGroup} object unmodified.
     *
     * @return the new, post-alter {@link ResourceGroup} that was inserted into the snapshot.
     */
    private ResourceGroup updateResourceGroup(ResourceGroup wg, AlterResourceGroupLog log) {
        // GSON round-trip deep-copy: mutations go to newWg only.
        // Old-snapshot holders retain a reference to the original, unmodified wg object.
        ResourceGroup newWg = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(wg), ResourceGroup.class);

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
        if (log.getWarehouses() != null) {
            newWg.setWarehouses(log.getWarehouses());
        }
        if (log.getVersion() != 0) {
            newWg.setVersion(log.getVersion());
        }

        // Single volatile write: atomically removes old indexing and inserts new.
        // Lock-free readers never observe a transient window where the group is absent.
        replaceResourceGroupInternal(wg.getName(), newWg);
        return newWg;
    }

    public void dropResourceGroup(DropResourceGroupStmt stmt) throws DdlException {
        writeLock();
        try {
            String name = stmt.getName();
            if (!snapshot.byName.containsKey(name)) {
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
        ResourceGroup wg = snapshot.byName.get(name);
        // Deep-copy before setting version so that snapshot holders see the pre-drop version.
        ResourceGroup wgForOp = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(wg), ResourceGroup.class);
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
                    // Single volatile write — no transient absence window for lock-free readers.
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
            ResourceGroup wg = snapshot.byName.get(log.getName());
            if (wg == null) {
                return;
            }
            updateResourceGroup(wg, log); // return value intentionally ignored for replay
        } finally {
            writeUnlock();
        }
    }

    private void removeResourceGroupInternal(String name) {
        // CopyOnWrite: build new snapshot from old then atomically publish via single volatile write.
        ResourceGroupSnapshot old = this.snapshot;
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
        // Single volatile write — readers always see all indexes updated atomically.
        this.snapshot = new ResourceGroupSnapshot(newByName, newById, newByClassifier, shortQuery);
    }

    private void addResourceGroupInternal(ResourceGroup wg) {
        // CopyOnWrite: build new snapshot from old then atomically publish via single volatile write.
        ResourceGroupSnapshot old = this.snapshot;
        Map<String, ResourceGroup> newByName = new HashMap<>(old.byName);
        newByName.put(wg.getName(), wg);
        Map<Long, ResourceGroup> newById = new HashMap<>(old.byId);
        newById.put(wg.getId(), wg);
        Map<Long, ResourceGroupClassifier> newByClassifier = new HashMap<>(old.byClassifier);
        for (ResourceGroupClassifier classifier : wg.classifiers) {
            newByClassifier.put(classifier.getId(), classifier);
        }
        ResourceGroup shortQuery = (wg.getResourceGroupType() == TWorkGroupType.WG_SHORT_QUERY)
                ? wg : old.shortQueryResourceGroup;
        // Single volatile write — readers always see all indexes updated atomically.
        this.snapshot = new ResourceGroupSnapshot(newByName, newById, newByClassifier, shortQuery);
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
    private void replaceResourceGroupInternal(String oldName, ResourceGroup newWg) {
        ResourceGroupSnapshot old = this.snapshot;
        ResourceGroup oldWg = old.byName.get(oldName);

        // Single pass: build all three local maps before any volatile write.
        Map<String, ResourceGroup>         newByName       = new HashMap<>(old.byName);
        Map<Long, ResourceGroup>           newById         = new HashMap<>(old.byId);
        Map<Long, ResourceGroupClassifier> newByClassifier = new HashMap<>(old.byClassifier);

        // Remove old entries (no-op if the group did not exist under oldName).
        if (oldWg != null) {
            newByName.remove(oldName);
            newById.remove(oldWg.getId());
            for (ResourceGroupClassifier c : oldWg.classifiers) {
                newByClassifier.remove(c.getId());
            }
        }

        // Add new entries.
        newByName.put(newWg.getName(), newWg);
        newById.put(newWg.getId(), newWg);
        for (ResourceGroupClassifier c : newWg.classifiers) {
            newByClassifier.put(c.getId(), c);
        }

        // Determine the short_query group for the new snapshot.
        ResourceGroup shortQuery;
        if (newWg.getResourceGroupType() == TWorkGroupType.WG_SHORT_QUERY) {
            shortQuery = newWg;
        } else if (oldWg != null && oldWg.getResourceGroupType() == TWorkGroupType.WG_SHORT_QUERY) {
            shortQuery = null;
        } else {
            shortQuery = old.shortQueryResourceGroup;
        }

        // Single volatile write — constructor wraps maps with Collections.unmodifiableMap.
        this.snapshot = new ResourceGroupSnapshot(newByName, newById, newByClassifier, shortQuery);
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
            // Use volatile snapshot to avoid holding both lock types simultaneously.
            Map<Long, ResourceGroup> idSnapshot = this.snapshot.byId;
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
        // Lock-free: single volatile snapshot read — consistent with byId/byClassifier.
        ResourceGroup rg = snapshot.byName.get(wgName);
        if (rg == null) {
            return null;
        }
        if (ctx != null && !isResourceGroupMatchWarehouse(rg, ctx.getCurrentWarehouseName())) {
            return null;
        }
        return rg.toThrift();
    }

    public TWorkGroup chooseResourceGroupByID(ConnectContext ctx, long wgID) {
        // Lock-free: single volatile snapshot read — consistent with byName/byClassifier.
        ResourceGroup rg = snapshot.byId.get(wgID);
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

        // CopyOnWrite: capture the volatile snapshot holder once — a single volatile read
        // guarantees all three indexes (byName, byId, byClassifier) are mutually consistent.
        // Writers publish them atomically by replacing the single ResourceGroupSnapshot field.
        ResourceGroupSnapshot snap = this.snapshot;
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
        Map<String, ResourceGroup> byName = snapshot.byName;
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
