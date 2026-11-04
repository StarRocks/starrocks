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

package com.starrocks.qe.scheduler;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.planner.ScanNode;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Best-effort worker restriction for queries that touch external catalogs with {@code labels.location}.
 * <ul>
 *   <li>L = label-matched backends that are schedulable on the captured {@link WorkerProvider}
 *       (alive and not excluded by blacklist / provider filtering)</li>
 *   <li>N = backends required by fixed-location non-connector scans (OlapScan replicas,
 *       MetaScan, BE-local SchemaScan, Kudu, etc.)</li>
 *   <li>If no connector location is set, or L is empty, scheduling stays unrestricted</li>
 *   <li>Otherwise allowed workers = L ∪ N; connector scans prefer L only</li>
 * </ul>
 */
public final class ConnectorLocationWorkerRestrictor {
    private static final Logger LOG = LogManager.getLogger(ConnectorLocationWorkerRestrictor.class);

    private ConnectorLocationWorkerRestrictor() {
    }

    public static final class Restriction {
        private final boolean restricted;
        private final Set<Long> preferredConnectorWorkerIds;
        private final Set<Long> allowedWorkerIds;

        private Restriction(boolean restricted, Set<Long> preferredConnectorWorkerIds, Set<Long> allowedWorkerIds) {
            this.restricted = restricted;
            this.preferredConnectorWorkerIds = preferredConnectorWorkerIds;
            this.allowedWorkerIds = allowedWorkerIds;
        }

        public static Restriction unrestricted() {
            return new Restriction(false, Collections.emptySet(), Collections.emptySet());
        }

        public static Restriction of(Set<Long> preferredConnectorWorkerIds, Set<Long> allowedWorkerIds) {
            return new Restriction(true, preferredConnectorWorkerIds, allowedWorkerIds);
        }

        public boolean isRestricted() {
            return restricted;
        }

        public Set<Long> getPreferredConnectorWorkerIds() {
            return preferredConnectorWorkerIds;
        }

        public Set<Long> getAllowedWorkerIds() {
            return allowedWorkerIds;
        }
    }

    public static Restriction resolve(List<ScanNode> scanNodes, SystemInfoService clusterInfo) {
        return resolve(scanNodes, clusterInfo, null, null);
    }

    public static Restriction resolve(List<ScanNode> scanNodes, SystemInfoService clusterInfo,
                                      WorkerProvider workerProvider) {
        return resolve(scanNodes, clusterInfo, null, workerProvider);
    }

    /**
     * @param catalogMgr optional; when null, resolved lazily from {@link GlobalStateMgr} only if needed
     */
    static Restriction resolve(List<ScanNode> scanNodes, SystemInfoService clusterInfo, CatalogMgr catalogMgr) {
        return resolve(scanNodes, clusterInfo, catalogMgr, null);
    }

    /**
     * @param catalogMgr     optional; when null, resolved lazily from {@link GlobalStateMgr} only if needed
     * @param workerProvider optional captured provider; when non-null, L is intersected with workers
     *                       visible on it (excludes blacklisted / otherwise unschedulable nodes)
     */
    public static Restriction resolve(List<ScanNode> scanNodes, SystemInfoService clusterInfo, CatalogMgr catalogMgr,
                                      WorkerProvider workerProvider) {
        if (!Config.enable_connector_location_worker_restriction) {
            return Restriction.unrestricted();
        }
        if (scanNodes == null || scanNodes.isEmpty()) {
            return Restriction.unrestricted();
        }

        Multimap<String, String> location = collectConnectorCatalogLocations(scanNodes, catalogMgr);
        if (location.isEmpty()) {
            return Restriction.unrestricted();
        }

        Set<Long> labelled = Sets.newHashSet(clusterInfo.getLocationMatchedBackendIds(location));
        Set<Long> preferred = Sets.newHashSet();
        for (Long id : labelled) {
            if (workerProvider != null) {
                // Provider view is authoritative for scheduling (alive + not blacklisted).
                if (workerProvider.getWorkerById(id) != null) {
                    preferred.add(id);
                }
            } else if (clusterInfo.getBackend(id) != null && clusterInfo.getBackend(id).isAlive()) {
                preferred.add(id);
            }
        }

        if (preferred.isEmpty()) {
            LOG.warn("Connector catalog labels.location={} matched no schedulable backends; " +
                    "falling back to unrestricted worker scheduling", location);
            return Restriction.unrestricted();
        }

        Set<Long> fixedRequired = computeFixedLocationRequiredBackendIds(scanNodes);
        Set<Long> allowed = Sets.newHashSet(preferred);
        allowed.addAll(fixedRequired);
        return Restriction.of(Collections.unmodifiableSet(preferred), Collections.unmodifiableSet(allowed));
    }

    static Multimap<String, String> collectConnectorCatalogLocations(List<ScanNode> scanNodes,
                                                                     CatalogMgr catalogMgr) {
        Multimap<String, String> location = HashMultimap.create();
        CatalogMgr resolvedCatalogMgr = catalogMgr;
        for (ScanNode scanNode : scanNodes) {
            if (!scanNode.isConnectorScanNode()) {
                continue;
            }
            Table table = scanNode.getDesc() != null ? scanNode.getDesc().getTable() : null;
            if (table == null) {
                continue;
            }
            String catalogName = table.getCatalogName();
            if (!CatalogMgr.isExternalCatalog(catalogName)) {
                continue;
            }
            if (resolvedCatalogMgr == null) {
                resolvedCatalogMgr = GlobalStateMgr.getCurrentState().getCatalogMgr();
            }
            Catalog catalog = resolvedCatalogMgr.getCatalogByName(catalogName);
            if (catalog == null || catalog.getConfig() == null) {
                continue;
            }
            Map<String, String> config = catalog.getConfig();
            String loc = config.get(PropertyAnalyzer.PROPERTIES_LABELS_LOCATION);
            if (StringUtils.isBlank(loc)) {
                continue;
            }
            try {
                location.putAll(PropertyAnalyzer.analyzeLocationStringToMap(loc));
            } catch (Exception e) {
                LOG.warn("Ignore invalid labels.location '{}' on catalog {}: {}", loc, catalogName, e.getMessage());
            }
        }
        return location;
    }

    /**
     * Collect worker IDs that fixed-location scans must keep available under L∪N.
     * Skips connector scans (assigned via preferred L). Includes OlapScan, MetaScan,
     * SchemaScan, Kudu, and any other non-connector scan that pins {@code backend_id}.
     */
    static Set<Long> computeFixedLocationRequiredBackendIds(List<ScanNode> scanNodes) {
        Set<Long> backendIds = Sets.newHashSet();
        for (ScanNode scanNode : scanNodes) {
            if (!scanNode.isConnectorScanNode()) {
                List<TScanRangeLocations> locations = scanNode.getScanRangeLocations(0);
                if (locations != null) {
                    for (TScanRangeLocations rangeLocations : locations) {
                        if (rangeLocations.getLocations() != null) {
                            for (TScanRangeLocation location : rangeLocations.getLocations()) {
                                if (location.isSetBackend_id()) {
                                    backendIds.add(location.getBackend_id());
                                }
                            }
                        }
                    }
                }
            }
        }
        return backendIds;
    }
}
