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

package com.starrocks.http.rest.v2.vo;

import com.google.gson.annotations.SerializedName;
import com.staros.client.StarClientException;
import com.staros.proto.ShardInfo;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.load.PartitionUtils;
import com.starrocks.load.PartitionUtils.RangePartitionBoundary;
import com.starrocks.server.GlobalStateMgr;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class PartitionInfoView {

    @SerializedName("type")
    private String type;

    @SerializedName("partitionColumns")
    private List<ColumnView> partitionColumns;

    public PartitionInfoView() {
    }

    /**
     * Create from {@link PartitionInfo}
     */
    public static PartitionInfoView createFrom(Table table, PartitionInfo partitionInfo) {
        PartitionInfoView pvo = new PartitionInfoView();
        pvo.setType(partitionInfo.getType().typeString);
        if (!partitionInfo.isPartitioned()) {
            return pvo;
        }

        Optional.of(partitionInfo.getPartitionColumns(table.getIdToColumn()))
                .map(columns -> columns.stream()
                        .filter(Objects::nonNull)
                        .map(ColumnView::createFrom)
                        .collect(Collectors.toList()))
                .ifPresent(pvo::setPartitionColumns);

        return pvo;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<ColumnView> getPartitionColumns() {
        return partitionColumns;
    }

    public void setPartitionColumns(List<ColumnView> partitionColumns) {
        this.partitionColumns = partitionColumns;
    }

    public static class PartitionView {

        @SerializedName("id")
        private Long id;

        @SerializedName("name")
        private String name;

        @SerializedName("bucketNum")
        private Integer bucketNum;

        @SerializedName("distributionType")
        private String distributionType;

        @SerializedName("visibleVersion")
        private Long visibleVersion;

        @SerializedName("visibleVersionTime")
        private Long visibleVersionTime;

        @SerializedName("nextVersion")
        private Long nextVersion;

        @SerializedName("isMinPartition")
        private Boolean isMinPartition;

        @SerializedName("isMaxPartition")
        private Boolean isMaxPartition;

        @SerializedName("startKeys")
        private List<Object> startKeys;

        @SerializedName("endKeys")
        private List<Object> endKeys;

        @SerializedName("inKeys")
        private List<List<Object>> inKeys;

        @SerializedName("storagePath")
        private String storagePath;

        @SerializedName("tablets")
        private List<TabletView> tablets;

        public PartitionView() {
        }

        /**
         * Create from {@link Partition}
         */
        public static PartitionView createFrom(PartitionInfo partitionInfo, Partition partition) {
            PartitionView pvo = new PartitionView();
            long partitionId = partition.getId();
            pvo.setId(partitionId);
            pvo.setName(partition.getName());

            Optional.ofNullable(partition.getDistributionInfo()).ifPresent(distributionInfo -> {
                pvo.setBucketNum(distributionInfo.getBucketNum());
                pvo.setDistributionType(distributionInfo.getTypeStr());
            });

            PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();

            pvo.setVisibleVersion(physicalPartition.getVisibleVersion());
            pvo.setVisibleVersionTime(physicalPartition.getVisibleVersionTime());
            pvo.setNextVersion(physicalPartition.getNextVersion());

            PartitionType partitionType = partitionInfo.getType();
            switch (partitionType) {
                case UNPARTITIONED:
                    pvo.setMinPartition(true);
                    pvo.setMaxPartition(true);
                    pvo.setStartKeys(new ArrayList<>(0));
                    pvo.setEndKeys(new ArrayList<>(0));
                    break;
                case RANGE:
                    RangePartitionInfo rpi = (RangePartitionInfo) partitionInfo;
                    RangePartitionBoundary boundary =
                            PartitionUtils.calRangePartitionBoundary(rpi.getRange(partitionId));
                    pvo.setMinPartition(boundary.isMinPartition());
                    pvo.setMaxPartition(boundary.isMaxPartition());
                    pvo.setStartKeys(boundary.getStartKeys());
                    pvo.setEndKeys(boundary.getEndKeys());
                    break;
                case LIST:
                    ListPartitionInfo lpi = (ListPartitionInfo) partitionInfo;
                    List<List<Object>> keys = PartitionUtils.calListPartitionKeys(
                            Optional.ofNullable(lpi.getMultiLiteralExprValues())
                                    .map(exprVals -> exprVals.get(partitionId)).orElse(new ArrayList<>(0)),
                            Optional.ofNullable(lpi.getLiteralExprValues())
                                    .map(exprVals -> exprVals.get(partitionId)).orElse(new ArrayList<>(0))
                    );
                    pvo.setInKeys(keys);
                    break;
                default:
                    // TODO add more type support in the future
            }

            List<MaterializedIndex> allIndices = physicalPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
            if (CollectionUtils.isNotEmpty(allIndices)) {
                MaterializedIndex materializedIndex = allIndices.get(0);
                List<Tablet> tablets = materializedIndex.getTablets();
                if (CollectionUtils.isNotEmpty(tablets)) {
                    Optional<LakeTablet> lakeTabletOptional = tablets.stream()
                            .filter(tablet -> tablet instanceof LakeTablet)
                            .map(tablet -> (LakeTablet) tablet)
                            .findFirst();
                    if (lakeTabletOptional.isPresent()) {
                        LakeTablet lakeTablet = lakeTabletOptional.get();
                        try {
                            ShardInfo shardInfo = GlobalStateMgr.getCurrentState().getStarOSAgent()
                                    .getShardInfo(lakeTablet.getShardId(), StarOSAgent.DEFAULT_WORKER_GROUP_ID);
                            pvo.setStoragePath(shardInfo.getFilePath().getFullPath());
                        } catch (StarClientException e) {
                            throw new IllegalStateException(e.getMessage(), e);
                        }
                    }
                    pvo.setTablets(tablets.stream().map(TabletView::createFrom).collect(Collectors.toList()));
                }
            }

            return pvo;
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getBucketNum() {
            return bucketNum;
        }

        public void setBucketNum(Integer bucketNum) {
            this.bucketNum = bucketNum;
        }

        public String getDistributionType() {
            return distributionType;
        }

        public void setDistributionType(String distributionType) {
            this.distributionType = distributionType;
        }

        public Long getVisibleVersion() {
            return visibleVersion;
        }

        public void setVisibleVersion(Long visibleVersion) {
            this.visibleVersion = visibleVersion;
        }

        public Long getVisibleVersionTime() {
            return visibleVersionTime;
        }

        public void setVisibleVersionTime(Long visibleVersionTime) {
            this.visibleVersionTime = visibleVersionTime;
        }

        public Long getNextVersion() {
            return nextVersion;
        }

        public void setNextVersion(Long nextVersion) {
            this.nextVersion = nextVersion;
        }

        public Boolean getMinPartition() {
            return isMinPartition;
        }

        public void setMinPartition(Boolean minPartition) {
            isMinPartition = minPartition;
        }

        public Boolean getMaxPartition() {
            return isMaxPartition;
        }

        public void setMaxPartition(Boolean maxPartition) {
            isMaxPartition = maxPartition;
        }

        public List<Object> getStartKeys() {
            return startKeys;
        }

        public void setStartKeys(List<Object> startKeys) {
            this.startKeys = startKeys;
        }

        public List<Object> getEndKeys() {
            return endKeys;
        }

        public void setEndKeys(List<Object> endKeys) {
            this.endKeys = endKeys;
        }

        public List<List<Object>> getInKeys() {
            return inKeys;
        }

        public void setInKeys(List<List<Object>> inKeys) {
            this.inKeys = inKeys;
        }

        public String getStoragePath() {
            return storagePath;
        }

        public void setStoragePath(String storagePath) {
            this.storagePath = storagePath;
        }

        public List<TabletView> getTablets() {
            return tablets;
        }

        public void setTablets(List<TabletView> tablets) {
            this.tablets = tablets;
        }
    }
}
