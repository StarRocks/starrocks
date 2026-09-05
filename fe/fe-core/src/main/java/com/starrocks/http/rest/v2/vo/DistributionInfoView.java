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
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.sql.common.MetaUtils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class DistributionInfoView {

    @SerializedName("type")
    private String type;

    @SerializedName("bucketNum")
    private Integer bucketNum;

    @SerializedName("distributionColumns")
    private List<ColumnView> distributionColumns;

    public DistributionInfoView() {
    }

    /**
     * Create from {@link DistributionInfo}
     */
    public static DistributionInfoView createFrom(Table table, DistributionInfo distributionInfo) {
        DistributionInfoView dvo = new DistributionInfoView();
        Optional.ofNullable(distributionInfo.getType())
                .map(Enum::name).ifPresent(dvo::setType);

        if (distributionInfo instanceof HashDistributionInfo) {
            HashDistributionInfo hdi = (HashDistributionInfo) distributionInfo;
            dvo.setBucketNum(hdi.getBucketNum());

            Optional.of(MetaUtils.getColumnsByColumnIds(table, hdi.getDistributionColumns()))
                    .map(columns -> columns.stream()
                            .filter(Objects::nonNull)
                            .map(ColumnView::createFrom)
                            .collect(Collectors.toList()))
                    .ifPresent(dvo::setDistributionColumns);
        } else if (distributionInfo instanceof RandomDistributionInfo) {
            RandomDistributionInfo rdi = (RandomDistributionInfo) distributionInfo;
            dvo.setBucketNum(rdi.getBucketNum());
        }

        return dvo;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getBucketNum() {
        return bucketNum;
    }

    public void setBucketNum(int bucketNum) {
        this.bucketNum = bucketNum;
    }

    public List<ColumnView> getDistributionColumns() {
        return distributionColumns;
    }

    public void setDistributionColumns(List<ColumnView> distributionColumns) {
        this.distributionColumns = distributionColumns;
    }
}
