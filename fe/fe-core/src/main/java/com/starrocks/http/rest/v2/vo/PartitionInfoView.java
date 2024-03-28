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
import com.starrocks.catalog.PartitionInfo;

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
    public static PartitionInfoView createFrom(PartitionInfo partitionInfo) {
        PartitionInfoView pvo = new PartitionInfoView();
        pvo.setType(partitionInfo.getType().typeString);
        if (!partitionInfo.isPartitioned()) {
            return pvo;
        }

        Optional.ofNullable(partitionInfo.getPartitionColumns())
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
}
