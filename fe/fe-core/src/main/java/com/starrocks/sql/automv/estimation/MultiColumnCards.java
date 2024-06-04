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

package com.starrocks.sql.automv.estimation;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.starrocks.sql.automv.qe.ColumnPlus;
import com.starrocks.sql.automv.util.ColumnDescription;
import com.starrocks.sql.automv.util.Util;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.starrocks.sql.automv.qe.ColumnPlus.BIGINT;
import static com.starrocks.sql.automv.qe.ColumnPlus.JSON;

public final class MultiColumnCards {
    public static final List<ColumnPlus> COLUMNS = collectColumns();
    @ColumnDescription(type = BIGINT)
    private Long rowCount;
    @ColumnDescription(type = JSON)
    private List<Long> cards;
    private long timeUsage;

    private static List<ColumnPlus> collectColumns() {
        return Stream.of(MultiColumnCards.class.getDeclaredFields())
                .filter(ColumnPlus::isAcceptable)
                .map(ColumnPlus::fieldToColumn)
                .collect(ImmutableList.toImmutableList());
    }

    public static List<ColumnPlus> getColumns() {
        return COLUMNS;
    }

    public Long getRowCount() {
        return rowCount;
    }

    public void setRowCount(Long rowCount) {
        this.rowCount = rowCount;
    }

    public List<Long> getCards() {
        return cards;
    }

    public void setCards(List<Long> cards) {
        this.cards = cards;
    }

    public void setCards(String cardsJson) {
        this.cards = new Gson().<List<Object>>fromJson(cardsJson, List.class)
                .stream().map(Util::toLong).collect(Collectors.toList());
    }

    public long getTimeUsage() {
        return this.timeUsage;
    }

    public void setTimeUsage(long timeUsage) {
        this.timeUsage = timeUsage;
    }
}