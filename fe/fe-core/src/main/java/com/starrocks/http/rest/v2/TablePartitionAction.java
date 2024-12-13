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

package com.starrocks.http.rest.v2;

import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.catalog.Partition;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.rest.v2.RestBaseResultV2.PagedResult;
import com.starrocks.http.rest.v2.vo.PartitionInfoView.PartitionView;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.starrocks.catalog.InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;

public class TablePartitionAction extends TableBaseAction {

    private static final String TEMPORARY_KEY = "temporary";

    public TablePartitionAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(
                HttpMethod.GET,
                String.format(
                        "/api/v2/catalogs/{%s}/databases/{%s}/tables/{%s}/partition", CATALOG_KEY, DB_KEY, TABLE_KEY),
                new TablePartitionAction(controller));
    }

    @Override
    protected void doExecuteWithoutPassword(
            BaseRequest request, BaseResponse response) throws AccessDeniedException {
        String catalogName = getSingleParameterOrDefault(
                request, CATALOG_KEY, DEFAULT_INTERNAL_CATALOG_NAME, Function.identity());
        String dbName = getSingleParameterRequired(request, DB_KEY, Function.identity());
        String tableName = getSingleParameterRequired(request, TABLE_KEY, Function.identity());
        boolean temporary = getSingleParameterOrDefault(request, TEMPORARY_KEY, false, BooleanUtils::toBoolean);
        int pageNum = getPageNum(request);
        int pageSize = getPageSize(request);

        PagedResult<PartitionView> pagedResult = this.getAndApplyOlapTable(catalogName, dbName, tableName, olapTable -> {
            PagedResult<PartitionView> result = new PagedResult<>();
            result.setPageNum(pageNum);
            result.setPageSize(pageSize);
            result.setPages(0);
            result.setTotal(0);
            result.setItems(new ArrayList<>(0));

            Collection<Partition> partitions =
                    temporary ? olapTable.getTempPartitions() : olapTable.getPartitions();
            if (CollectionUtils.isNotEmpty(partitions)) {
                int pages = partitions.size() / pageSize;
                result.setPages(partitions.size() % pageSize == 0 ? pages : (pages + 1));
                result.setTotal(partitions.size());
                result.setItems(partitions.stream()
                        .sorted(Comparator.comparingLong(Partition::getId))
                        .skip((long) pageNum * pageSize)
                        .limit(pageSize)
                        .map(partition ->
                                PartitionView.createFrom(olapTable.getPartitionInfo(), partition))
                        .collect(Collectors.toList())
                );
            }

            return result;
        });

        sendResult(request, response, RestBaseResultV2.ok(pagedResult));
    }

}
