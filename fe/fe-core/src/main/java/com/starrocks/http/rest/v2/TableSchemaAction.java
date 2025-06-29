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
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.rest.v2.vo.TableSchemaView;
import io.netty.handler.codec.http.HttpMethod;

import java.util.function.Function;

import static com.starrocks.catalog.InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;

public class TableSchemaAction extends TableBaseAction {

    public TableSchemaAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(
                HttpMethod.GET,
                String.format(
                        "/api/v2/catalogs/{%s}/databases/{%s}/tables/{%s}/schema", CATALOG_KEY, DB_KEY, TABLE_KEY),
                new TableSchemaAction(controller));
    }

    @Override
    protected void doExecuteWithoutPassword(
            BaseRequest request, BaseResponse response) throws AccessDeniedException {
        String catalogName = getSingleParameterOrDefault(
                request, CATALOG_KEY, DEFAULT_INTERNAL_CATALOG_NAME, Function.identity());
        String dbName = getSingleParameterRequired(request, DB_KEY, Function.identity());
        String tableName = getSingleParameterRequired(request, TABLE_KEY, Function.identity());

        TableSchemaView result = this.getAndApplyOlapTable(
                catalogName, dbName, tableName, TableSchemaView::createFrom);
        sendResult(request, response, new RestBaseResultV2<>(result));
    }

}
