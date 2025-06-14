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

package com.starrocks.http.rest;

import com.google.common.base.Strings;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.dump.QueryDumper;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/* Usage:
   eg:
        POST  /api/query_dump?db=test  post_data=query
 return:
        {"statement": "...", "table_meta" : {..}, "table_row_count" : {...}, "session_variables" : "...",
         "column_statistics" : {...}}
 */

public class QueryDumpAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(QueryDumpAction.class);

    public static final String URL = "/api/query_dump";
    private static final String DB = "db";
    private static final String MOCK = "mock";

    public QueryDumpAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST, URL, new QueryDumpAction(controller));
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException {
        ConnectContext context = ConnectContext.get();
        String catalogDbName = request.getSingleParameter(DB);
        boolean enableMock = request.getSingleParameter(MOCK) == null ||
                "true".equalsIgnoreCase(StringUtils.trim(request.getSingleParameter(MOCK)));

        String catalogName = "";
        String dbName = "";
        if (!Strings.isNullOrEmpty(catalogDbName)) {
            String[] catalogDbNames = catalogDbName.split("\\.");

            catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
            if (catalogDbNames.length == 2) {
                catalogName = catalogDbNames[0];
            }
            dbName = catalogDbNames[catalogDbNames.length - 1];
        }
        context.setIsHTTPQueryDump(true);

        String query = request.getContent();

        Pair<HttpResponseStatus, String> statusAndRes = QueryDumper.dumpQuery(catalogName, dbName, query, enableMock);

        response.getContent().append(statusAndRes.second);
        sendResult(request, response, statusAndRes.first);
    }
}
