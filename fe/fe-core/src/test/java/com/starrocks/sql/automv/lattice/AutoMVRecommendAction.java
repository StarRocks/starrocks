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

package com.starrocks.sql.automv.lattice;

import com.starrocks.common.DdlException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.HttpConnectContext;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.rest.RestBaseAction;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpUtil;

import java.io.PrintWriter;
import java.io.StringWriter;

public class AutoMVRecommendAction extends RestBaseAction {
    public AutoMVRecommendAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST, "/api/v1/automv_recommend", new AutoMVRecommendAction(controller));
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException {
        String content = request.getContent();
        try {
            realWork(request, content, response);
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            response.setContentType("application/plain-text; charset=utf-8");
            response.appendContent(sw.toString());
            sendResult(request, response);
            throw new RuntimeException(e);
        }
    }

    private void realWork(BaseRequest request, String requestContent, BaseResponse response) throws Exception {
        response.setContentType("application/plain-text; charset=utf-8");
        HttpConnectContext context = request.getConnectContext();

        boolean keepAlive = HttpUtil.isKeepAlive(request.getRequest());
        if (keepAlive) {
            context.setKeepAlive(true);
        }

        MVRecommendParams params = MVRecommendParams.parseFromQueryParams(request.getAllParameters());
        QueryDumpMVRecommender recommender = QueryDumpMVRecommender.of();
        String mv = recommender.recommend(requestContent, params::setSessionVariables);
        response.appendContent(mv);
        response.appendContent("\n");
        sendResult(request, response);
    }
}