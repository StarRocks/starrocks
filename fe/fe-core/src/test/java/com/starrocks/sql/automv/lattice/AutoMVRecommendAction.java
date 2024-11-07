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
import com.starrocks.sql.util.Util;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpUtil;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AutoMVRecommendAction extends RestBaseAction {
    public static final Logger LOG = LogManager.getLogger(AutoMVRecommendAction.class);
    private static final File HISTORY_DIR = new File(
            Optional.ofNullable(System.getenv("MV_RECOMMEND_HISTORY_DIR"))
                    .orElse("history_dir"));
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private String previousTimestamp = Util.yyyyMMddTHHmmss();
    private Integer idx = 0;

    public AutoMVRecommendAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST, "/api/v1/automv_recommend", new AutoMVRecommendAction(controller));
    }

    private void createHistoryDirIfNotExists() {
        synchronized (HISTORY_DIR) {
            if (!HISTORY_DIR.exists()) {
                try {
                    HISTORY_DIR.mkdirs();
                } catch (Throwable ignored) {
                }
            }
        }
    }

    String uniqueFileName() {
        String currentTimestamp = Util.yyyyMMddTHHmmss();
        synchronized (this) {
            if (currentTimestamp.equals(previousTimestamp)) {
                String suffix = "0000" + (++idx);
                suffix = suffix.substring(suffix.length() - 4);
                return currentTimestamp + "_" + suffix;
            } else {
                previousTimestamp = currentTimestamp;
                idx = 0;
                return currentTimestamp + "_0000";
            }
        }
    }

    void saveHistoryAsync(String queryDump, String result) {
        executor.submit(() -> {
            createHistoryDirIfNotExists();
            String name = uniqueFileName();
            File queryDumpFile = new File(HISTORY_DIR, name + ".json");
            File resultFile = new File(HISTORY_DIR, name + "_result.txt");
            try {
                FileWriter queryDumpWriter = new FileWriter(queryDumpFile);
                FileWriter resultWriter = new FileWriter(resultFile);
                IOUtils.write(queryDump, queryDumpWriter);
                IOUtils.write(result, resultWriter);
                queryDumpWriter.flush();
                queryDumpWriter.close();
                resultWriter.flush();
                resultWriter.close();
            } catch (Throwable ex) {
                LOG.error("Fail to create file", ex);
            }
        });
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
        String output = recommender.recommendAndFormatOutput(requestContent, params::setSessionVariables);
        response.appendContent(output);
        sendResult(request, response);
        saveHistoryAsync(requestContent, output);
    }
}