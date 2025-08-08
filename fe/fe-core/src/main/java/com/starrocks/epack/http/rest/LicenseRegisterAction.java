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
package com.starrocks.epack.http.rest;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.starrocks.common.DdlException;
import com.starrocks.epack.system.InvalidLicenseException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LicenseRegisterAction extends RestBaseAction {
    protected static final Logger LOG = LogManager.getLogger(LicenseRegisterAction.class);

    public static final String URI = "/api/v1/license/register";

    public LicenseRegisterAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST, URI, new LicenseRegisterAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) throws DdlException {
        if (redirectToLeader(request, response)) {
            return;
        }

        String license = null;
        try {
            license = request.getContent();
        } catch (Exception e) {
            // ignore, will handle below
        }
        if (license == null || license.trim().isEmpty()) {
            JsonObject result = new JsonObject();
            result.addProperty("error", "license is required in request body");
            response.setContentType("application/json");
            response.getContent().append(new Gson().toJson(result));
            sendResult(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }
        try {
            GlobalStateMgr.getCurrentState().getLicenseMgr().registerLicense(license.trim());
            sendResult(request, response);
        } catch (InvalidLicenseException e) {
            LOG.error("register license failed", e);
            JsonObject result = new JsonObject();
            result.addProperty("error", e.getMessage());
            response.setContentType("application/json");
            response.getContent().append(new Gson().toJson(result));
            sendResult(request, response, HttpResponseStatus.BAD_REQUEST);
        }
    }
}
