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
import com.starrocks.epack.system.LicenseInfo;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;

import java.util.List;

public class LicenseListAction extends RestBaseAction {
    public static final String URI = "/api/v1/license/list";

    public LicenseListAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, URI, new LicenseListAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        List<LicenseInfo> licenseInfos = GlobalStateMgr.getCurrentState().getLicenseMgr().getAllLicenseInfo();
        response.setContentType("application/json");
        response.getContent().append(new Gson().toJson(licenseInfos));
        sendResult(request, response);
    }
}
