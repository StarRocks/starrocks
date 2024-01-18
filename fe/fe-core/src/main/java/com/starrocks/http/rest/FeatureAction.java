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

import com.starrocks.common.Version;
import com.starrocks.common.feature.ProductFeature;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import io.netty.handler.codec.http.HttpMethod;

public class FeatureAction extends RestBaseAction {
    public FeatureAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller)
            throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/v2/feature", new FeatureAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        response.setContentType("application/json");

        RestResult result = new RestResult();
        result.addResultEntry("version", Version.STARROCKS_VERSION);
        result.addResultEntry("features", ProductFeature.getFeatures());
        sendResult(request, response, result);
    }
}
