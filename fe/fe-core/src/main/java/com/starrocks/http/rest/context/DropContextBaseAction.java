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

package com.starrocks.http.rest.context;

import com.google.common.base.Strings;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.common.DdlException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.http.rest.RestBaseResult;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;

/**
 * {@code DELETE /api/contextbases/{name}}. Query parameter {@code if_exists=true} makes the call idempotent.
 */
public class DropContextBaseAction extends RestBaseAction {

    private static final String NAME_KEY = "name";

    public DropContextBaseAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.DELETE, "/api/contextbases/{" + NAME_KEY + "}",
                new DropContextBaseAction(controller));
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException, AccessDeniedException {
        String name = request.getSingleParameter(NAME_KEY);
        if (Strings.isNullOrEmpty(name)) {
            sendResult(request, response, new RestBaseResult("contextbase name required in path"));
            return;
        }
        ContextRestAuth.checkOnContextBase(ConnectContext.get(), name, ContextRestAuth.BaseAction.DROP);
        boolean ifExists = "true".equalsIgnoreCase(request.getSingleParameter("if_exists"));
        try {
            GlobalStateMgr.getCurrentState().getContextMgr().dropContextBase(name, ifExists);
            sendResult(request, response);
        } catch (com.starrocks.context.error.ContextException e) {
            sendResultByJson(request, response,
                    ContextErrorResult.fromException(e, ContextRestAuth.currentRequestId()));
        } catch (IllegalStateException e) {
            sendResult(request, response, new RestBaseResult(e.getMessage()));
        }
    }
}
