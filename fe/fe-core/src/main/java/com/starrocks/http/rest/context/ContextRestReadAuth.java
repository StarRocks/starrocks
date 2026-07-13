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
import com.starrocks.context.ContextMgr;
import com.starrocks.context.ContextReadExecutor;
import com.starrocks.context.service.ContextQueryService;
import com.starrocks.qe.ConnectContext;

final class ContextRestReadAuth {

    static final String HIDDEN_ENTITY_MESSAGE = "entity not found";

    private ContextRestReadAuth() {
    }

    static ContextMgr.ContextBaseMeta authorizeHistoryEntity(ContextMgr mgr,
                                                             ContextReadExecutor reader,
                                                             long entityId)
            throws AccessDeniedException {
        ConnectContext ctx = ConnectContext.get();
        boolean admin = ContextRestAuth.hasFullVisibility(ctx);
        long contextBaseId = reader.resolveContextBaseIdForEntity(entityId);
        if (contextBaseId < 0) {
            if (admin) {
                return null;
            }
            throw hiddenEntityDenied();
        }
        ContextMgr.ContextBaseMeta cb = mgr.getContextBaseById(contextBaseId);
        if (cb == null) {
            if (admin) {
                return null;
            }
            throw hiddenEntityDenied();
        }
        try {
            ContextRestAuth.checkOnContextBase(ctx, cb.getName(), ContextRestAuth.BaseAction.USAGE);
            return cb;
        } catch (AccessDeniedException e) {
            if (admin) {
                throw e;
            }
            throw hiddenEntityDenied();
        }
    }

    static void authorizeGetRequest(ContextMgr mgr,
                                    ContextReadExecutor reader,
                                    ContextQueryService.ReadRequest readRequest)
            throws AccessDeniedException {
        ConnectContext ctx = ConnectContext.get();
        boolean admin = ContextRestAuth.hasFullVisibility(ctx);
        boolean hasDeclaredBase = !Strings.isNullOrEmpty(readRequest.contextBase);

        if (hasDeclaredBase) {
            ContextRestAuth.checkOnContextBase(ctx, readRequest.contextBase, ContextRestAuth.BaseAction.USAGE);
            if (readRequest.id != null && readRequest.id > 0) {
                long owningBaseId = reader.resolveContextBaseIdForEntity(readRequest.id);
                ContextMgr.ContextBaseMeta owning = owningBaseId > 0 ? mgr.getContextBaseById(owningBaseId) : null;
                if (owning == null) {
                    if (!admin) {
                        throw hiddenEntityDenied();
                    }
                    return;
                }
                if (!owning.getName().equals(readRequest.contextBase)) {
                    if (admin) {
                        throw new AccessDeniedException(
                                "entity " + readRequest.id + " does not belong to contextbase "
                                        + readRequest.contextBase);
                    }
                    throw hiddenEntityDenied();
                }
            }
            return;
        }

        if (!Strings.isNullOrEmpty(readRequest.entityKey)) {
            throw new AccessDeniedException("\"contextbase\" is required when looking up by entity_key");
        }
        if (readRequest.id == null || readRequest.id <= 0) {
            if (admin) {
                throw new AccessDeniedException("contextbase or resolvable entity_id is required");
            }
            throw hiddenEntityDenied();
        }

        long owningBaseId = reader.resolveContextBaseIdForEntity(readRequest.id);
        ContextMgr.ContextBaseMeta cb = owningBaseId > 0 ? mgr.getContextBaseById(owningBaseId) : null;
        if (cb == null) {
            if (admin) {
                return;
            }
            throw hiddenEntityDenied();
        }
        try {
            ContextRestAuth.checkOnContextBase(ctx, cb.getName(), ContextRestAuth.BaseAction.USAGE);
        } catch (AccessDeniedException e) {
            if (admin) {
                throw e;
            }
            throw hiddenEntityDenied();
        }
    }

    private static AccessDeniedException hiddenEntityDenied() {
        return new AccessDeniedException(HIDDEN_ENTITY_MESSAGE);
    }
}
