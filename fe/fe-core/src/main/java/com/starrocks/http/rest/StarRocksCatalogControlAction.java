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

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.common.DdlException;
import com.starrocks.connector.starrocks.StarRocksRemoteScanWire;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.StarRocksRemoteScanService;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * HTTP/JSON entry points for the StarRocks catalog control plane consumed by a peer
 * cluster's StarRocks catalog: capabilities, list databases/tables, get table, and
 * the remote-scan lifecycle (prepare / start / batch cleanup). Served over HTTP for
 * the same reason as the catalog statistics endpoints — the FE thrift port is
 * internal-facing and typically not reachable across cluster boundaries.
 *
 * <p>Authentication is handled by {@link RestBaseAction} (HTTP Basic); each endpoint
 * runs per-object authorization against the authenticated {@link ConnectContext}. The
 * logical status of an operation is carried inside the JSON envelope (so a "table not
 * found" is HTTP 200 with {@code status=404}); only transport/credential failures use
 * non-200 HTTP statuses.
 *
 * <p>The session-addressed endpoints (start_scan / cleanup_sessions) accept a
 * {@code forward_request} URL parameter: sessions live in the memory of the one FE
 * that served prepare_scan, and the remote FEs are often published behind a load
 * balancer that routes each request to a random FE. The calling cluster sends
 * {@code forward_request=true}; an FE that does not hold the session then asks its
 * peer FEs, sending {@code forward_request=false} so forwarding is a single hop. A
 * {@code status=404} envelope means "session not on this FE" (the forwarder tries the
 * next peer), any other failure comes from the FE that owns the session and is final.
 */
public class StarRocksCatalogControlAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(StarRocksCatalogControlAction.class);
    private static final Gson GSON = new Gson();

    private static final String PREFIX = "/api/_starrocks_remote";

    private enum Mode {
        CAPABILITIES,
        DATABASES,
        TABLES,
        TABLE,
        PREPARE_SCAN,
        START_SCAN,
        CLEANUP_SESSIONS
    }

    private final Mode mode;

    private StarRocksCatalogControlAction(ActionController controller, Mode mode) {
        super(controller);
        this.mode = mode;
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, PREFIX + "/capabilities",
                new StarRocksCatalogControlAction(controller, Mode.CAPABILITIES));
        controller.registerHandler(HttpMethod.GET, PREFIX + "/databases",
                new StarRocksCatalogControlAction(controller, Mode.DATABASES));
        controller.registerHandler(HttpMethod.GET, PREFIX + "/tables",
                new StarRocksCatalogControlAction(controller, Mode.TABLES));
        controller.registerHandler(HttpMethod.GET, PREFIX + "/table",
                new StarRocksCatalogControlAction(controller, Mode.TABLE));
        controller.registerHandler(HttpMethod.POST, PREFIX + "/prepare_scan",
                new StarRocksCatalogControlAction(controller, Mode.PREPARE_SCAN));
        controller.registerHandler(HttpMethod.POST, PREFIX + "/start_scan",
                new StarRocksCatalogControlAction(controller, Mode.START_SCAN));
        controller.registerHandler(HttpMethod.POST, PREFIX + "/cleanup_sessions",
                new StarRocksCatalogControlAction(controller, Mode.CLEANUP_SESSIONS));
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException, AccessDeniedException {
        ConnectContext context = ConnectContext.get();
        StarRocksRemoteScanService service = GlobalStateMgr.getCurrentState().getStarRocksRemoteScanService();
        Object result;
        try {
            switch (mode) {
                case CAPABILITIES:
                    result = service.getCapabilities(context);
                    break;
                case DATABASES:
                    result = service.listDatabases(context);
                    break;
                case TABLES:
                    result = service.listTables(context, request.getSingleParameter("db"));
                    break;
                case TABLE:
                    result = service.getTable(context,
                            request.getSingleParameter("db"), request.getSingleParameter("table"));
                    break;
                case PREPARE_SCAN:
                    result = service.prepareRemoteScan(context,
                            parseBody(request, StarRocksRemoteScanWire.PrepareScanRequest.class));
                    break;
                case START_SCAN:
                    result = service.startRemoteScan(context,
                            parseBody(request, StarRocksRemoteScanWire.ScanControlRequest.class),
                            isForwardRequest(request), request.getAuthorizationHeader());
                    break;
                case CLEANUP_SESSIONS:
                    result = service.batchCleanupScanSessions(context,
                            parseBody(request, StarRocksRemoteScanWire.BatchCleanupRequest.class),
                            isForwardRequest(request), request.getAuthorizationHeader());
                    break;
                default:
                    throw new DdlException("unsupported StarRocks catalog control mode: " + mode);
            }
        } catch (MalformedRequestBodyException e) {
            LOG.warn("malformed StarRocks catalog control request body [{}]", mode, e);
            StarRocksRemoteScanWire.SimpleResponse error = new StarRocksRemoteScanWire.SimpleResponse();
            error.status = HttpResponseStatus.BAD_REQUEST.code();
            error.exception = e.getMessage();
            result = error;
        } catch (Exception e) {
            LOG.warn("failed to serve StarRocks catalog control request [{}]", mode, e);
            StarRocksRemoteScanWire.SimpleResponse error = new StarRocksRemoteScanWire.SimpleResponse();
            error.status = HttpResponseStatus.INTERNAL_SERVER_ERROR.code();
            error.exception = e.getMessage();
            result = error;
        }

        response.setContentType(JSON_CONTENT_TYPE);
        response.getContent().append(GSON.toJson(result));
        // The logical status rides inside the JSON envelope; the HTTP status is always OK
        // so the client reads the body uniformly (HttpUtils.get throws on non-200 transport
        // codes). Credential failures are rejected earlier by RestBaseAction.
        sendResult(request, response, HttpResponseStatus.OK);
    }

    /**
     * Deserializes a POST body, reporting a malformed one as the caller's error. Left to the
     * generic handler it would surface as {@code status=500}, blaming this cluster for the
     * caller's bad request.
     */
    @VisibleForTesting
    static <T> T parseBody(BaseRequest request, Class<T> type) throws MalformedRequestBodyException {
        try {
            T parsed = GSON.fromJson(request.getContent(), type);
            if (parsed == null) {
                throw new MalformedRequestBodyException("request body is empty");
            }
            return parsed;
        } catch (MalformedRequestBodyException e) {
            throw e;
        } catch (Exception e) {
            throw new MalformedRequestBodyException("malformed json request body: " + e.getMessage());
        }
    }

    @VisibleForTesting
    static class MalformedRequestBodyException extends Exception {
        MalformedRequestBodyException(String message) {
            super(message);
        }
    }

    @VisibleForTesting
    static boolean isForwardRequest(BaseRequest request) {
        return Boolean.parseBoolean(request.getSingleParameter("forward_request"));
    }
}
