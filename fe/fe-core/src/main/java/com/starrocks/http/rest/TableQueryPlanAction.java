// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/TableQueryPlanAction.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.http.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksHttpException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.planner.PlanFragment;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TMemoryScratchSink;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanFragment;
import com.starrocks.thrift.TQueryPlanInfo;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TTabletVersionInfo;
import com.starrocks.thrift.TUniqueId;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
<<<<<<< HEAD
=======
import org.apache.thrift.transport.TTransportException;
import org.codehaus.jackson.map.ObjectMapper;
>>>>>>> branch-2.5-mrs
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * This class responsible for parse the sql and generate the query plan fragment for a (only one) table{@see OlapTable}
 * the related tablet maybe pruned by query planer according the `where` predicate.
 */
public class TableQueryPlanAction extends RestBaseAction {

    public static final Logger LOG = LogManager.getLogger(TableQueryPlanAction.class);

    public TableQueryPlanAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST,
                "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_query_plan",
                new TableQueryPlanAction(controller));
        controller.registerHandler(HttpMethod.GET,
                "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_query_plan",
                new TableQueryPlanAction(controller));
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException {
        // just allocate 2 slot for top holder map
        Map<String, Object> resultMap = new HashMap<>(4);
        String dbName = request.getSingleParameter(DB_KEY);
        String tableName = request.getSingleParameter(TABLE_KEY);
        String postContent = request.getContent();
        try {
            // may be these common validate logic should be moved to one base class
            if (Strings.isNullOrEmpty(dbName)
                    || Strings.isNullOrEmpty(tableName)) {
                throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST, "{database}/{table} must be selected");
            }
            String sql;
            if (Strings.isNullOrEmpty(postContent)) {
                throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST,
                        "POST body must contains [sql] root object");
            }
            JSONObject jsonObject;
            try {
                jsonObject = new JSONObject(postContent);
            } catch (JSONException e) {
                throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST,
                        "malformed json [ " + postContent + " ]");
            }
            sql = jsonObject.optString("sql");
            if (Strings.isNullOrEmpty(sql)) {
                throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST,
                        "POST body must contains [sql] root object");
            }
            LOG.info("receive SQL statement [{}] from external service [ user [{}]] for database [{}] table [{}]",
                    sql, ConnectContext.get().getCurrentUserIdentity(), dbName, tableName);

            // check privilege for select, otherwise return HTTP 401
            checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), dbName, tableName, PrivPredicate.SELECT);
            Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
            if (db == null) {
                throw new StarRocksHttpException(HttpResponseStatus.NOT_FOUND,
                        "Database [" + dbName + "] " + "does not exists");
            }
            // may be should acquire writeLock
            db.readLock();
            try {
                Table table = db.getTable(tableName);
                if (table == null) {
                    throw new StarRocksHttpException(HttpResponseStatus.NOT_FOUND,
                            "Table [" + tableName + "] " + "does not exists");
                }
                // just only support OlapTable and MaterializedView, ignore others such as ESTable
                if (!table.isLocalTable()) {
                    // Forbidden
                    throw new StarRocksHttpException(HttpResponseStatus.FORBIDDEN,
                            "Only support OlapTable and MaterializedView currently");
                }
                // parse/analysis/plan the sql and acquire tablet distributions
                handleQuery(ConnectContext.get(), db.getFullName(), tableName, sql, resultMap);
            } finally {
                db.readUnlock();
            }
        } catch (StarRocksHttpException e) {
            // status code  should conforms to HTTP semantic
            resultMap.put("status", e.getCode().code());
            resultMap.put("exception", e.getMessage());
        } catch (TTransportException e) {
            resultMap.put("status", "-1");
            resultMap.put("exception", e.getMessage());
        }

        ObjectMapper mapper = new ObjectMapper();
        try {
            String result = mapper.writeValueAsString(resultMap);
            // send result with extra information
            response.setContentType("application/json");
            response.getContent().append(result);
            sendResult(request, response,
                    HttpResponseStatus.valueOf(Integer.parseInt(String.valueOf(resultMap.get("status")))));
        } catch (Exception e) {
            // may be this never happen
            response.getContent().append(e.getMessage());
            sendResult(request, response, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * process the sql syntax and return the resolved pruned tablet
     *
     * @param context context for analyzer
     * @param sql     the single table select statement
     * @param result  the acquired results
     * @return
     * @throws StarRocksHttpException
     */
    private void handleQuery(ConnectContext context, String requestDb, String requestTable, String sql,
                             Map<String, Object> result) throws TTransportException {
        StatementBase statementBase;
        ExecPlan execPlan;
        try {
            /*
             * SingleNodeExecPlan is set in TableQueryPlanAction to generate a single-node Plan,
             * currently only used in Spark/Flink Connector
             */
            context.getSessionVariable().setSingleNodeExecPlan(true);
            statementBase =
                    com.starrocks.sql.parser.SqlParser.parse(sql, context.getSessionVariable()).get(0);
            execPlan = new StatementPlanner().plan(statementBase, context);
            context.getSessionVariable().setSingleNodeExecPlan(false);
        } catch (Exception e) {
            LOG.error("error occurred when optimizing queryId: {}", context.getQueryId(), e);
            throw new StarRocksHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    "The Sql is invalid");
        }

        // only process select semantic
        if (!(statementBase instanceof QueryStatement)) {
            throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST,
                    "Select statement needed, but found [" + sql + " ]");
        }

        SelectRelation stmt = (SelectRelation) ((QueryStatement) statementBase).getQueryRelation();
        // only process sql like `select * from table where <predicate>`, only support executing scan semantic
        if (stmt.hasAggregation() || stmt.hasAnalyticInfo()
                || stmt.hasOrderByClause() || stmt.hasOffset() || stmt.hasLimit() || statementBase.isExplain()) {
            throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST,
                    "only support single table filter-prune-scan, but found [ " + sql + "]");
        }

        // only process one table by one http query
        if (AnalyzerUtils.collectAllTable(statementBase).size() != 1) {
            if (stmt.getRelation() instanceof TableRelation) {
                throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST,
                        "Select statement must have only one table");
            }
        }

        if (stmt.getRelation() instanceof SubqueryRelation) {
            throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST,
                    "Select statement must not embed another statement");
        }

        // check consistent http requested resource with sql referenced
        // if consistent in this way, can avoid check privilege
        TableName tableAndDb = stmt.getRelation().getResolveTableName();
        if (!(tableAndDb.getDb().equals(requestDb) && tableAndDb.getTbl().equals(requestTable))) {
            throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST,
                    "requested database and table must consistent with sql: request [ "
                            + requestDb + "." + requestTable + "]" + "and sql [" + tableAndDb.toString() + "]");
        }

        if (execPlan == null) {
            LOG.error("plan is null for queryId: {}", context.getQueryId());
            throw new StarRocksHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    "The Sql is invalid");
        }

        // acquire ScanNode to obtain pruned tablet
        // in this way, just retrieve only one scannode
        if (execPlan.getScanNodes().size() != 1) {
            throw new StarRocksHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    "Planner should plan just only one ScanNode but found [ " + execPlan.getScanNodes().size() + "]");
        }
        List<TScanRangeLocations> scanRangeLocations =
                execPlan.getScanNodes().get(0).getScanRangeLocations(0);
        // acquire the PlanFragment which the executable template
        List<PlanFragment> fragments = execPlan.getFragments();
        if (fragments.size() != 1) {
            throw new StarRocksHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    "Planner should plan just only one PlanFragment but found [ " + fragments.size() + "]");
        }

        TQueryPlanInfo tQueryPlanInfo = new TQueryPlanInfo();

        // acquire TPlanFragment
        TPlanFragment tPlanFragment = fragments.get(0).toThrift();
        // set up TMemoryScratchSink
        TDataSink tDataSink = new TDataSink();
        tDataSink.type = TDataSinkType.MEMORY_SCRATCH_SINK;
        tDataSink.memory_scratch_sink = new TMemoryScratchSink();
        tPlanFragment.output_sink = tDataSink;

        tQueryPlanInfo.plan_fragment = tPlanFragment;
        tQueryPlanInfo.desc_tbl = execPlan.getDescTbl().toThrift();
        // set query_id
        UUID uuid = UUID.randomUUID();
        tQueryPlanInfo.query_id = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());

        Map<Long, TTabletVersionInfo> tabletInfo = new HashMap<>();
        // acquire resolved tablet distribution
        Map<String, Node> tabletRoutings = assemblePrunedPartitions(scanRangeLocations);
        tabletRoutings.forEach((tabletId, node) -> {
            long tablet = Long.parseLong(tabletId);
            tabletInfo.put(tablet, new TTabletVersionInfo(tablet, node.version, 0, node.schemaHash));
        });
        tQueryPlanInfo.tablet_info = tabletInfo;

        // serialize TQueryPlanInfo and encode plan with Base64 to string in order to translate by json format
        TSerializer serializer = new TSerializer();
        String opaquedQueryPlan;
        try {
            byte[] queryPlanStream = serializer.serialize(tQueryPlanInfo);
            opaquedQueryPlan = Base64.getEncoder().encodeToString(queryPlanStream);
        } catch (TException e) {
            throw new StarRocksHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    "TSerializer failed to serialize PlanFragment, reason [ " + e.getMessage() + " ]");
        }
        result.put("partitions", tabletRoutings);
        result.put("opaqued_query_plan", opaquedQueryPlan);
        result.put("status", 200);
    }

    /**
     * acquire all involved (already pruned) tablet routing
     *
     * @param scanRangeLocationsList
     * @return
     */
    private Map<String, Node> assemblePrunedPartitions(List<TScanRangeLocations> scanRangeLocationsList) {
        Map<String, Node> result = new HashMap<>();
        for (TScanRangeLocations scanRangeLocations : scanRangeLocationsList) {
            // only process starrocks scan range
            TInternalScanRange scanRange = scanRangeLocations.scan_range.internal_scan_range;
            Node tabletRouting = new Node(Long.parseLong(scanRange.version), Integer.parseInt(scanRange.schema_hash));
            for (TNetworkAddress address : scanRange.hosts) {
                tabletRouting.addRouting(address.hostname + ":" + address.port);
            }
            result.put(String.valueOf(scanRange.tablet_id), tabletRouting);
        }
        return result;
    }

    // helper class for json transformation
    final class Node {
        // ["host1:port1", "host2:port2", "host3:port3"]
        public List<String> routings = new ArrayList<>();
        public long version;
        public int schemaHash;

        public Node(long version, int schemaHash) {
            this.version = version;
            this.schemaHash = schemaHash;
        }

        private void addRouting(String routing) {
            routings.add(routing);
        }
    }
}
