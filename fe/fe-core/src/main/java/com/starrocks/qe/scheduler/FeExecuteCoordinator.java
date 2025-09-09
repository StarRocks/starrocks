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

package com.starrocks.qe.scheduler;

import com.google.common.collect.Lists;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.Status;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.datacache.DataCacheSelectMetrics;
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.planner.ScanNode;
import com.starrocks.proto.PPlanFragmentCancelReason;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryStatisticsItem;
import com.starrocks.qe.RowBatch;
import com.starrocks.qe.scheduler.slot.LogicalSlot;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.common.RyuDouble;
import com.starrocks.sql.common.RyuFloat;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TReportAuditStatisticsParams;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TSinkCommitInfo;
import com.starrocks.thrift.TTabletCommitInfo;
import com.starrocks.thrift.TTabletFailInfo;
import com.starrocks.thrift.TUniqueId;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FeExecuteCoordinator extends Coordinator {

    private final ConnectContext connectContext;

    private final ExecPlan execPlan;

    public FeExecuteCoordinator(ConnectContext context, ExecPlan execPlan) {
        this.connectContext = context;
        this.execPlan = execPlan;
    }

    @Override
    public void startScheduling(ScheduleOption option) throws StarRocksException {

    }

    @Override
    public String getSchedulerExplain() {
        return "FE EXECUTION";
    }

    @Override
    public void updateFragmentExecStatus(TReportExecStatusParams params) {

    }

    @Override
    public void updateAuditStatistics(TReportAuditStatisticsParams params) {

    }

    @Override
    public void cancel(PPlanFragmentCancelReason reason, String message) {

    }

    @Override
    public void onFinished() {

    }

    @Override
    public LogicalSlot getSlot() {
        return null;
    }

    @Override
    public RowBatch getNext() throws Exception {
        RowBatch rowBatch = new RowBatch();
        TResultBatch resultBatch = new TResultBatch();
        resultBatch.rows = covertToMySQLRowBuffer();
        PQueryStatistics statistics = new PQueryStatistics();
        statistics.returnedRows = Long.valueOf(resultBatch.rows.size());
        rowBatch.setBatch(resultBatch);
        rowBatch.setQueryStatistics(statistics);
        return rowBatch;
    }

    @Override
    public boolean join(int timeoutSecond) {
        return false;
    }

    @Override
    public boolean checkBackendState() {
        return false;
    }

    @Override
    public boolean isThriftServerHighLoad() {
        return false;
    }

    @Override
    public TLoadJobType getLoadJobType() {
        return null;
    }

    @Override
    public void setLoadJobType(TLoadJobType type) {

    }

    @Override
    public long getLoadJobId() {
        return 0;
    }

    @Override
    public void setLoadJobId(Long jobId) {

    }

    @Override
    public Map<Integer, TNetworkAddress> getChannelIdToBEHTTPMap() {
        return null;
    }

    @Override
    public Map<Integer, TNetworkAddress> getChannelIdToBEPortMap() {
        return null;
    }

    @Override
    public boolean isEnableLoadProfile() {
        return false;
    }

    @Override
    public void clearExportStatus() {

    }

    @Override
    public void collectProfileSync() {

    }

    @Override
    public boolean tryProcessProfileAsync(Consumer<Boolean> task) {
        return false;
    }

    @Override
    public void setTopProfileSupplier(Supplier<RuntimeProfile> topProfileSupplier) {

    }

    @Override
    public void setExecPlan(ExecPlan execPlan) {

    }

    @Override
    public RuntimeProfile buildQueryProfile(boolean needMerge) {
        return null;
    }

    @Override
    public RuntimeProfile getQueryProfile() {
        return null;
    }

    @Override
    public List<String> getDeltaUrls() {
        return null;
    }

    @Override
    public Map<String, String> getLoadCounters() {
        return null;
    }

    @Override
    public List<TTabletFailInfo> getFailInfos() {
        return null;
    }

    @Override
    public List<TTabletCommitInfo> getCommitInfos() {
        return null;
    }

    @Override
    public List<TSinkCommitInfo> getSinkCommitInfos() {
        return null;
    }

    @Override
    public List<String> getExportFiles() {
        return null;
    }

    @Override
    public String getTrackingUrl() {
        return null;
    }

    @Override
    public List<String> getRejectedRecordPaths() {
        return null;
    }

    @Override
    public List<QueryStatisticsItem.FragmentInstanceInfo> getFragmentInstanceInfos() {
        return null;
    }

    @Override
    public DataCacheSelectMetrics getDataCacheSelectMetrics() {
        return null;
    }

    @Override
    public PQueryStatistics getAuditStatistics() {
        return null;
    }

    @Override
    public Status getExecStatus() {
        return null;
    }

    @Override
    public boolean isUsingBackend(Long backendID) {
        return false;
    }

    @Override
    public boolean isDone() {
        return false;
    }

    @Override
    public TUniqueId getQueryId() {
        return null;
    }

    @Override
    public void setQueryId(TUniqueId queryId) {

    }

    @Override
    public List<ScanNode> getScanNodes() {
        return null;
    }

    @Override
    public long getStartTimeMs() {
        return 0;
    }

    @Override
    public void setTimeoutSecond(int timeoutSecond) {

    }

    @Override
    public boolean isProfileAlreadyReported() {
        return false;
    }

    @Override
    public String getWarehouseName() {
        if (connectContext == null) {
            return "";
        }
        return connectContext.getSessionVariable().getWarehouseName();
    }

    public long getCurrentWarehouseId() {
        return connectContext.getCurrentWarehouseId();
    }

    @Override
    public String getResourceGroupName() {
        return "";
    }

    public boolean isShortCircuit() {
        return false;
    }

    private List<ByteBuffer> covertToMySQLRowBuffer() {
        final MysqlSerializer serializer = MysqlSerializer.newInstance();
        final PhysicalValuesOperator valuesOperator = (PhysicalValuesOperator) execPlan.getPhysicalPlan().getOp();
        final List<ColumnRefOperator> outputColumnRefs = execPlan.getOutputColumns();
        final Projection valueOperatorProjection = valuesOperator.getProjection();
        final List<ColumnRefOperator> valuesOperatorColumnRefs = valuesOperator.getColumnRefSet();

        // NOTE: this is only used when value operator's projection is null since row will be output
        // by projection mapping if project exists.
        final List<ByteBuffer> res = Lists.newArrayList();
        if (valueOperatorProjection == null) {
            // Map values operator's output column references to their indices
            // NOTE: valuesOperatorColumnRefs's length may be different from outputColumnRefs's
            // length when the query is from an empty table scan.
            final Map<ColumnRefOperator, Integer> valuesOperatorOutputMap = IntStream.range(0, valuesOperatorColumnRefs.size())
                    .boxed()
                    .collect(Collectors.toMap(valuesOperatorColumnRefs::get, i -> i));
            // Find the indices of the output columns in the values operator's output
            final List<Integer> alignedOutputIndexes = outputColumnRefs.stream()
                    .map(valuesOperatorOutputMap::get)
                    .collect(Collectors.toList());
            for (final List<ScalarOperator> row : valuesOperator.getRows()) {
                serializer.reset();
                final List<ScalarOperator> alignedRow = alignedOutputIndexes.stream()
                        .map(i -> row.get(i))
                        .collect(Collectors.toUnmodifiableList());
                serializeAlignedRow(alignedRow, serializer);
                res.add(serializer.toByteBuffer());
            }
        } else {
            List<ScalarOperator> alignedRow = execPlan.getOutputExprs().stream()
                    .map(expr -> {
                        int slotId = ((SlotRef) expr).getSlotId().asInt();
                        return valuesOperator.getProjection().getColumnRefMap().entrySet().stream()
                                .filter(entry -> entry.getKey().getId() == slotId)
                                .map(Map.Entry::getValue)
                                .findFirst()
                                .orElseThrow(() -> new IllegalStateException("No match for slotId: " + slotId));
                    })
                    .collect(Collectors.toList());
            for (final List<ScalarOperator> row : valuesOperator.getRows()) {
                serializer.reset();
                serializeAlignedRow(alignedRow, serializer);
                res.add(serializer.toByteBuffer());
            }
        }
        return res;
    }

    private void serializeAlignedRow(List<ScalarOperator> alignedRow,
                                     MysqlSerializer serializer) {
        for (ScalarOperator scalarOperator : alignedRow) {
            ConstantOperator constantOperator = (ConstantOperator) scalarOperator;
            if (constantOperator.isNull()) {
                serializer.writeNull();
            } else if (constantOperator.isTrue()) {
                serializer.writeLenEncodedString("1");
            } else if (constantOperator.isFalse()) {
                serializer.writeLenEncodedString("0");
            } else if (constantOperator.getType().getPrimitiveType().isBinaryType()) {
                serializer.writeVInt(constantOperator.getBinary().length);
                serializer.writeBytes(constantOperator.getBinary());
            } else {
                String value;
                switch (constantOperator.getType().getPrimitiveType()) {
                    case TINYINT:
                        value = String.valueOf(constantOperator.getTinyInt());
                        break;
                    case SMALLINT:
                        value = String.valueOf(constantOperator.getSmallint());
                        break;
                    case INT:
                        value = String.valueOf(constantOperator.getInt());
                        break;
                    case BIGINT:
                        value = String.valueOf(constantOperator.getBigint());
                        break;
                    case LARGEINT:
                        value = String.valueOf(constantOperator.getLargeInt());
                        break;
                    case FLOAT:
                        value = RyuFloat.floatToString((float) constantOperator.getFloat());
                        break;
                    case DOUBLE:
                        value = RyuDouble.doubleToString(constantOperator.getDouble());
                        break;
                    case DECIMALV2:
                        value = constantOperator.getDecimal().toPlainString();
                        break;
                    case DECIMAL32:
                    case DECIMAL64:
                    case DECIMAL128:
                    case DECIMAL256:
                        int scale = ((ScalarType) constantOperator.getType()).getScalarScale();
                        BigDecimal val1 = constantOperator.getDecimal();
                        DecimalFormat df = new DecimalFormat((scale == 0 ? "0" : "0.") + StringUtils.repeat("0", scale));
                        value = df.format(val1);
                        break;
                    case CHAR:
                        value = constantOperator.getChar();
                        break;
                    case VARCHAR:
                        value = constantOperator.getVarchar();
                        break;
                    case TIME:
                        value = convertToTimeString(constantOperator.getTime());
                        break;
                    case DATE:
                        LocalDateTime date = constantOperator.getDate();
                        value = date.format(DateUtils.DATE_FORMATTER_UNIX);
                        break;
                    case DATETIME:
                        LocalDateTime datetime = constantOperator.getDate();
                        if (datetime.getNano() != 0) {
                            value = datetime.format(DateUtils.DATE_TIME_MS_FORMATTER_UNIX);
                        } else {
                            value = datetime.format(DateUtils.DATE_TIME_FORMATTER_UNIX);
                        }
                        break;
                    default:
                        value = constantOperator.toString();
                }
                serializer.writeLenEncodedString(value);
            }
        }
    }

    private String convertToTimeString(double time) {
        StringBuilder sb = new StringBuilder();
        if (time < 0) {
            sb.append("-");
            time = Math.abs(time);
        }

        int day = (int) (time / 86400);
        time = time % 86400;
        int hour = (int) (time / 3600);
        time = time % 3600;
        int minute = (int) (time / 60);
        time = time % 60;
        int second = (int) time;
        sb.append(String.format("%02d:%02d:%02d", hour + day * 24, minute, second));
        return sb.toString();
    }
}
