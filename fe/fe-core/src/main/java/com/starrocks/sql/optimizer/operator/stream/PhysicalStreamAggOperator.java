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

package com.starrocks.sql.optimizer.operator.stream;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.planner.BinlogScanNode;
import com.starrocks.planner.stream.StreamAggNode;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class PhysicalStreamAggOperator extends PhysicalStreamOperator {

    private static final Logger LOG = LogManager.getLogger(BinlogScanNode.class);

    private final List<ColumnRefOperator> groupBys;
    private final Map<ColumnRefOperator, CallOperator> aggregations;

    // IMT information
    private TableName resultIMTName;
    private TableName intermediateIMTName;
    private TableName detailIMTName;

    private IMTStateTable resultIMT;

    private IMTStateTable intermediateIMT;

    private IMTStateTable detailIMT;

    private StreamAggNode streamAggNode;

    public PhysicalStreamAggOperator(List<ColumnRefOperator> groupBys,
                                     Map<ColumnRefOperator, CallOperator> aggregations, ScalarOperator predicate,
                                     Projection projection) {
        super(OperatorType.PHYSICAL_STREAM_AGG);

        // Change function name in Stream MV situation.
        Map<ColumnRefOperator, CallOperator> streamAggregations = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, CallOperator> aggregation : aggregations.entrySet()) {
            CallOperator newAggregation;
            String fnName = aggregation.getValue().getFnName();

            CallOperator oldAggregation = aggregation.getValue();
            newAggregation = aggregation.getValue();
            if (fnName.equalsIgnoreCase(FunctionSet.MAX)) {
                newAggregation.resetFnName(FunctionSet.RETRACT_MAX);
            } else if (fnName.equalsIgnoreCase(FunctionSet.MIN)) {
                newAggregation.resetFnName(FunctionSet.RETRACT_MIN);
            }
            streamAggregations.put(aggregation.getKey(), newAggregation);
        }
        this.aggregations = aggregations;

        this.groupBys = groupBys;
    }

    public List<ColumnRefOperator> getGroupBys() {
        return groupBys;
    }

    public Map<ColumnRefOperator, CallOperator> getAggregations() {
        return aggregations;
    }

    public TableName getResultIMTName() {
        return resultIMTName;
    }

    public void setResultIMTName(TableName resultIMTName) {
        this.resultIMTName = resultIMTName;
    }

    public TableName getIntermediateIMTName() {
        return intermediateIMTName;
    }

    public void setIntermediateIMTName(TableName intermediateIMTName) {
        this.intermediateIMTName = intermediateIMTName;
    }

    public TableName getDetailIMTName() {
        return detailIMTName;
    }

    public IMTStateTable getResultIMT() {
        return resultIMT;
    }

    public IMTStateTable getIntermediateIMT() {
        return intermediateIMT;
    }

    public IMTStateTable getDetailIMT() {
        return detailIMT;
    }

    public void setStreamAggNode(StreamAggNode streamAggNode) {
        this.streamAggNode = streamAggNode;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalStreamAgg(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalStreamAgg(optExpression, context);
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        ColumnRefSet columns = super.getUsedColumns();
        groupBys.forEach(columns::union);
        aggregations.values().forEach(d -> columns.union(d.getUsedColumns()));
        return columns;
    }

    @Override
    public List<IMTStateTable> assignIMTInfos() throws DdlException  {
        Preconditions.checkState(resultIMTName != null);
        List<IMTStateTable> imtStateTables = Lists.newArrayList();
        try {
            long dbId = GlobalStateMgr.getCurrentState().getDb(resultIMTName.getDb()).getId();
            LOG.info("Create result IMTInfo: {}", resultIMTName);
            resultIMT = IMTStateTable.fromTableName(dbId, resultIMTName);
            streamAggNode.setResultImt(resultIMT);
            imtStateTables.add(resultIMT);

            if (intermediateIMTName != null) {
                LOG.info("Create intermediate IMTInfo: {}", intermediateIMT);
                intermediateIMT = IMTStateTable.fromTableName(dbId, intermediateIMTName);
                streamAggNode.setIntermediateImt(intermediateIMT);
                imtStateTables.add(intermediateIMT);
            }
            if (detailIMTName != null) {
                LOG.info("Create detail IMTInfo: {}", detailIMTName);
                detailIMT = IMTStateTable.fromTableName(dbId, detailIMTName);
                streamAggNode.setDetailImt(detailIMT);
                imtStateTables.add(detailIMT);
            }
        } catch (UserException e) {
            throw new DdlException("Failed to deduce IMT Info, " + e.getMessage(), e);
        }
        return imtStateTables;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(aggregations.values().stream().map(CallOperator::toString).collect(Collectors.joining(", ")));
        if (CollectionUtils.isNotEmpty(groupBys)) {
            sb.append(" group by ");
            sb.append(groupBys.stream().map(ColumnRefOperator::getName).collect(Collectors.joining(", ")));
        }
        return "PhysicalStreamAgg " + sb;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), groupBys, aggregations.keySet());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        PhysicalStreamAggOperator that = (PhysicalStreamAggOperator) o;
        return Objects.equals(aggregations, that.aggregations) && Objects.equals(groupBys, that.groupBys);
    }
}
