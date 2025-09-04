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

package com.starrocks.sql.ast;

import com.google.common.base.Joiner;
import com.starrocks.failpoint.TriggerPolicy;
import com.starrocks.proto.FailPointTriggerModeType;
import com.starrocks.proto.PFailPointTriggerMode;
import com.starrocks.proto.PUpdateFailPointStatusRequest;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TUpdateFailPointRequest;

import java.util.List;

public class UpdateFailPointStatusStatement extends StatementBase {
    private String name;
    private boolean isEnable = false;
    private Integer nTimes = null;
    private Double probability = null;
    private List<String> backends = null;

    public UpdateFailPointStatusStatement(String name, boolean isEnable, List<String> backends, NodePosition pos) {
        super(pos);
        this.name = name;
        this.isEnable = isEnable;
        this.backends = backends;
    }

    public UpdateFailPointStatusStatement(String name, int nTimes, List<String> backends, NodePosition pos) {
        this(name, true, backends, pos);
        this.nTimes = nTimes;
    }

    public UpdateFailPointStatusStatement(String name, double probability, List<String> backends, NodePosition pos) {
        this(name, true, backends, pos);
        this.probability = probability;
    }

    public String getName() {
        return name;
    }

    public PUpdateFailPointStatusRequest toProto() {
        PFailPointTriggerMode mode = new PFailPointTriggerMode();
        if (isEnable) {
            if (nTimes != null) {
                mode.mode = FailPointTriggerModeType.ENABLE_N_TIMES;
                mode.nTimes = nTimes;
            } else if (probability != null) {
                mode.mode = FailPointTriggerModeType.PROBABILITY_ENABLE;
                mode.probability = probability.doubleValue();
            } else {
                mode.mode = FailPointTriggerModeType.ENABLE;
            }
        } else {
            mode.mode = FailPointTriggerModeType.DISABLE;
        }
        PUpdateFailPointStatusRequest request = new PUpdateFailPointStatusRequest();
        request.failPointName = name;
        request.triggerMode = mode;
        return request;
    }

    public TUpdateFailPointRequest toThrift() {
        TUpdateFailPointRequest request = new TUpdateFailPointRequest();
        request.setName(name);
        request.setIs_enable(isEnable);
        if (nTimes != null) {
            request.setTimes(nTimes);
        }
        if (probability != null) {
            request.setProbability(probability);
        }
        return request;
    }

    public PFailPointTriggerMode getFailPointMode() {
        PFailPointTriggerMode mode = new PFailPointTriggerMode();
        if (isEnable) {
            if (nTimes != null) {
                mode.mode = FailPointTriggerModeType.ENABLE_N_TIMES;
                mode.nTimes = nTimes;
            } else if (probability != null) {
                mode.mode = FailPointTriggerModeType.PROBABILITY_ENABLE;
                mode.probability = probability.doubleValue();
            } else {
                mode.mode = FailPointTriggerModeType.ENABLE;
            }
        } else {
            mode.mode = FailPointTriggerModeType.DISABLE;
        }
        return mode;
    }

    public TriggerPolicy getTriggerPolicy() {
        if (nTimes != null) {
            return TriggerPolicy.timesPolicy(nTimes);
        }
        if (probability != null) {
            return TriggerPolicy.probabilityPolicy(probability);
        }
        return TriggerPolicy.enablePolicy();
    }

    public List<String> getBackends() {
        return backends;
    }

    public boolean isForFrontend() {
        return backends == null;
    }

    public boolean getIsEnable() {
        return isEnable;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitUpdateFailPointStatusStatement(this, context);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder("ADMIN ");
        if (isEnable) {
            sb.append("ENABLE");
        } else {
            sb.append("DISABLE");
        }
        sb.append(" FAILPOINT '").append(name).append("'");
        if (nTimes != null) {
            sb.append(" WITH ").append(nTimes).append(" TIMES");
        } else if (probability != null) {
            sb.append(" WITH ").append(probability).append(" PROBABILITY");
        }
        if (backends == null) {
            sb.append(" ON FRONTEND");
        } else if (!backends.isEmpty()) {
            sb.append(" ON BACKEND '").append(Joiner.on(",").join(backends)).append("'");
        }
        return sb.toString();
    }
}
