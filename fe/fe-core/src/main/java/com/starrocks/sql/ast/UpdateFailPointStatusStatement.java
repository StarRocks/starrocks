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
import com.starrocks.common.Config;
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
    private boolean pause = false;
    // Snapshotted when the statement is built, not read per serialization: toThrift() is called once
    // for the local frontend and again for EVERY follower, so re-reading a mutable Config here would
    // let a concurrent ADMIN SET FRONTEND CONFIG hand different nodes different armed timeouts.
    private int pauseTimeoutSecond = 0;
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

    /**
     * ADMIN ENABLE FAILPOINT '&lt;name&gt;' WITH PAUSE: park threads that reach the failpoint until it is
     * disabled or the pause times out. Exclusive with N TIMES / PROBABILITY at the grammar level, so
     * this is a factory rather than a constructor overload (a boolean overload would collide with
     * the isEnable constructor).
     */
    public static UpdateFailPointStatusStatement pauseStatement(String name, List<String> backends,
                                                                NodePosition pos) {
        UpdateFailPointStatusStatement statement = new UpdateFailPointStatusStatement(name, true, backends, pos);
        statement.pause = true;
        statement.pauseTimeoutSecond =
                TriggerPolicy.normalizePauseTimeoutSecond(Config.failpoint_pause_timeout_second);
        return statement;
    }

    public String getName() {
        return name;
    }

    /**
     * Whether this statement arms a policy rather than removing one. NOT the same as
     * {@link #getIsEnable()}: a pause is an ENABLE statement, but it is transmitted with
     * is_enable = false so that a node predating the pause field removes the policy instead of
     * arming an ENABLE it cannot honour. Every arm-or-remove decision must go through this.
     */
    public boolean isArming() {
        return isEnable || pause;
    }

    public PUpdateFailPointStatusRequest toProto() {
        PUpdateFailPointStatusRequest request = new PUpdateFailPointStatusRequest();
        request.failPointName = name;
        request.triggerMode = getFailPointMode();
        if (pause) {
            // The discriminator rides on the REQUEST, never inside triggerMode: protobuf preserves
            // unknown fields, so a BE predating the pause would copy a nested flag into its stored
            // mode and echo it back from list_fail_point, making SHOW FAILPOINTS report PAUSE for a
            // failpoint it merely disabled.
            request.pause = true;
            request.pauseTimeoutSecond = pauseTimeoutSecond;
        }
        return request;
    }

    public TUpdateFailPointRequest toThrift() {
        TUpdateFailPointRequest request = new TUpdateFailPointRequest();
        request.setName(name);
        // A pause sends is_enable = false for the same reason the proto sends DISABLE: an FE that
        // predates the pause field then removes the policy instead of arming an ENABLE.
        request.setIs_enable(isEnable && !pause);
        if (pause) {
            request.setPause(true);
            request.setPause_timeout_second(pauseTimeoutSecond);
        }
        if (nTimes != null) {
            request.setTimes(nTimes);
        }
        if (probability != null) {
            request.setProbability(probability);
        }
        return request;
    }

    private PFailPointTriggerMode getFailPointMode() {
        PFailPointTriggerMode mode = new PFailPointTriggerMode();
        if (isEnable) {
            if (pause) {
                // DISABLE, NOT a dedicated enum value: proto2 would report an unknown enum value as
                // the default ENABLE, so a BE predating the pause field would inject the fault
                // instead of pausing. Disabling is the safe degradation. The pause flag itself is
                // set on the request by toProto().
                mode.mode = FailPointTriggerModeType.DISABLE;
            } else if (nTimes != null) {
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

    /**
     * The policy this statement arms on the local frontend. Derived from the same thrift encoding the
     * followers receive, so the leader and its followers cannot diverge -- a second decode ladder here
     * is exactly how a new trigger mode ends up honoured on one frontend and not the others.
     */
    public TriggerPolicy getTriggerPolicy() {
        return TriggerPolicy.fromThrift(toThrift());
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
        if (pause) {
            sb.append(" WITH PAUSE");
        } else if (nTimes != null) {
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
