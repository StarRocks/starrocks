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

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.parser.NodePosition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OptimizeRange implements ParseNode, Writable {

    @SerializedName(value = "start")
    private StringLiteral start;

    @SerializedName(value = "end")
    private StringLiteral end;

    private final NodePosition pos;
    
    /**
     * Default constructor with ZERO position
     */
    public OptimizeRange() {
        this.pos = NodePosition.ZERO;
    }
    
    /**
     * Constructor with node position
     * 
     * @param pos Node position information
     */
    public OptimizeRange(NodePosition pos) {
        this.pos = pos != null ? pos : NodePosition.ZERO;
    }
    
    /**
     * Constructor with start, end and position
     * 
     * @param start Start value as StringLiteral
     * @param end End value as StringLiteral
     * @param pos Node position information
     */
    public OptimizeRange(StringLiteral start, StringLiteral end, NodePosition pos) {
        this.start = start;
        this.end = end;
        this.pos = pos != null ? pos : NodePosition.ZERO;
    }
    
    /**
     * Get start value of range
     * 
     * @return Start value as StringLiteral
     */
    public StringLiteral getStart() {
        return start;
    }
    
    /**
     * Set start value of range
     * 
     * @param start Start value as StringLiteral
     */
    public void setStart(StringLiteral start) {
        this.start = start;
    }
    
    /**
     * Get end value of range
     * 
     * @return End value as StringLiteral
     */
    public StringLiteral getEnd() {
        return end;
    }
    
    /**
     * Set end value of range
     * 
     * @param end End value as StringLiteral
     */
    public void setEnd(StringLiteral end) {
        this.end = end;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(" BETWEEN ");
        if (start != null) {
            sb.append(start.getStringValue());
        }
        sb.append(" AND ");
        if (end != null) {
            sb.append(end.getStringValue());
        }
        return sb.toString();
    }
    
    /**
     * Write object to output stream
     * 
     * @param out DataOutput to write to
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    /**
     * Read OptimizeRange from input stream
     * 
     * @param in DataInput to read from
     * @return New OptimizeRange object
     * @throws IOException if an I/O error occurs
     */
    public static OptimizeRange read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, OptimizeRange.class);
    }
}
