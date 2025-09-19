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

package com.starrocks.sql.formatter;

public class FormatOptions {
    // when you want to get the full string of a functionCallExpr set it true
    // when you just want to a function name as its alias set it false
    private boolean addFunctionDbName = true;

    // when you want to get an expr name with backquote set it true
    // when you just want to get the real expr name set it false
    private boolean withBackquote = true;

    private boolean hideCredential = true;

    private boolean columnSimplifyTableName = true;

    private boolean columnWithTableName = true;

    private boolean enableDigest = true;

    private boolean enableNewLine = true;

    private boolean enableMassiveExpr = true;

    private boolean printActualSelectItem = true;

    private boolean printLevelCompound = true;

    private boolean enableHints = true;

    private FormatOptions() {}

    public static FormatOptions allEnable() {
        return new FormatOptions();
    }

    public boolean isColumnSimplifyTableName() {
        return columnSimplifyTableName;
    }

    public boolean isColumnWithTableName() {
        return columnWithTableName;
    }

    public boolean isEnableDigest() {
        return enableDigest;
    }

    public boolean isAddFunctionDbName() {
        return addFunctionDbName;
    }

    public boolean isWithBackquote() {
        return withBackquote;
    }

    public boolean isHideCredential() {
        return hideCredential;
    }

    public boolean isEnableMassiveExpr() {
        return enableMassiveExpr;
    }

    public boolean isPrintActualSelectItem() {
        return printActualSelectItem;
    }

    public boolean isPrintLevelCompound() {
        return printLevelCompound;
    }

    public boolean isEnableHints() {
        return enableHints;
    }

    public FormatOptions setColumnSimplifyTableName(boolean columnSimplifyTableName) {
        this.columnSimplifyTableName = columnSimplifyTableName;
        return this;
    }

    public FormatOptions setColumnWithTableName(boolean columnWithTableName) {
        this.columnWithTableName = columnWithTableName;
        return this;
    }

    public FormatOptions setEnableDigest(boolean enableDigest) {
        this.enableDigest = enableDigest;
        return this;
    }

    public FormatOptions setAddFunctionDbName(boolean addFunctionDbName) {
        this.addFunctionDbName = addFunctionDbName;
        return this;
    }

    public FormatOptions setWithBackquote(boolean withBackquote) {
        this.withBackquote = withBackquote;
        return this;
    }

    public FormatOptions setHideCredential(boolean hideCredential) {
        this.hideCredential = hideCredential;
        return this;
    }

    public FormatOptions setEnableNewLine(boolean enableNewLine) {
        this.enableNewLine = enableNewLine;
        return this;
    }

    public FormatOptions setEnableMassiveExpr(boolean enableMassiveExpr) {
        this.enableMassiveExpr = enableMassiveExpr;
        return this;
    }

    public FormatOptions setPrintActualSelectItem(boolean printActualSelectItem) {
        this.printActualSelectItem = printActualSelectItem;
        return this;
    }

    public FormatOptions setPrintLevelCompound(boolean printLevelCompound) {
        this.printLevelCompound = printLevelCompound;
        return this;
    }

    public FormatOptions setEnableHints(boolean enableHints) {
        this.enableHints = enableHints;
        return this;
    }

    public String newLine() {
        return enableNewLine ? "\n" : " ";
    }
}
