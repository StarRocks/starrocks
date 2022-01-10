// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.alter.AlterOpType;
import com.starrocks.common.AnalysisException;
import org.apache.parquet.Strings;

// clause which is used to swap table
// eg:
// ALTER TABLE tbl SWAP WITH TABLE tbl2;
public class SwapTableClause extends AlterTableClause {
    private final String tblName;

    public SwapTableClause(String tblName) {
        super(AlterOpType.SWAP);
        this.tblName = tblName;
    }

    public String getTblName() {
        return tblName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (Strings.isNullOrEmpty(tblName)) {
            throw new AnalysisException("No table specified");
        }
    }

    @Override
    public String toSql() {
        return "SWAP WITH TABLE " + tblName;
    }

    @Override
    public String toString() {
        return toSql();
    }
}
