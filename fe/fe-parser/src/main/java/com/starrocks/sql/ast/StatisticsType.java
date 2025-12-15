package com.starrocks.sql.ast;

// used to record statistics type for multi-columns.
// For version compatibility, single-column statistics are not recorded StatisticsType
public enum StatisticsType {
    // for single column statistics
    COMMON,
    // for single column histogram
    HISTOGRAM,
    // for multi-column combined ndv
    MCDISTINCT
}
