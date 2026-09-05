package com.starrocks.udf;

import org.apache.commons.math3.special.Erf;

public class NormalCdf {
    public final Double evaluate(Double mean, Double standardDeviation, Double value) {
        if (mean == null || standardDeviation == null || value == null) {
            return null;
        }
        return 0.5 * (1 + Erf.erf((value - mean) / (standardDeviation * Math.sqrt(2))));
    }
}
