package com.starrocks.udf;

import org.apache.commons.math3.special.Erf;

/**
 * Computes the cumulative distribution function (CDF) of a normal distribution.
 * This UDF takes the mean, standard deviation, and a value as inputs and returns the corresponding CDF value.
 */
public class NormalCdf {
    public final Double evaluate(Double mean, Double standardDeviation, Double value) {
        if (mean == null || standardDeviation == null || value == null) {
            return null;
        }
        return 0.5 * (1 + Erf.erf((value - mean) / (standardDeviation * Math.sqrt(2))));
    }
}
