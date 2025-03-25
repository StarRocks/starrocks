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

package com.starrocks.statistic.sample;

import com.google.common.base.Preconditions;
import com.starrocks.common.Config;
import org.apache.commons.lang3.EnumUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.MessageFormat;

/**
 * Estimate the overall NDV based on sampled data
 */
public abstract class NDVEstimator {

    private static final Logger LOG = LogManager.getLogger(NDVEstimator.class);

    static NDVEstimator build() {
        NDVEstimatorDesc desc = NDVEstimatorDesc.get();
        switch (desc) {
            case DUJ1 -> {
                return new DUJ1Estimator();
            }
            case GEE -> {
                return new GEEEstimator();
            }
            case LINEAR -> {
                return new LinearEstimator();
            }
            case POLYNOMIAL -> {
                return new PolynomialEstimator();
            }
        }
        throw new IllegalArgumentException("unknown estimator: " + desc);
    }

    public enum NDVEstimatorDesc {
        DUJ1,
        GEE,
        LINEAR,
        POLYNOMIAL;

        public static NDVEstimatorDesc defaultConfig() {
            return DUJ1;
        }

        public static NDVEstimatorDesc get() {
            NDVEstimatorDesc desc =
                    EnumUtils.getEnumIgnoreCase(NDVEstimatorDesc.class, Config.statistics_sample_ndv_estimator);
            if (desc != null) {
                return desc;
            }
            LOG.warn("unknown NDV estimator {}, fallback to default", Config.statistics_sample_ndv_estimator);
            return defaultConfig();
        }
    }

    /**
     * Generate the SQL query which implement the estimator algorithm
     *
     * @return sql
     */
    abstract String generateQuery(double sampleRatio);

    /**
     * From PostgreSQL: n*d / (n - f1 + f1*n/N)
     * (https://github.com/postgres/postgres/blob/master/src/backend/commands/analyze.c)
     * and paper: ESTIMATING THE NUMBER OF CLASSES IN A FINITE POPULATION
     * (http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.93.8637&rep=rep1&type=pdf)
     * sample_row * count_distinct / ( sample_row - once_count + once_count * sample_row / total_row)
     */
    static class DUJ1Estimator extends NDVEstimator {

        @Override
        String generateQuery(double sampleRatio) {
            Preconditions.checkArgument(sampleRatio <= 1.0, "invalid sample ratio: " + sampleRatio);
            String sampleRows = "SUM(t1.count)";
            String onceCount = "SUM(IF(t1.count = 1, 1, 0))";
            String countDistinct = "COUNT(1)";
            String fn = MessageFormat.format("{0} * {1} / ({0} - {2} + {2} * {3})", sampleRows,
                    countDistinct, onceCount, String.valueOf(sampleRatio));
            return "IFNULL(" + fn + ", COUNT(1))";
        }
    }

    /**
     * sampleDistinct / sampleRatio
     */
    static class LinearEstimator extends NDVEstimator {

        @Override
        String generateQuery(double sampleRatio) {
            return MessageFormat.format("COUNT(1) / {0}", sampleRatio);
        }
    }

    /**
     * sampleDistinct / (1 - (1-sampleRatio)^3)
     */
    static class PolynomialEstimator extends NDVEstimator {

        @Override
        String generateQuery(double sampleRatio) {
            final int K = 3;
            double ratio = 1 - Math.pow(1 - sampleRatio, K);
            return "COUNT(1) / " + ratio;
        }
    }

    /**
     * GEE (Guaranteed-Error Estimator) https://dl.acm.org/doi/pdf/10.1145/335168.335230
     * D_GEE = d + (sqrt(n / r) - 1) * f1
     * D_GEE is the estimated number of distinct values.
     * n is the total number of rows in the table.
     * r is the size of the random sample.
     * d is the number of distinct values observed in the sample.
     * f1 is the number of values that appear exactly once in the sample.
     */
    static class GEEEstimator extends NDVEstimator {

        @Override
        String generateQuery(double sampleRatio) {
            String onceCount = "SUM(IF(t1.count = 1, 1, 0))";
            String sampleNdv = "COUNT(1)";
            double inverseRatio = 1 / sampleRatio;
            double fold = Math.sqrt(inverseRatio) - 1;
            String fn = MessageFormat.format("{0} + {1} * {2}", sampleNdv, fold, onceCount);
            return "IFNULL(" + fn + ", COUNT(1))";
        }
    }

}
