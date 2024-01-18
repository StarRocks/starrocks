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


package com.starrocks.cloudnative.compaction;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

public class Quantiles implements Comparable<Quantiles> {
    @SerializedName(value = "avg")
    private final double avg;
    @SerializedName(value = "p50")
    private final double p50;
    @SerializedName(value = "max")
    private final double max;

    @NotNull
    public static Quantiles compute(@NotNull Collection<Double> values) {
        if (values.isEmpty()) {
            return new Quantiles(0, 0, 0);
        }
        List<Double> sortedValues = values.stream().sorted().collect(Collectors.toList());
        int size = sortedValues.size();
        double avg = sortedValues.stream().mapToDouble(a -> a).average().orElse(0);
        double p50 = sortedValues.get(size / 2);
        double max = sortedValues.get(size - 1);
        return new Quantiles(avg, p50, max);
    }

    private Quantiles(double avg, double p50, double max) {
        this.avg = avg;
        this.p50 = p50;
        this.max = max;
    }

    public double getAvg() {
        return avg;
    }

    public double getP50() {
        return p50;
    }

    public double getMax() {
        return max;
    }

    @Override
    public int compareTo(@NotNull Quantiles o) {
        if (avg != o.avg) {
            return avg > o.avg ? 1 : -1;
        }
        if (p50 != o.p50) {
            return p50 > o.p50 ? 1 : -1;
        }
        if (max != o.max) {
            return max > o.max ? 1 : -1;
        }
        return 0;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
