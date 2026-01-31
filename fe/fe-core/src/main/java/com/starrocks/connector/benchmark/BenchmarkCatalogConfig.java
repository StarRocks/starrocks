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

package com.starrocks.connector.benchmark;

import com.starrocks.connector.exception.StarRocksConnectorException;

public class BenchmarkCatalogConfig {
    private final double scaleFactor;

    private BenchmarkCatalogConfig(double scaleFactor) {
        this.scaleFactor = scaleFactor;
    }

    public static BenchmarkCatalogConfig from(BenchmarkConfig config) {
        if (config == null) {
            throw new StarRocksConnectorException("Missing benchmark connector configuration");
        }

        String scaleString = config.getScale();
        if (scaleString == null || scaleString.isEmpty()) {
            scaleString = "1";
        }

        double scale;
        try {
            scale = Double.parseDouble(scaleString);
        } catch (NumberFormatException e) {
            throw new StarRocksConnectorException("Invalid benchmark scale factor: " + scaleString, e);
        }

        if (!(scale > 0)) {
            throw new StarRocksConnectorException("Benchmark scale factor must be greater than 0");
        }

        return new BenchmarkCatalogConfig(scale);
    }

    public double getScaleFactor() {
        return scaleFactor;
    }

}
