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

package com.starrocks.common;

import com.starrocks.common.io.ParamsKey;
import com.starrocks.sql.analyzer.SemanticException;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

public class VectorIndexParams {

    public enum CommonIndexParamKey implements ParamsKey {
        // Vector index type, the enumeration of values refer to VectorIndexType
        INDEX_TYPE {
            private final Set<String> checkSets =
                    Arrays.stream(VectorIndexType.values()).map(type -> type.name().toUpperCase(Locale.ROOT))
                            .collect(Collectors.toSet());

            @Override
            public void check(String value) {
                if (!checkSets.contains(value.toUpperCase(Locale.ROOT))) {
                    throw new SemanticException(String.format("Value of `index_type` must in (%s)", String.join(",", checkSets)));
                }
            }
        },
        // Vector dimension
        DIM {
            @Override
            public void check(String value) {
                validateInteger(value, "DIM", 1);
            }
        },
        // Vector space metrics method, the enumeration of values refer to MetricsType
        METRIC_TYPE {
            private final Set<String> metricTypes = Arrays.stream(MetricsType.values())
                    .map(m -> m.name().toLowerCase()).collect(Collectors.toSet());

            @Override
            public void check(String value) {
                if (!metricTypes.contains(value.toLowerCase())) {
                    throw new SemanticException(String.format("Value of `METRIC_TYPE` must be in %s", metricTypes));
                }
            }
        },

        // Whether vector should be normed
        IS_VECTOR_NORMED {
            @Override
            public void check(String value) {
                if (!StringUtils.equalsIgnoreCase(value, "true") && !StringUtils.equalsIgnoreCase(value, "false")) {
                    throw new SemanticException("Value of `IS_VECTOR_NORMED` must be `true` or `false`");
                }
            }
        },

        // Threshold of row number to build index file
        INDEX_BUILD_THRESHOLD
    }

    public enum VectorIndexType {
        // Hierarchical Navigable Small World
        HNSW,

        // Inverted File with Product Quantization
        IVFPQ
    }

    public enum MetricsType {
        COSINE_SIMILARITY,
        L2_DISTANCE,
    }

    public enum IndexParamsKey implements ParamsKey {
        // For HNSW

        // the parameter "M" is a crucial construction parameter that refers to the maximum number of neighbors each node can have at the base layer,
        // which is the bottommost layer of the graph.
        M(VectorIndexType.HNSW) {
            @Override
            public void check(String value) {
                validateInteger(value, "M", 2);
            }
        },

        // EF_CONSTRUCTION is an important parameter that stands for the construction-time expansion factor.
        // This parameter controls the depth of the neighbor search for each data point during the index construction process.
        // Specifically, EF_CONSTRUCTION determines the size of the candidate list for nearest neighbors when inserting new nodes into the HNSW graph.
        EFCONSTRUCTION(VectorIndexType.HNSW) {
            @Override
            public void check(String value) {
                validateInteger(value, "EFCONSTRUCTION", 1);
            }
        },

        // For IVFPQ

        // NBITS is a key parameter that refers to the number of bits used to quantize each sub-vector. Within the context of Product Quantization (PQ),
        // NBITS determines the size of the quantization codebook, which is the number of cluster centers in each quantized subspace.
        NBITS(VectorIndexType.IVFPQ) {
            @Override
            public void check(String value) {
                try {
                    double num = Double.parseDouble(value);
                    if (num != 8) {
                        throw new SemanticException(String.format("Value of `%s` must be 8", "NBITS"));
                    }
                } catch (NumberFormatException e) {
                    throw new SemanticException(String.format("Value of `%s` must be a integer", "NBITS"));
                }
            }
        },

        // NLIST is a parameter in the IVF (Inverted File) indexing structure that represents the number of inverted lists, or equivalently,
        // the number of cluster centers (also known as visual words).
        // This parameter is set when constructing the index using the k-means clustering algorithm, with the purpose of grouping the vectors in the dataset
        // around these cluster centers.
        NLIST(VectorIndexType.IVFPQ) {
            @Override
            public void check(String value) {
                validateInteger(value, "NLIST", 1);
            }
        },

        M_IVFPQ(VectorIndexType.IVFPQ) {
            @Override
            public void check(String value) {
                validateInteger(value, "M_IVFPQ", 1);
            }
        };

        private final VectorIndexType belongVectorIndexType;

        IndexParamsKey(VectorIndexType vectorIndexType) {
            belongVectorIndexType = vectorIndexType;
        }

        public VectorIndexType getBelongVectorIndexType() {
            return belongVectorIndexType;
        }
    }

    public enum SearchParamsKey implements ParamsKey {
        // For HNSW
        // The EF_SEARCH parameter represents the size of the dynamic candidate list during the search process, meaning that during the search phase,
        // the algorithm maintains a priority queue of size ef_search. This queue is used to store the current nearest neighbor candidates and graph
        // nodes for further exploration.
        EFSEARCH(VectorIndexType.HNSW) {
            @Override
            public void check(String value) {
                validateInteger(value, "EFSEARCH", 1);
            }
        },

        // For IVFPG
        // NPROBE determines the number of Voronoi cells (or inverted lists)
        // to be accessed during the search phase.
        // In IVFPQ, the dataset is first divided into multiple Voronoi cells, and an inverted list is established for each cell. Then, for each query,
        // we first find the nearest NPROBE Voronoi cells, and then only search in the inverted lists of these cells.
        // The value of NPROBE affects the accuracy and efficiency of the search.
        NPROBE(VectorIndexType.IVFPQ) {
            @Override
            public void check(String value) {
                validateInteger(value, "NPROBE", 1);
            }
        },

        // MAX_CODES determines the maximum number of codes to be inspected during the search phase.
        // In IVFPQ, the dataset is divided into multiple Voronoi cells, and each cell has an inverted list that contains the codes of all points in the cell.
        // For each query, we first find the nearest NPROBE Voronoi cells, and then search in the inverted lists of these cells.
        // However, to control the computational complexity of the search, we usually do not inspect all codes, but only inspect the first maxCodes codes in
        // each inverted list. The value of maxCodes affects the accuracy and efficiency of the search
        MAX_CODES(VectorIndexType.IVFPQ) {
            @Override
            public void check(String value) {
                validateInteger(value, "MAX_CODES", 0);
            }
        },

        // SCAN_TABLE_THRESHOLD parameter is used to control the number of entries that are scanned in the lookup table during the search process.
        // The lookup table is a data structure that stores precomputed distances between the query and the centroids of the quantization cells.
        // During the search process, the algorithm scans the lookup table to find the nearest centroids to the query.
        // The SCAN_TABLE_THRESHOLD parameter determines the maximum number of entries that the algorithm will scan in the lookup table.
        // By adjusting this parameter, one can control the balance between search accuracy and efficiency.
        SCAN_TABLE_THRESHOLD(VectorIndexType.IVFPQ) {
            @Override
            public void check(String value) {
                validateInteger(value, "SCAN_TABLE_THRESHOLD", 0);
            }
        },

        // POLYSEMOUS_HT is a parameter related to the Polysemous Coding technique.
        // Polysemous Coding is a technique used to balance the trade-off between recall and precision in large-scale search problems.
        // It is based on the idea that some codewords (or quantization cells) can be associated with multiple meanings (hence the term "polysemous"),
        // which can be exploited to improve the search performance.
        //
        // The POLYSEMOUS_HT parameter is a threshold used in the Polysemous Coding technique. It determines the number of "hamming thresholds"
        // to be used in the search process. The hamming threshold is a measure of similarity between two binary codes: the lower the hamming distance,
        // the more similar the codes are.
        POLYSEMOUS_HT(VectorIndexType.IVFPQ) {
            @Override
            public void check(String value) {
                validateInteger(value, "POLYSEMOUS_HT", 0);
            }
        },

        // The RANGE_SEARCH_CONFIDENCE parameter determines the confidence level of the range search in IVFPQ (Inverted File with Product Quantization).
        // We have developed our own index search algorithm based on the triangle inequality. Adjusting this parameter allows us to control the
        // performance and accuracy of the range search.
        RANGE_SEARCH_CONFIDENCE(VectorIndexType.IVFPQ) {
            @Override
            public void check(String value) {
                validateDouble(value, "RANGE_SEARCH_CONFIDENCE", 0.0, 1.0);
            }
        };

        private VectorIndexType belongVectorIndexType = null;

        SearchParamsKey(VectorIndexType vectorIndexType) {
            belongVectorIndexType = vectorIndexType;
        }

        public VectorIndexType getBelongVectorIndexType() {
            return belongVectorIndexType;
        }
    }

    private static void validateInteger(String value, String key, Integer min) {
        try {
            int num = Integer.parseInt(value);
            if (min != null && num < min) {
                throw new SemanticException(String.format("Value of `%s` must be >= %d", key, min));
            }
        } catch (NumberFormatException e) {
            throw new SemanticException(String.format("Value of `%s` must be a integer", key));
        }
    }

    private static void validateDouble(String value, String key, Double min, Double max) {
        try {
            double num = Double.parseDouble(value);
            if (min != null && num < min) {
                throw new SemanticException(String.format("Value of `%s` must be >= %f", key, min));
            }
            if (max != null && num > max) {
                throw new SemanticException(String.format("Value of `%s` must be <= %f", key, max));
            }
        } catch (NumberFormatException e) {
            throw new SemanticException(String.format("Value of `%s` must be a double", key));
        }
    }

}
