# HLL(HyperLogLog)

## description

VARCHAR(M)

Variable length string, M represents the length of variable length string. The range of M is 1-16385

user does not need to specify the length and default value. The length is controlled within the system according to the degree of data aggregation.

And HLL columns can only be queried or used through the matching hll_union_agg, hll_raw_agg, hll_cardinality, and hll_hash]

## keyword

HLL,HYPERLOGLOG
