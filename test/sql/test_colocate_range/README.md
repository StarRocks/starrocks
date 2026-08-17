## test_colocate_range tests

Colocate-checker SQL integration tests. T/ files exist; R/ recordings
are absent — they require a real BE in shared-data mode and are recorded via TSP.

The fail-closed-then-recover scenarios (Level-1 split observed before scanner
converges) need a millisecond-scale pause that the scanner does not currently
expose, so those cases are covered by FE unit tests
(`ColocateCheckerTest`) instead and not duplicated here.
