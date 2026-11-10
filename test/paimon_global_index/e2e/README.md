# Paimon Global Index SQL regression runner

Use a disposable StarRocks cluster built from the candidate source. Start one FE
and two BEs. Mount the generated fixture warehouse at the same absolute path
in all three containers. Record the source and artifact identities, Docker image
IDs, container configuration, and the two live BEs before running SQL. A public
image without the PR's native Paimon Global Index scanner cannot establish
distributed-path coverage.

Generate the data with Java 17, Paimon Bundle 2.0.0, and the Hadoop client
API/runtime and SLF4J JARs. The warehouse directory must be new.

```sh
java -cp "$PAIMON_FIXTURE_CLASSPATH" PaimonIndexFixture.java \
  /path/to/new-warehouse /path/to/scratch
```

Expose the isolated FE's MySQL port on loopback. The runner uses the standard
MySQL client and expects ROOT access to this disposable instance.

```sh
python3 run_sql_matrix.py \
  --port 19030 \
  --warehouse /warehouse \
  --evidence /path/to/new-evidence-directory
```

The runner saves every query, result, EXPLAIN, error, and a JSON verdict. It
checks row equivalence under stages 0, 1, and 2. It requires EXPLAIN to report
`DISTRIBUTED INDEX: applied` for the healthy-table stage 2 case A03.
The empty result in C02 may be pruned before index execution. Its EXPLAIN
distinguishes an applied empty bitmap from an earlier empty result.
For A01 and A02 stages 1 and 2, `TRACE VALUES EXTERNAL` must record that
historical encoding was incompatible. Keep FE/BE logs or profiles to confirm
actual BE placement. The runner records backend status and requires two live BEs.

The SQL runner covers A01, A02, A03, A06, A07, and the C02 empty, range and
NULL cases. Use the separate access-controller tests for injected row and masking
policies. Full Ranger or cancellation testing requires a separately configured
cluster. Report those cases separately in the acceptance report.
