# Legacy GEO metadata fixtures

Generated on 2026-11-18 with the unmodified production writer from StarRocks
main `60c3ba60b6261cd2d2e3b9bc994f4b733df4ea7e`, before GEO descriptors were persisted.
The writer, test generator and fixtures use the repository's Apache-2.0 license;
no external implementation or dataset is used.

`GeoMetadataPersistenceTest.testOwnerImagesAndJournal` executes the statements in
[fixtures.sql](fixtures.sql), then exports these complete owner payloads before
checking descriptor preservation:

| File | Contents |
| --- | --- |
| `local-meta.image.json` | Complete `LOCAL_META_STORE` v2 metablock, including VIEW schemas and database SQL UDFs. |
| `global-functions.image.json` | Complete `GLOBAL_FUNCTION_MGR` v2 metablock containing global SQL UDFs. |
| `create.journal` | CREATE VIEW and database/global ADD FUNCTION records, each with a short opcode and a `Text`-framed JSON payload. |
| `alter.journal` | ALTER/REPLACE VIEW records with the same journal framing. |

The image files are owner metablocks, not a whole FE startup image. The journal
records require database `geo_metadata_persistence`, ID `900001`, which the test
initializes before replay. `fixtures.sql` records the generating SQL, including
both GEO kinds, EPSG:3857, EPSG:4326, `custom:local` and DECIMAL control columns.

The baseline test deliberately fails its descriptor equality assertion after
the complete export: the old writer retains the GEO primitive but omits the descriptor.
Production classes were not modified, and no descriptor field was removed from
the output to manufacture a legacy record. The candidate test consumes these
bytes and checks load/replay, query analysis, DESCRIBE/SHOW and another checkpoint.

## Provenance

SHA-256 of the baseline build's `fe-server/target/lib/fe-core-main.jar`:

```text
8425dc65ef5ce9f90ca0ca2de946a07d3cd7410fd03696e7d53ff6cc1bb49c95
```

SHA-256 of `GeoMetadataPersistenceTest.java` used for this export:

```text
e393cb4be5f123979c4e657d8447f87a8f2426d2c94728a85d354e6bbe7c32e0
```

SHA-256 of the exported files:

```text
91efb13b0786aa83bbbfc78c99eb19962be08e1333a9f10a7d0b91ff2297e8a3  alter.journal
29146a115972e5420d97b0a34f3d0f219b4b88f21be56d6ceab1a9348ba89e02  create.journal
e7638b6dfa9b7e56eb7f34be2a8d20b7d5631cffdf7079fb4e8349d5ba75657a  fixtures.sql
f33d4020ac7b6eb49159956ef6cf41e513e31da4fe3231c8e0bd35c194168ad1  global-functions.image.json
9c495d69995c7336d03be16847bd5634bf52c59d8ebbaca440ddf8ea4cdcb5a1  local-meta.image.json
```

## Regeneration

Build the pinned baseline above with the repository's Java 21/Maven setup, then
copy only `GeoMetadataPersistenceTest.java` into that checkout. From `fe/`, run:

```bash
mvn -o -B -ntp -pl fe-server -am test \
    -Dtest=GeoMetadataPersistenceTest#testOwnerImagesAndJournal \
    -Dgeo.metadata.fixture.dir=/evidence/legacy \
    -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false
```

Expect the preservation assertion to fail after all five files are written.
Record the source SHA, writer artifact hash, generator hash and output hashes
for every regeneration; creation timestamps can change the output bytes.

Earlier local `image.2698` excerpts established that descriptorless VIEW/UDF
metadata existed, but their owner objects were truncated and their exact writer
SHA was unknown. Those excerpts are not the source of these fixtures.
