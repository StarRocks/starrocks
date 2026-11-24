# FE Metadata Persistence

## Intent

Keep Image V2 and Image V3 aligned when FE metadata evolves. Adding a Java field with
`@SerializedName` can make it persist through V2's Gson path, but it does not add the field to
V3's protobuf schema or converters. An omitted V3 field can compile successfully and silently
restore as zero, false, null, or an empty collection.

New edit-log operations must also follow the [FE edit-log policy](fe-edit-log.md)
and use protobuf payloads. Journal serialization and image serialization are separate
contracts; verify both when a change affects both paths.

## Applies To

- Changes to image-persisted FE state, including inherited fields, nested objects and collections,
  persisted subtypes and enums, and serialization or recovery hooks.
- All FE metadata modules.
- New and already-open PRs, rebases, backports, and code syncs. A branch opened before a model's
  V3 implementation landed must still preserve that model in both formats before it merges.

This requirement applies wherever V3 persistence exists, even if the current checkpoint or
startup path still uses V2. Rollout status does not exempt metadata changes from keeping the
existing V3 implementation current.

## Enforcement

### Implementation

1. Trace the changed state from its owning manager's image save/load path through the persisted
   model. Check `@SerializedName` fields, inherited and nested state, and values produced or
   restored by `gsonPreProcess`/`gsonPostProcess`; an annotation search alone is not a complete inventory.
2. Update the corresponding schema under
   [`fe/fe-core/src/main/proto/persist/`](../../fe/fe-core/src/main/proto/persist/) and both
   conversion directions in the same PR. Inspect `toProto`/`fromProto`,
   `fillProtoVariant`/`fillFromVariantProto`, protobuf constructors, and shared converter helpers.
   For new collections or types, also check the manager's V3 sections, dispatch, and recovery hooks.
3. Preserve existing protobuf field numbers and enum values. Use compatible additions and
   never reuse removed numbers; reserve removed field numbers and names. Define how old images
   without the new field restore, including distinctions between absent, null, and empty values.
   A constructor assignment that merely makes the code compile is insufficient if it discards
   the stored value.
4. For new persisted subtypes or enum values, update the corresponding V3 discriminator or
   enum mapping and conversion dispatch. Verify unknown-type handling follows the existing
   image reader contract.
5. Add tests that start with non-default source values and independently assert the result of
   V2 and V3 restoration. Use the owning manager's actual image save/load path where available. Cover absent
   fields in older payloads and meaningful restored behavior, such as version checks, indexes,
   or resumed jobs. Equality between two restored objects alone can hide a shared omission.

### In-flight PRs and review

- Before merging a metadata change, update the branch against the latest target branch and
  recheck the final field/type inventory against its V3 implementation. A clean rebase does not
  establish persistence completeness: an independent protobuf constructor or converter may
  need changes even when Git reports no conflict.
- Human and AI reviewers must check V3 coverage when an image-persisted model changes, even
  when the PR diff contains no `.proto` file. Compilation, wire-compatibility checks, and old
  round-trip tests using default values do not prove coverage of a new field.
- Review findings must identify the affected persisted state, its owning image path, and the
  missing schema/conversion/recovery behavior. Check the exceptions below before reporting a
  missing protobuf change.
- Describe the V3 schema/converter impact, old-image defaults, and relevant validation in the
  PR. If existing generic persistence already covers the change, identify that path and its
  verification instead of adding a redundant protobuf field.

## Verification and Handoff

Use the existing [image persistence tests](../../fe/fe-core/src/test/java/com/starrocks/persist/image/)
and tests beside the affected model as examples. Regenerate protobuf classes through the normal
build and run the affected tests on the final source after resolving branch drift. Record the
tested base/head and validation scope; earlier green CI does not validate newly merged fields.

## Exceptions

- Runtime-only fields do not need an image representation. Confirm that they are intentionally
  transient or rebuilt after load before applying this exception.
- A journal-only payload is not automatically an image entry. Trace the state its replay changes
  and verify that the owning image model already preserves that state; do not create an image
  protobuf solely because a journal DTO has a new `@SerializedName` field.
- An existing protobuf map, list, property bag, or reflective serializer may already preserve
  the new state. Verify both directions and the restored value before concluding that no schema
  or converter change is needed.

These exceptions concern the representation required for the state. They do not waive checking
the state that must survive image recovery.
