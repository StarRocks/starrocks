# JDK Support Policy

## Intent

Derive the JDK versions StarRocks requires and recommends from the Java LTS calendar, not from StarRocks release numbers, so that every floor raise is announced a full cycle ahead and the required JDK is always still being built.

StarRocks has moved its JDK floor three times: JDK 8 to 11 in v3.3, 11 to 17 in v3.5, and a JDK 21 recommendation on `main` (v4.2). Each move was right and each arrived as a surprise. A schedule of StarRocks release numbers cannot fix that, because release dates are not knowable far enough ahead to promise anything. The Java LTS calendar is published years ahead, so the policy anchors to it.

The policy fixes two integers per release, the same two that `bin/start_fe.sh` and `bin/start_backend.sh` already sort JDKs by:

- `MIN_JDK_VERSION`: below it, startup fails.
- `RECOMMENDED_JDK_VERSION`: below it, startup warns that support will be removed.

What the policy adds is the dates on which those integers change, and one commitment: recommended is the next minimum.

## Applies To

- The FE JVM: every module compiled at the FE target (`fe/fe-core/pom.xml` and the sibling `fe/fe-*` modules).
- The BE and CN embedded JVM used for Java UDFs, connectors, and JNI readers (`bin/start_backend.sh`; `bin/start_cn.sh` delegates to it).
- `java-extensions/` (`java.version` in `java-extensions/pom.xml`).
- The startup checks in `bin/start_fe.sh` and `bin/start_backend.sh`.
- Container images under `docker/dockerfiles/`.
- The public JDK matrix in `docs/{en,zh,ja}/deployment/preparation/environment_configurations.md` and the JDK lines in `docs/{en,zh,ja}/deployment/preparation/deployment_prerequisites.md`.
- Release notes for any minor that changes either integer.

Artifacts that run inside a foreign JVM are governed by the Exceptions section, not by the ladder.

## The Rule

Three LTS versions are live at any time, and exactly two of them carry a promise. An LTS arrives as the **edge**, with nothing promised about it. It becomes the **recommendation** when the LTS two generations older stops being built, becomes the **minimum** when the previous LTS stops being built, and is **retired when it stops being built** itself.

```
minimum      = previous LTS end  ->  own LTS end
recommended  = the next LTS      =   the next minimum
edge         = any newer LTS, no commitment
today        = min 17, rec 21    (Sep 2025 - Oct 2027)
```

Both ends of the minimum rung are LTS end dates, which makes the invariant exact: the minimum is always the oldest LTS still being built. One version hands the floor to the next at the instant it stops shipping. There are never two minimums at once, never an interval with none, and never a version required after its last build.

A rung is therefore as long as the gap between two consecutive LTS end dates. That is nominally two years, because LTS releases land two years apart, but the published calendar gives 25, 26, and 21 months for Java 17, 21, and 25. The rung stretches or compresses; it never gaps or overlaps.

### Why LTS end dates and not GA plus a constant

A GA-anchored ladder needs a number: require an LTS at GA plus 42 months, or 48. The offset it implies is not constant, so any fixed choice tears at the seams:

| LTS | GA | LTS end | Minimum rung | Length | A fixed two-year rung would |
| --- | --- | --- | --- | --- | --- |
| 17 | Sep 2021 | Oct 2027 | Sep 2025 - Oct 2027 | 25 mo | start Oct 2025, a month late |
| 21 | Sep 2023 | Dec 2029 | Oct 2027 - Dec 2029 | 26 mo | start Dec 2027, leaving 2 months with no minimum |
| 25 | Sep 2025 | Sep 2031 | Dec 2029 - Sep 2031 | 21 mo | start Sep 2029, giving 3 months with two minimums |

Those are the two states a floor policy must never have: an interval where no version is the minimum, and an interval where two are. Aligning the rung to the previous LTS's end closes both by construction.

End anchoring also keeps the full upgrade runway. A GA-plus-42-months rule would end Java 17 support in Mar 2027, seven months before the last Temurin 17 build ships, and would need its margin re-checked every cycle in case it goes negative. And it degrades safely in the case that actually worries us: Adoptium guarantees four years per LTS and has been publishing six. If some future LTS gets only four, a GA-anchored floor would require it for months after its builds stopped. End-anchored, the unpromised edge span absorbs the shortfall while the recommended and minimum rungs keep their full length. Operators never lose notice; the project loses lead time, which is the right place for the loss to land.

The policy has no tunable parameter. The LTS cadence sets the nominal rung length, the LTS lifetime sets the rung count, and the published end dates set every boundary.

## Version States

| State | Window | Meaning |
| --- | --- | --- |
| Edge | GA until the start of its recommended rung | Any LTS newer than the recommendation: normally one, briefly two between a new LTS GA and the following shift. Not required, not recommended, not promised. It usually runs, since it is above the recommendation, but the policy claims nothing about it yet. |
| Recommended | Until the previous LTS's end date | What the Ubuntu images ship, what the docs lead with, and what the startup warning names as the incoming floor. Published as the next minimum a full rung before it becomes one. |
| Minimum | Previous LTS end date until its own | Required. The FE compile target equals it. The FE refuses to start below it with an actionable message. The BE and CN treat Java as optional, so they log an error and start; Java-dependent features are unsupported there. It holds the floor for exactly as long as it is the oldest LTS still built. |
| Retired | After its own LTS end date | Off the ladder at the moment its last build ships. Existing release branches keep working; their floor was frozen at GA. |

Only the middle two carry promises, and both are already implemented by the start scripts. The first states what the policy does not yet claim; the last, what it no longer does.

## Rules

1. **R1: Every rung boundary is an LTS end date.** An LTS is the minimum from the previous LTS's end until its own, the recommendation for the rung before that, and the edge from GA until then. Dates come from Adoptium's published end of availability, so the ladder is recomputed, never renegotiated, when a date is published or revised.
2. **R2: The minimum is the oldest LTS still being built.** This is R1 restated as the invariant to test against. At every instant exactly one LTS is the minimum, it is still downloadable, and the handover happens at the instant the previous one stops shipping. A change that would create two minimums, none, or a required version with no builds is wrong regardless of what else recommends it.
3. **R3: Recommended is the next minimum.** Publishing a recommendation commits the project to making that version the floor at the next shift. This is what lets the deprecation warning name a concrete version a full rung ahead, and why the container images ship the recommended JDK rather than the minimum one. A version may not be published as recommended until it is green in CI.
4. **R4: A rung is as long as the gap between two LTS ends.** Nominally two years, because LTS releases land two years apart, but the published calendar gives 25, 26, and 21 months for Java 17, 21, and 25. The rung stretches or compresses; the boundaries do not move. The next three shifts are Oct 2027, Dec 2029, and Sep 2031, and each falls within a quarter of a new LTS GA, which is the cross-check that the calendar has not changed under the policy.
5. **R5: A release resolves against the timeline at GA, then freezes.** A minor takes the pair in force on its GA date and keeps it for the life of its branch, because a patch release must never stop a running cluster from restarting. Support for a newer LTS may be backported when CI on that branch proves it; nothing that raises a floor may be. Raising the FE compile target is a minor-release change by definition.
6. **R6: A slip past a shift date does not cost the notice period.** A release that GAs after a shift date takes the new pair only if the previous release already warned at startup about the incoming floor. If it did not, the release keeps the old pair and the shift moves to the next minor. A full rung of notice is the promise; a late release is not a reason to shorten it. A short LTS life is not either: if an LTS gets less than six years, the unpromised edge span absorbs the difference, and in the limit it arrives already recommended.
7. **R7: Extended availability does not extend support.** Adoptium builds Java 8 until at least Dec 2030 and Java 11 until at least Oct 2027. Neither end date is earlier than the end date of the LTS that follows it, so neither can hold an end-date-bounded rung. Such an LTS leaves the ladder at the GA of the LTS three generations newer, the moment it stops being one of the three newest LTS releases. That retired Java 8 at Java 21's GA (Sep 2023) and Java 11 at Java 25's GA (Sep 2025) while Adoptium still built both, and it is why the current rung began in Sep 2025 rather than at Java 11's nominal end. The shift chain is monotonic: a shift is never dated earlier than the shift before it. Revisions follow the same rule: a shift date is fixed at the end-of-availability date first published for the LTS, and a later extension by Adoptium does not move it. Adoptium publishes its dates as "at least", so they do not move earlier. Non-LTS releases never appear on the ladder at all: never required, never blocking, never in the CI matrix, and a bug report against one needs a reproduction on a supported LTS.
8. **R8: Artifacts that run in someone else's JVM are out of scope.** Anything loaded into a foreign runtime targets the lowest JDK its host ecosystem still supports, Java 8 today, and moves only when that ecosystem moves. Each declares its target beside a comment naming the host that pins it and the oldest host version StarRocks documents as supported. The target moves when that oldest documented host version no longer runs on Java 8, and the PR that drops support for that host version raises the target in the same change. The current list is in Exceptions.

## Shift Chain

Each row is a date on which the two integers move and the values that take effect from it. This is a projection of the rules, not an independent schedule. The last column records which minors happened to land in each rung: observed for the past, unknowable ahead, and in no case load-bearing.

| Shift | Minimum | Recommended | Edge LTS | Triggered by | Holds for | StarRocks at the time |
| --- | --- | --- | --- | --- | --- | --- |
| Sep 2021 | 8 | 11 | 17 | Java 17 GA | 24 mo | v2.5 - v3.1 required 8, recommended 11 (matches) |
| Sep 2023 | 11 | 17 | 21 | Java 21 GA; Java 8 leaves the ladder under R7 | 24 mo | v3.2 (Nov 2023) still required 8; v3.3 (Jun 2024) took the 11 floor, 9 months late |
| **Sep 2025** | **17** | **21** | **25** | Java 25 GA; Java 11 leaves the ladder under R7 | 25 mo | v3.5 (Jun 2025) took the 17 floor, 3 months early; v3.5 - v4.1 require 17 with no separate recommendation; `main` (v4.2) carries (17, 21) and the startup warning (matches) |
| Oct 2027 | 21 | 25 | 29 | Java 17 stops being built | 26 mo | floor raise: first minor to GA after this date, subject to R6 |
| Dec 2029 | 25 | 29 | 33 | Java 21 stops being built | 21 mo | floor raise: first minor to GA after this date, subject to R6 |
| Sep 2031 | 29 | 33 | 37 | Java 25 stops being built | - | subject to Java 29 and 33 GAing on the two-year cadence |

Applied backwards the chain lands close to what the project actually did, and closer with each cycle, which is the case for trusting it forwards. Java 8 held the floor legitimately until the Sep 2023 shift, and v3.2 was two months past it. The Java 11 floor arrived nine months late, in v3.3. The Java 17 floor arrived three months early, in v3.5. `main` today carries exactly the pair the rule prescribes, which is the first time the project has been in front of its own JDK calendar rather than behind it.

## Enforcement

What upholds the policy today:

- `bin/start_fe.sh` (`MIN_JDK_VERSION=17`, `RECOMMENDED_JDK_VERSION=21`) fails startup below the minimum and warns below the recommendation, naming the recommended version in both messages. `bin/start_backend.sh` carries the same pair and prints the same two messages for the BE and CN embedded JVM, and, because the BE treats Java as optional (a missing `JAVA_HOME` is also only a warning), logs an error below the minimum instead of exiting.
- The FE compile target is 17 in `fe/fe-core/pom.xml` and the sibling `fe/fe-*` modules, and `java-extensions/pom.xml` sets `java.version` to 17. Both equal the minimum, as the Minimum state requires.
- The Ubuntu images (`docker/dockerfiles/{fe,be,allin1}/*-ubuntu.Dockerfile`) install OpenJDK 21, the recommended version.
- The public matrix in `docs/en/deployment/preparation/environment_configurations.md` (mirrored in `zh` and `ja`) lists the minimum and recommended JDK per StarRocks version and points at the next scheduled change, and the release notes for v3.3.0 and v3.5.0 state each floor raise.
- The user-facing statement of this policy is the JDK Support Policy section of `docs/en/developers/versions.md` (mirrored in `zh` and `ja`): the two integers, when the minimum changes, the schedule, and the foreign-JVM components. Keep it in sync with this page; it carries no repo paths or rule numbers.

Review rules derived from R1 to R8:

- A PR that raises `MIN_JDK_VERSION` or the FE compile target is a minor-release change. It lands on `main` before the release branch is cut and is never backported (R5).
- A PR that raises `RECOMMENDED_JDK_VERSION` must point at the next minimum in the shift chain, and that JDK must already be green in CI (R3).
- A PR that adds JDK-specific behavior (for example `--add-opens` flags or JVM options) must work on both the minimum and the recommended JDK.
- A PR that touches a foreign-JVM artifact keeps its Java 8 target and the comment that pins it, or moves the target together with the host ecosystem the comment names (R8).
- A JDK bug report that reproduces only on a non-LTS release is closed pending a reproduction on a supported LTS (R7).

Checklist for a shift, carried by whichever minor GAs first after the shift date:

1. Confirm the previous minor already shipped the startup warning naming the new minimum (R6). If it did not, defer the shift to the next minor.
2. Raise the FE compile target in `fe/fe-core/pom.xml` and the sibling `fe/fe-*` modules, and `java.version` in `java-extensions/pom.xml`, to the new minimum.
3. Set the pair in `bin/start_fe.sh` and `bin/start_backend.sh` to (new minimum, new recommended).
4. Move the Ubuntu images to the new recommended JDK.
5. Add the row to the JDK matrix in `docs/en`, `docs/zh`, and `docs/ja`, and update the JDK lines in the deployment prerequisites pages.
6. State the raise in the release notes and the upgrade notes for the minor.

Scheduled actions this policy creates in the repo. New non-LTS releases need none (R7), and a new LTS GA needs none either: it is a cross-check that the calendar has not moved, not a task.

| When | Action |
| --- | --- |
| Before Oct 2027 | Get JDK 25 green in CI. It becomes the recommendation at the first shift, and a recommendation is a commitment (R3), so it has to be provable before the shift rather than after. This is the only outstanding action in the current rung: the pair on `main` is correct and the warning naming 21 already ships, so R6's notice is satisfied. |
| Oct 2027 shift | Raise the floor to 21 in one release: compile target 17 to 21, the pair to (21, 25), images to 25, the docs matrix, and the release notes. |
| Before Dec 2029 | Get JDK 29 green in CI and ship the warning naming 25. Both earlier in the cycle than last time: the rung that follows is 21 months, the shortest in the published calendar, so there is less room between proving the incoming LTS and requiring it. |
| Dec 2029 shift | Raise the floor to 25: compile target 21 to 25, the pair to (25, 29), images to 29. |
| Sep 2031 shift | Raise the floor to 29: compile target 25 to 29, the pair to (29, 33), images to 33. |

## Exceptions

- **Foreign-JVM artifacts (R8).** These compile at Java 8 and move only when the oldest documented host version no longer runs on Java 8. Each must keep a comment beside the target naming the host that pins it and that oldest host version:
  - `fe/plugin/spark-dpp/pom.xml`: external Spark clusters. Comment present.
  - `fe/plugin/hive-udf/pom.xml`: UDF runtime environments. Comment present.
  - `fe/fe-utils/pom.xml`: a dependency of both plugins above, so it inherits their target. No comment yet.
  - `format-sdk/pom.xml`: consumers of the format SDK. No comment yet.
  - `fs_brokers/apache_hdfs_broker/src/pom.xml`: the broker JVM. No comment yet.
- **Non-LTS Java releases** are never on the ladder (R7). They are not required, not blocking, and not in the CI matrix.
- **Release branches** keep the pair frozen at their GA (R5). Backporting support for a newer LTS is allowed once CI on that branch proves it; backporting anything that raises a floor is not.
- **A late minor** keeps the old pair when taking the new one would cut the notice period short (R6).

## References

1. Eclipse Temurin release roadmap: https://adoptium.net/support. Source of every LTS GA and end-of-availability date in this page, and of the two statements the policy leans on: one feature release every two years is designated LTS, and each LTS is supported for at least four years. The published windows currently run about six years, which is what makes three rungs fit.
2. Public JDK matrix: `docs/en/deployment/preparation/environment_configurations.md`. Public policy statement, release plan, and branch lifecycle: `docs/en/developers/versions.md`.

Repo state verified at `main` commit 49091c482b3 (2026-09-02). StarRocks release dates are tag dates in this repository: v3.2.0 2023-11-29, v3.3.0 2024-06-20, v3.5.0 2025-06-11, v4.0.0 2025-10-15, v4.1.0 2026-04-09; v4.2 is not yet tagged. LTS end dates are Adoptium's published "at least" dates. Java 29, 33, and 37 assume the two-year LTS cadence holds.
