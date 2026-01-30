# Building FE with Gradle

This repository still ships a Maven build, but you can use the included Gradle scripts to produce the same artifacts, run the same checks, and execute unit tests. The Gradle configuration mirrors the Maven goals so you can migrate workflows or adapt CI jobs without rewriting the style/config rules.

## Prerequisites and environment

- **Java** – the modules in `fe-core`, `fe-parser`, `fe-type`, etc. target Java 17. Set `JAVA_HOME` accordingly and make sure `java -version` reports 17 (`JAVA_HOME` is also used by Maven-style generation scripts invoked from Gradle).
- **Protobuf & Thrift toolchains** – some modules invoke `generateProtoSources`/`generateThriftSources` via the embedded Gradle tasks and expect the CLI tools from Apache Protobuf/Thrift. Install them system-wide and expose them on `PATH`, for example:
  ```sh
  brew install protobuf thrift
  export PATH="$HOME/.local/bin:$PATH"
  ```
  If you need a specific version, download the official releases and set `PROTOC`/`THRIFT` environment variables so Gradle picks them up (the projects mostly rely on code generated under `build/generated-sources` so it must exist before compilation).
- **Python** – some build scripts still call Python helpers (`gen_build_version.py`, `gen_functions.py`). Ensure the `python` (or `python3`) command is in your `PATH`.
- **System memory** – Gradle runs code generation and JMockit-heavy tests; the default `org.gradle.jvmargs=-Xmx4g -XX:MaxMetaspaceSize=512m` (from `gradle.properties`) is usually enough, but you can bump it if you see `OutOfMemoryError`.
- **Optional tools** – install `curl`, `unzip`, etc., if not already available; they are used by some helper scripts but not enforced.

## Common Gradle workflows

1. **Download dependencies & build everything** (fastest full build):
   ```sh
   ./gradlew clean build
   ```
   This runs production compilation, test execution, checkstyle, and packaging across all modules (connectors, plugins and the FE server jar).

2. **Build a single module** – focus on `fe-core` when making FE changes:
   ```sh
   ./gradlew :fe-core:build
   ```
   Gradle will run all the required generate-sources tasks before compiling and will publish the module to the local Gradle cache.

3. **Run style checks only** – the Gradle `checkstyleMain/checkstyleTest` tasks track the Maven setup:
   ```sh
   ./gradlew :fe-core:checkstyleMain :fe-core:checkstyleTest
   ```
   This uses the shared `checkstyle.xml`, disables generated sources, and fails on violations, consistent with Maven.

4. **Execute helper sub-tasks** – you can invoke any reusable task directly:
   ```sh
   ./gradlew :fe-core:generateProtoSources
   ./gradlew :fe-core:generateThriftSources
   ./gradlew :fe-core:generateByScripts
   ```
   Bind the generated sources to `compileJava` (via `dependsOn`), so those updates happen automatically during `build`.

## Running unit tests

- **Module-level tests**: Most modules still “behave like Maven,” meaning they incorporate the same JMockit agent arguments and `fe_ut_parallel` tuning flags.
  ```sh
  ./gradlew :fe-core:test
  ```
- **Control concurrency**: the build defines a project property `fe_ut_parallel`. To run tests with a lower parallelism (or to match local resources), pass it on the command line:
  ```sh
  ./gradlew :fe-core:test -Pfe_ut_parallel=4
  ```
- **JUnit filtering**: target individual tests with `--tests`, e.g.
  ```sh
  ./gradlew :fe-core:test --tests "com.starrocks.qe.QueryPlannerTest"
  ```
- **Reuse forks**: the Gradle config mirrors Maven’s `reuseForks=false` by forcing `forkEvery = 1`. There’s no need to pass additional JVM arguments unless you tweak memory yourself.

## Troubleshooting tips

- The build generates sources under `build/generated-sources`. If a Gradle task fails because the generated files are missing, rerun the relevant `generate…` task with `--info`.
- Checkstyle uses the same `puppycrawl.version` defined in `pom.xml`, so updating Maven’s checkstyle version automatically updates Gradle. No manual sync is required.
- Some connectors rely on the `:plugin:hive-udf:shadowJar` artifact. The `fe-core` compilation explicitly depends on that shadow JAR to avoid “missing class” errors.

Feel free to reuse this file in your documentation, CI scripts, or onboarding guides; the Gradle paths stay stable even if the Maven configuration remains the canonical build reference. Let's chat if you need help wiring this into your CI pipeline. 
