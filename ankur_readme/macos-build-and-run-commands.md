# Iceberg on macOS — Build & Run Commands

Quick command reference for this machine (Apple Silicon, bash). For background and the deeper
walkthrough, see `iceberg-build-and-actions-guide.md` in this folder.

As of 2026-10-10: Gradle 8.14.5 (wrapper), default engines Flink 2.3 and Spark 4.2.

---

## 1. JDK setup (one time)

Iceberg only builds with **JDK 17 or 21** (`build.gradle` fails on anything else). Homebrew's
unversioned `openjdk` is 24, so point at the versioned `openjdk@21` instead.

```bash
brew install openjdk@21          # already installed on this machine
```

In `~/.bash_profile` (already done; the old JDK 24 lines are commented out):

```bash
export JAVA_HOME="/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
```

Do **not** use `/opt/homebrew/opt/openjdk/...`. It always points to Homebrew's newest JDK.

Alternative that survives Homebrew path changes (uses the Temurin 21 also installed here):

```bash
export JAVA_HOME="$(/usr/libexec/java_home -v 21)"
```

Verify:

```bash
source ~/.bash_profile
echo $JAVA_HOME                  # .../openjdk@21/...
java --version                   # openjdk 21.x
```

Optional: pin Gradle to 21 regardless of `JAVA_HOME` by adding this to `~/.gradle/gradle.properties`:

```properties
org.gradle.java.home=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
```

---

## 2. First build

```bash
cd ~/dev/iceberg
./gradlew --stop                              # kill daemons started under an old JDK
./gradlew --version                           # should report JVM 21
./gradlew build -x test -x integrationTest    # compile everything, no tests
```

The first run downloads every dependency and is slow; later runs use the build cache.

Faster when you only touch core modules (skips the Spark, Flink and Kafka modules):

```bash
./gradlew build -x test -x integrationTest -DsparkVersions= -DflinkVersions= -DkafkaVersions=
```

---

## 3. Everyday build commands

| Goal | Command |
| --- | --- |
| List all module paths | `./gradlew projects` |
| List tasks for a module | `./gradlew :iceberg-core:tasks` |
| Compile one module | `./gradlew :iceberg-core:compileJava` |
| Build one module, no tests | `./gradlew :iceberg-core:build -x test` |
| Format code (run before every commit) | `./gradlew spotlessApply` |
| Check formatting only | `./gradlew spotlessCheck` |
| Clean one module | `./gradlew :iceberg-core:clean` |
| Build the runtime jar for Flink 2.3 | `./gradlew :iceberg-flink:iceberg-flink-runtime-2.3:shadowJar` |

Jars land in `<module>/build/libs/`.

---

## 4. Running tests

```bash
# All tests in one module
./gradlew :iceberg-core:test

# One test class
./gradlew :iceberg-core:test --tests "org.apache.iceberg.TestSnapshot"

# One test method
./gradlew :iceberg-core:test --tests "org.apache.iceberg.TestSnapshot.testAppendFilesFromTable"

# A whole package
./gradlew :iceberg-core:test --tests "org.apache.iceberg.actions.*"

# Show stdout/stack traces and ignore up-to-date checks
./gradlew :iceberg-core:test --tests "org.apache.iceberg.TestSnapshot" --info --rerun-tasks
```

`--tests` takes a fully-qualified class name (wildcards like `*Merge*` allowed). If it matches
nothing, Gradle fails with `No tests found for given includes`.

HTML test reports: `<module>/build/reports/tests/test/index.html`

```bash
open core/build/reports/tests/test/index.html
```

Avoid a bare `./gradlew test` because it runs every module and takes hours.

---

## 5. Engine modules (Flink / Spark)

Only the **default** engine version is part of the build unless you ask for others.

| Engine | Default | Known versions | Selector flag |
| --- | --- | --- | --- |
| Flink | 2.3 | 1.20, 2.2, 2.3 | `-DflinkVersions=` |
| Spark | 4.2 | 3.5, 4.0, 4.1, 4.2 | `-DsparkVersions=` |
| Kafka Connect | 3 | 3 | `-DkafkaVersions=` |

### Flink (this branch: upsert / delete-vector work)

```bash
# Build Flink 2.3 module, no tests
./gradlew :iceberg-flink:iceberg-flink-2.3:build -x test

# Run Flink 2.3 tests (single class shown)
./gradlew :iceberg-flink:iceberg-flink-2.3:test --tests "org.apache.iceberg.flink.sink.TestIcebergSink"

# Include older Flink versions too
./gradlew -DflinkVersions=1.20,2.2,2.3 :iceberg-flink:iceberg-flink-1.20:build -x test
```

Source for each version lives in `flink/v<version>/flink/`.

### Spark

```bash
# Spark 4.2 (Scala 2.13)
./gradlew :iceberg-spark:iceberg-spark-4.2_2.13:build -x test
./gradlew :iceberg-spark:iceberg-spark-4.2_2.13:test --tests "org.apache.iceberg.spark.sql.TestSelect"
./gradlew :iceberg-spark:iceberg-spark-extensions-4.2_2.13:test --tests "org.apache.iceberg.spark.extensions.TestMergeOnReadDelete"

# Spark 3.5 (Scala 2.12 by default)
./gradlew -DsparkVersions=3.5 :iceberg-spark:iceberg-spark-3.5_2.12:build -x test
```

Run `./gradlew projects` with the same `-D...Versions=` flags to see the exact paths.

---

## 6. Docker (integration tests only)

Unit tests in `core`, `api` and `data` don't need Docker. Hive metastore, AWS, and other
Testcontainers-based tests do.

```bash
brew install --cask docker
open -a Docker
# If tests fail with "Could not find a valid Docker environment":
sudo ln -s $HOME/.docker/run/docker.sock /var/run/docker.sock
```

---

## 7. Troubleshooting

| Symptom | Fix |
| --- | --- |
| `This build must be run with JDK 17 or 21 but was executed with JDK 24` | Check `echo $JAVA_HOME` and `java --version`, run `source ~/.bash_profile`, then `./gradlew --stop` |
| Still on old JDK after fixing `JAVA_HOME` | Old Gradle daemon: `./gradlew --stop` |
| IntelliJ uses wrong JDK | `File → Project Structure → SDK` and `Settings → Build Tools → Gradle → Gradle JVM` set to 21 |
| Build runs out of memory or the laptop slows to a crawl | `./gradlew <task> --max-workers=4` |
| Module path not found (e.g. `iceberg-flink-1.20`) | Add the version flag: `-DflinkVersions=1.20` |
| Spotless failure in CI | `./gradlew spotlessApply` and commit the result |
