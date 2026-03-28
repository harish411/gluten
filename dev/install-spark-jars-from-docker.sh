#!/usr/bin/env bash
# Install Amazon Spark/Hive/Parquet jars from the local jars/ folder into a Maven local repository.
#
# Usage: ./dev/install-spark-jars-from-docker.sh [--repo PATH]
#
# Options:
#   --repo PATH   Path to the Maven local repository (default: ~/.m2/repository)
#
# Jars are read from <project-root>/jars/ which must be populated in advance
# (e.g. by copying from the EMR Docker container).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JARS_DIR="${SCRIPT_DIR}/../jars"
MVN="${SCRIPT_DIR}/../build/mvn"
LOCAL_REPO="${HOME}/.m2/repository"

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)
      LOCAL_REPO="$2"; shift 2 ;;
    *)
      echo "Unknown argument: $1" >&2
      echo "Usage: $0 [--repo PATH]" >&2
      exit 1 ;;
  esac
done

if [[ ! -d "${JARS_DIR}" ]]; then
  echo "ERROR: jars directory not found: ${JARS_DIR}" >&2
  exit 1
fi

# Initialise the local repository directory if it does not exist yet
mkdir -p "${LOCAL_REPO}"

echo "Jars directory : ${JARS_DIR}"
echo "Local repository: ${LOCAL_REPO}"
echo "Maven wrapper  : ${MVN}"
echo ""

INSTALLED=()
FAILED=()

install_jar() {
  local group="$1" artifact="$2" version="$3" filename="$4" classifier="${5:-}"
  local jar="${JARS_DIR}/${filename}"

  if [[ ! -f "${jar}" ]]; then
    echo "    MISSING file: ${filename}" >&2
    FAILED+=("${artifact}")
    return
  fi

  local label="${group}:${artifact}:${version}"
  [[ -n "${classifier}" ]] && label="${label}:${classifier}"
  echo ">>> Installing ${label} ..."

  local extra_args=()
  [[ -n "${classifier}" ]] && extra_args+=("-Dclassifier=${classifier}")

  if "${MVN}" install:install-file \
      -Dfile="${jar}" \
      -DgroupId="${group}" \
      -DartifactId="${artifact}" \
      -Dversion="${version}" \
      -Dpackaging=jar \
      -DgeneratePom=true \
      -Dmaven.repo.local="${LOCAL_REPO}" \
      ${extra_args[@]+"${extra_args[@]}"} \
      -q; then
    echo "    OK"
    INSTALLED+=("${artifact}")
  else
    echo "    FAILED" >&2
    FAILED+=("${artifact}")
  fi
}

# ── Spark (org.apache.spark, 3.5.0-amzn-0) ──────────────────────────────────
SPARK_VERSION="3.5.0-amzn-0"
SCALA_VERSION="2.12"
for ARTIFACT in \
    spark-sql_${SCALA_VERSION} \
    spark-hive_${SCALA_VERSION} \
    spark-catalyst_${SCALA_VERSION} \
    spark-core_${SCALA_VERSION} \
    spark-yarn_${SCALA_VERSION} \
    spark-common-utils_${SCALA_VERSION} \
    spark-sql-api_${SCALA_VERSION} \
    spark-tags_${SCALA_VERSION} \
    spark-kvstore_${SCALA_VERSION} \
    spark-network-common_${SCALA_VERSION} \
    spark-network-shuffle_${SCALA_VERSION} \
    spark-unsafe_${SCALA_VERSION} \
    spark-launcher_${SCALA_VERSION} \
    spark-sketch_${SCALA_VERSION}; do
  install_jar "org.apache.spark" "${ARTIFACT}" "${SPARK_VERSION}" "${ARTIFACT}-${SPARK_VERSION}.jar"
done

# ── Hive (org.apache.hive, 2.3.9-amzn-3) ────────────────────────────────────
HIVE_VERSION="2.3.9-amzn-3"
install_jar "org.apache.hive" "hive-exec"         "${HIVE_VERSION}" "hive-exec-${HIVE_VERSION}-core.jar" "core"
install_jar "org.apache.hive" "hive-metastore"    "${HIVE_VERSION}" "hive-metastore-${HIVE_VERSION}.jar"
install_jar "org.apache.hive" "hive-common"       "${HIVE_VERSION}" "hive-common-${HIVE_VERSION}.jar"
install_jar "org.apache.hive" "hive-serde"        "${HIVE_VERSION}" "hive-serde-${HIVE_VERSION}.jar"
install_jar "org.apache.hive" "hive-shims"        "${HIVE_VERSION}" "hive-shims-${HIVE_VERSION}.jar"
install_jar "org.apache.hive" "hive-shims-common" "${HIVE_VERSION}" "hive-shims-common-${HIVE_VERSION}.jar"

# ── Parquet (org.apache.parquet, 1.13.1-amzn-0) ─────────────────────────────
PARQUET_VERSION="1.13.1-amzn-0"
for ARTIFACT in parquet-column parquet-common parquet-encoding parquet-format-structures parquet-hadoop parquet-jackson; do
  install_jar "org.apache.parquet" "${ARTIFACT}" "${PARQUET_VERSION}" "${ARTIFACT}-${PARQUET_VERSION}.jar"
done

# ── Hadoop (org.apache.hadoop, 3.3.6-amzn-2) ────────────────────────────────
# NOTE: hadoop-hdfs-client, hadoop-mapreduce-client-*, hadoop-yarn-* only exist
#       as broken symlinks in the container; those resolve via Maven Central at
#       3.3.6 (open-source) via the spark-3.5 profile dependencyManagement.
HADOOP_VERSION="3.3.6-amzn-2"
for ARTIFACT in hadoop-annotations hadoop-auth hadoop-client hadoop-client-api hadoop-client-runtime hadoop-common; do
  install_jar "org.apache.hadoop" "${ARTIFACT}" "${HADOOP_VERSION}" "${ARTIFACT}-${HADOOP_VERSION}.jar"
done

# ── Thrift ───────────────────────────────────────────────────────────────────
install_jar "org.apache.thrift" "libthrift" "0.12.0" "libthrift-0.12.0.jar"

# ── Netty ────────────────────────────────────────────────────────────────────
install_jar "io.netty" "netty-common" "4.1.96.Final" "netty-common-4.1.96.Final.jar"

# ── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo "=============================="
echo "Summary"
echo "=============================="
echo "Installed (${#INSTALLED[@]}):"
for a in ${INSTALLED[@]+"${INSTALLED[@]}"}; do echo "  - $a"; done
echo ""
echo "Failed (${#FAILED[@]}):"
for a in ${FAILED[@]+"${FAILED[@]}"}; do echo "  - $a"; done

if [[ ${#FAILED[@]} -gt 0 ]]; then
  exit 1
fi
