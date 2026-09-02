"""SparkSession construction. Mind the JAR versions before changing anything."""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from fuel_lakehouse.config import load_config

# Must match the hadoop-client PySpark 4.2.0 bundles, or S3A dies at class
# load with an error that points nowhere. Ivy pulls AWS SDK v2 from here.
HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.5.0"

# PySpark 4 supports Java 17 and 21. Newer JDKs fail at session start.
SUPPORTED_JAVA = (17, 21)

# 200 is a cluster default; on one machine it is mostly scheduling overhead.
LOCAL_SHUFFLE_PARTITIONS = "8"


def _java_binary() -> str:
    """The java Spark will launch. JAVA_HOME wins over PATH."""
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        candidate = Path(java_home) / "bin" / "java"
        if candidate.exists():
            return str(candidate)
    return "java"


def _java_major_version() -> int | None:
    try:
        out = subprocess.run(
            [_java_binary(), "-version"], capture_output=True, text=True, timeout=15
        ).stderr
    except (OSError, subprocess.SubprocessError):
        return None
    match = re.search(r'version "(\d+)', out)
    return int(match.group(1)) if match else None


def _check_java() -> None:
    major = _java_major_version()
    if major is None or major in SUPPORTED_JAVA:
        return
    raise RuntimeError(
        f"Java {major} is on PATH, but PySpark 4 supports Java "
        f"{' or '.join(str(v) for v in SUPPORTED_JAVA)}. "
        f"Set JAVA_HOME to a supported JDK, for example:\n"
        f"  export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64"
    )


def build_spark(app_name: str, *, local_storage: bool = False) -> SparkSession:
    """Delta enabled session.

    local_storage drops S3A entirely, so tests need neither the JARs nor MinIO.
    """
    _check_java()
    cfg = load_config()

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", LOCAL_SHUFFLE_PARTITIONS)
        .config("spark.ui.showConsoleProgress", "false")
    )

    extra_packages = []
    if not local_storage:
        extra_packages.append(HADOOP_AWS)
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", cfg.storage.endpoint)
            .config("spark.hadoop.fs.s3a.access.key", cfg.storage.access_key)
            .config("spark.hadoop.fs.s3a.secret.key", cfg.storage.secret_key)
            # MinIO serves buckets as a path, not as a subdomain.
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.connection.ssl.enabled",
                str(cfg.storage.endpoint.startswith("https")).lower(),
            )
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
        )

    session: SparkSession = configure_spark_with_delta_pip(
        builder, extra_packages=extra_packages
    ).getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return session
