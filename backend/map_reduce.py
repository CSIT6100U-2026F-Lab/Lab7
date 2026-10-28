"""
Lab 7 — workspace top-k words with Spark (local mode).

Spark splits the files (textFile). Students only change map_partition.
reduce_counts is already correct.

Starter map (incorrect): each partition keeps only its local top-k.
Correct map: emit every word count in the partition (exhaustive / top-all),
then reduceByKey adds them, then takeOrdered(k) is the global top-k.
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading
from collections import Counter
from typing import Iterable, List, Sequence, Tuple

WordCount = Tuple[str, int]

_BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
_spark = None
_spark_lock = threading.Lock()


def _prepare_worker_imports() -> None:
    """Spark workers are separate Python processes; they must import this package."""
    if _BACKEND_DIR not in sys.path:
        sys.path.insert(0, _BACKEND_DIR)
    current = os.environ.get("PYTHONPATH", "")
    parts = current.split(os.pathsep) if current else []
    if _BACKEND_DIR not in parts:
        os.environ["PYTHONPATH"] = (
            _BACKEND_DIR + os.pathsep + current if current else _BACKEND_DIR
        )


def tokenize(line: str) -> List[str]:
    """Whitespace-separated tokens, lowercased."""
    return [token.lower() for token in line.split() if token]


def map_partition(lines: Iterable[str], k: int) -> Iterable[WordCount]:
    """The Map function for a spark split.
    """
    # TODO: You may need to change this method:
    
    counts: Counter = Counter()
    for line in lines:
        counts.update(tokenize(line))

    return counts.most_common(k)


def reduce_counts(left: int, right: int) -> int:
    """The Reduce method"""
    
    # TODO: You may need to change this method:
    return left + right

def compute_topk(paths: Sequence[str], k: int) -> List[WordCount]:
    """Read UTF-8 text files with Spark and apply map and reduce
    """
    if k <= 0 or not paths:
        return []

    abs_paths = [os.path.abspath(path) for path in paths]
    spark = get_spark()
    rdd = spark.sparkContext.textFile(",".join(abs_paths))
    mapped = rdd.mapPartitions(lambda iterator: map_partition(iterator, k))
    reduced = mapped.reduceByKey(reduce_counts)
    return reduced.takeOrdered(k, key=lambda item: (-item[1], item[0]))

def _ensure_java() -> None:
    """Set JAVA_HOME on macOS when the user has a JDK but /usr/bin/java is a stub."""
    if os.environ.get("JAVA_HOME"):
        return
    if sys.platform != "darwin":
        return
    try:
        home = subprocess.check_output(
            ["/usr/libexec/java_home"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return
    if home:
        os.environ["JAVA_HOME"] = home


def get_spark():
    """One local SparkSession for the process (JVM start is slow)."""
    global _spark
    with _spark_lock:
        if _spark is None:
            _ensure_java()
            _prepare_worker_imports()
            os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
            os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
            from pyspark.sql import SparkSession

            _spark = (
                SparkSession.builder.master("local[2]")
                .appName("lab7-topk")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.executorEnv.PYTHONPATH", os.environ.get("PYTHONPATH", _BACKEND_DIR))
                .getOrCreate()
            )
            _spark.sparkContext.setLogLevel("ERROR")
            _spark.sparkContext.addPyFile(os.path.join(_BACKEND_DIR, "map_reduce.py"))
        return _spark


def stop_spark() -> None:
    global _spark
    with _spark_lock:
        if _spark is not None:
            _spark.stop()
            _spark = None



