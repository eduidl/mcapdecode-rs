import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1]))
import benchmark


@pytest.mark.parametrize("case", benchmark.CASES)
def test_rosbags_matches_mcapdecode(case, tmp_path):
    path = benchmark.generate_fixture(case, tmp_path)

    native, pure_python = benchmark.validate_equivalence(path)

    assert native.equals(pure_python)


def test_percentile_interpolates_between_samples():
    assert benchmark.percentile([0, 10], 0.95) == pytest.approx(9.5)


def test_benchmark_report_contains_statistics(tmp_path):
    path = benchmark.generate_fixture("flat", tmp_path)
    result = benchmark.benchmark_case(path, iterations=2, warmup=0)

    assert result["mcapdecode"]["median_ms"] > 0
    assert result["rosbags"]["messages_per_s"] > 0
    assert result["median_speedup"] > 0
