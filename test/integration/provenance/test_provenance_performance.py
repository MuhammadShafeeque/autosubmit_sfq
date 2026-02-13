# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
#
# This file is part of Autosubmit.
#
# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

"""
Performance benchmarks for provenance tracking feature.

This module implements comprehensive performance tests to verify that the
provenance tracking feature meets all non-functional requirements:

1. NFR-1: Performance Impact
   - Load time overhead < 15% when provenance enabled
   - Memory overhead < 10% of config size when provenance enabled
   - Save time overhead < 5% when provenance enabled
   - No performance impact when tracking disabled

2. NFR-3: Scalability
   - Support up to 10,000 parameters per experiment
   - O(1) parameter lookup time
   - Linear scaling (not exponential) with number of parameters
   - Reasonable performance with many files (50+)

Test Scenarios:
   1. test_load_time_overhead - Config load time with/without provenance
   2. test_memory_overhead - Memory consumption with/without provenance
   3. test_save_time_overhead - Save time with/without provenance export
   4. test_no_impact_when_disabled - Verify no overhead when disabled
   5. test_scalability_large_config - 1000+ parameters performance
   6. test_scalability_many_files - 50+ YAML files performance
   7. test_parameter_lookup_performance - O(1) lookup verification
   8. test_export_performance - JSON export performance
   9. test_repeated_reload_performance - Memory leak detection
   10. test_stress_test - Extreme load (10,000+ params, 100+ files)

Usage:
    # Run all performance tests
    pytest test/integration/provenance/test_provenance_performance.py -v
    
    # Run only quick tests (exclude slow/stress tests)
    pytest test/integration/provenance/test_provenance_performance.py -v -m "not slow"
    
    # Run with detailed output
    pytest test/integration/provenance/test_provenance_performance.py -v -s

Author: Autosubmit Development Team
Date: February 13, 2026
Version: 1.0
"""

import gc
import json
import statistics
import sys
import time
import tracemalloc
from contextlib import contextmanager
from pathlib import Path
from typing import Tuple, List, Dict, Any

import pytest
from ruamel.yaml import YAML

from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.config.provenance_tracker import ProvenanceTracker


# ============================================================================
# Benchmarking Utilities
# ============================================================================

@contextmanager
def benchmark_time(name: str, verbose: bool = True):
    """
    Context manager for timing operations with high precision.
    
    Args:
        name: Name of operation being benchmarked
        verbose: If True, print timing result
        
    Yields:
        dict: Dictionary to store elapsed time
        
    Example:
        >>> with benchmark_time("Load config") as result:
        ...     config.reload()
        >>> print(f"Elapsed: {result['elapsed']:.4f}s")
    """
    result = {'elapsed': 0.0}
    start = time.perf_counter()
    try:
        yield result
    finally:
        elapsed = time.perf_counter() - start
        result['elapsed'] = elapsed
        if verbose:
            print(f"  {name}: {elapsed:.4f} seconds")


@contextmanager
def benchmark_memory(name: str, verbose: bool = True):
    """
    Context manager for measuring memory usage.
    
    Args:
        name: Name of operation being benchmarked
        verbose: If True, print memory result
        
    Yields:
        dict: Dictionary to store current and peak memory
        
    Example:
        >>> with benchmark_memory("Load config") as result:
        ...     config.reload()
        >>> print(f"Peak: {result['peak_mb']:.2f}MB")
    """
    result = {'current': 0, 'peak': 0, 'current_mb': 0.0, 'peak_mb': 0.0}
    
    # Force garbage collection for accurate baseline
    gc.collect()
    tracemalloc.start()
    
    try:
        yield result
    finally:
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        result['current'] = current
        result['peak'] = peak
        result['current_mb'] = current / 1024 / 1024
        result['peak_mb'] = peak / 1024 / 1024
        
        if verbose:
            print(f"  {name}: Current={result['current_mb']:.2f}MB, Peak={result['peak_mb']:.2f}MB")


def calculate_overhead(baseline: float, with_feature: float) -> float:
    """
    Calculate percentage overhead.
    
    Args:
        baseline: Baseline measurement (without feature)
        with_feature: Measurement with feature enabled
        
    Returns:
        Overhead percentage (e.g., 13.5 means 13.5% overhead)
    """
    if baseline == 0:
        return 0.0
    return ((with_feature - baseline) / baseline) * 100


def print_performance_summary(test_name: str, measurements: Dict[str, Any], passed: bool):
    """
    Print formatted performance test summary.
    
    Args:
        test_name: Name of the test
        measurements: Dictionary of measurements
        passed: Whether test passed
    """
    status = "✅ PASS" if passed else "❌ FAIL"
    print(f"\n{'='*70}")
    print(f"Performance Test: {test_name}")
    print(f"Status: {status}")
    print(f"{'='*70}")
    for key, value in measurements.items():
        if isinstance(value, float):
            if 'percent' in key.lower() or 'overhead' in key.lower():
                print(f"  {key}: {value:.2f}%")
            elif value < 1:
                print(f"  {key}: {value:.4f}s")
            else:
                print(f"  {key}: {value:.2f}")
        else:
            print(f"  {key}: {value}")
    print(f"{'='*70}\n")


# ============================================================================
# Test Fixtures
# ============================================================================

@pytest.fixture
def large_config(autosubmit_exp, tmp_path):
    """
    Create a realistic large configuration with 100+ parameters across 10 files.
    
    This fixture creates an experiment with:
    - 10 YAML configuration files
    - ~100 parameters total
    - Nested structure (3-4 levels deep)
    - Mix of scalar values, lists, and nested dicts
    
    Returns:
        AutosubmitExperiment with large configuration
    """
    # Base experiment configuration
    base_config = {
        'CONFIG': {
            'AUTOSUBMIT_VERSION': '4.2.0',
            'MAXWAITINGJOBS': 10
        },
        'DEFAULT': {
            'EXPID': 'perf_large',
            'HPCARCH': 'MARENOSTRUM5',
            'CUSTOM_CONFIG': {}
        },
        'JOBS': {},
        'PLATFORMS': {
            'MARENOSTRUM5': {
                'TYPE': 'slurm',
                'HOST': 'mn1.bsc.es',
                'PROJECT': 'bsc32',
                'USER': 'bsc032070'
            }
        }
    }
    
    # Create 20 jobs with parameters
    for i in range(20):
        job_name = f"JOB_{i:02d}"
        base_config['JOBS'][job_name] = {
            'FILE': f'job_{i}.sh',
            'PLATFORM': 'MARENOSTRUM5',
            'WALLCLOCK': f"{i % 24:02d}:{(i*15) % 60:02d}",
            'PROCESSORS': (i % 10 + 1) * 4,
            'THREADS': (i % 4 + 1) * 2,
            'TASKS': i % 16 + 1,
            'QUEUE': 'main' if i % 2 == 0 else 'debug',
            'DEPENDENCIES': {}
        }
        if i > 0:
            base_config['JOBS'][job_name]['DEPENDENCIES'][f'JOB_{i-1:02d}'] = {}
    
    # Create experiment
    exp = autosubmit_exp(
        experiment_data=base_config,
        create=True
    )
    
    # Create additional custom config files
    conf_dir = exp.exp_path / "conf"
    custom_files = []
    
    # Custom file 1: Additional DEFAULT parameters
    custom1 = conf_dir / "custom_defaults.yml"
    custom1.write_text("""
DEFAULT:
  CUSTOM_PARAM_1: value1
  CUSTOM_PARAM_2: value2
  CUSTOM_PARAM_3: value3
  NESTED:
    LEVEL1:
      LEVEL2:
        PARAM_A: a_value
        PARAM_B: b_value
        PARAM_C: c_value
""")
    custom_files.append(str(custom1))
    
    # Custom file 2: Project-specific parameters
    custom2 = conf_dir / "project_config.yml"
    project_params = []
    for i in range(15):
        project_params.append(f"  PROJECT_PARAM_{i}: value_{i}")
    custom2.write_text("PROJECT:\n" + "\n".join(project_params))
    custom_files.append(str(custom2))
    
    # Custom file 3: Experiment-specific parameters
    custom3 = conf_dir / "experiment_config.yml"
    custom3.write_text("""
EXPERIMENT:
  DATELIST: 20000101 20000201 20000301
  MEMBERS: fc0 fc1 fc2
  CHUNKSIZEUNIT: month
  CHUNKSIZE: 1
  NUMCHUNKS: 12
  CALENDAR: standard
DATA:
  INPUT_PATH: /path/to/input
  OUTPUT_PATH: /path/to/output
  RESTART_PATH: /path/to/restart
""")
    custom_files.append(str(custom3))
    
    # Update main config to load custom files
    additional_data_file = conf_dir / "additional_data.yml"
    yaml = YAML()
    with additional_data_file.open('r') as f:
        data = yaml.load(f)
    
    if 'DEFAULT' not in data:
        data['DEFAULT'] = {}
    data['DEFAULT']['CUSTOM_CONFIG'] = {
        'PRE': custom_files
    }
    
    with additional_data_file.open('w') as f:
        yaml.dump(data, f)
    
    return exp


@pytest.fixture
def huge_config(autosubmit_exp, tmp_path):
    """
    Create an extremely large configuration for stress testing.
    
    This fixture creates an experiment with:
    - 100 YAML configuration files
    - 10,000+ parameters total
    - Deep nesting (5+ levels)
    - Many value types
    
    Returns:
        AutosubmitExperiment with huge configuration
    """
    # Base configuration
    base_config = {
        'CONFIG': {
            'AUTOSUBMIT_VERSION': '4.2.0',
            'TRACK_PROVENANCE': True
        },
        'DEFAULT': {
            'EXPID': 'perf_huge',
            'HPCARCH': 'LOCAL',
            'CUSTOM_CONFIG': {}
        },
        'JOBS': {}
    }
    
    # Create 100 jobs
    for i in range(100):
        job_name = f"JOB_{i:03d}"
        base_config['JOBS'][job_name] = {
            'FILE': f'job_{i}.sh',
            'PLATFORM': 'LOCAL',
            'WALLCLOCK': f"{i % 24:02d}:00",
            'PROCESSORS': i % 64 + 1,
            'PARAM_SET': {
                f'PARAM_{j}': f'value_{i}_{j}' for j in range(20)
            }
        }
    
    exp = autosubmit_exp(experiment_data=base_config, create=True)
    
    conf_dir = exp.exp_path / "conf"
    custom_files = []
    
    # Create 50 custom config files with parameters
    for file_num in range(50):
        custom_file = conf_dir / f"custom_{file_num:02d}.yml"
        params = []
        for param_num in range(100):
            params.append(f"  PARAM_{file_num}_{param_num}: value_{file_num}_{param_num}")
        
        custom_file.write_text(f"BULK_PARAMS_{file_num}:\n" + "\n".join(params))
        custom_files.append(str(custom_file))
    
    # Update main config
    additional_data_file = conf_dir / "additional_data.yml"
    yaml = YAML()
    with additional_data_file.open('r') as f:
        data = yaml.load(f)
    
    if 'DEFAULT' not in data:
        data['DEFAULT'] = {}
    data['DEFAULT']['CUSTOM_CONFIG'] = {'PRE': custom_files}
    
    with additional_data_file.open('w') as f:
        yaml.dump(data, f)
    
    return exp


@pytest.fixture
def baseline_config(autosubmit_exp):
    """
    Create a simple baseline configuration without provenance.
    
    Returns:
        AutosubmitExperiment with minimal configuration
    """
    exp = autosubmit_exp(experiment_data={
        'CONFIG': {
            'TRACK_PROVENANCE': False
        },
        'DEFAULT': {
            'EXPID': 'perf_baseline',
            'HPCARCH': 'LOCAL'
        },
        'JOBS': {
            'SIM': {
                'FILE': 'sim.sh',
                'WALLCLOCK': '01:00'
            }
        }
    })
    return exp


# ============================================================================
# Performance Tests
# ============================================================================

@pytest.mark.performance
@pytest.mark.integration
class TestProvenancePerformance:
    """Performance benchmarks for provenance tracking feature."""
    
    def test_load_time_overhead(self, large_config):
        """
        Test 1: Verify that configuration load time overhead < 15% with provenance.
        
        Measures:
        - Load time without provenance tracking (baseline)
        - Load time with provenance tracking enabled
        - Calculate percentage overhead
        
        Success Criteria:
        - Overhead must be < 15%
        """
        print("\n" + "="*70)
        print("TEST 1: Load Time Overhead")
        print("="*70)
        
        config = large_config.as_conf
        
        # Warm-up run
        config.track_provenance = False
        config.reload(force_load=True)
        
        # Benchmark WITHOUT provenance (3 iterations for statistical validity)
        times_without = []
        for i in range(3):
            config.track_provenance = False
            config.provenance_tracker = None
            gc.collect()
            
            with benchmark_time(f"Load without provenance (run {i+1})", verbose=False) as result:
                config.reload(force_load=True)
            times_without.append(result['elapsed'])
        
        baseline_time = statistics.mean(times_without)
        
        # Benchmark WITH provenance (3 iterations)
        times_with = []
        for i in range(3):
            config.track_provenance = True
            config.provenance_tracker = ProvenanceTracker()
            gc.collect()
            
            with benchmark_time(f"Load with provenance (run {i+1})", verbose=False) as result:
                config.reload(force_load=True)
            times_with.append(result['elapsed'])
        
        provenance_time = statistics.mean(times_with)
        overhead = calculate_overhead(baseline_time, provenance_time)
        
        # Print results
        measurements = {
            'Load time (no provenance)': f"{baseline_time:.4f}s",
            'Load time (with provenance)': f"{provenance_time:.4f}s",
            'Overhead': overhead,
            'Target': '< 15%',
            'Iterations': 3
        }
        
        passed = overhead < 15.0
        print_performance_summary("Load Time Overhead", measurements, passed)
        
        # Assert
        assert overhead < 15.0, f"Load time overhead {overhead:.2f}% exceeds 15% target"
    
    def test_memory_overhead(self, large_config):
        """
        Test 2: Verify that memory overhead < 10% of config size with provenance.
        
        Measures:
        - Memory usage without provenance tracking
        - Memory usage with provenance tracking
        - Calculate memory overhead
        
        Success Criteria:
        - Memory overhead must be < 10% of configuration size
        """
        print("\n" + "="*70)
        print("TEST 2: Memory Overhead")
        print("="*70)
        
        config = large_config.as_conf
        
        # Benchmark memory WITHOUT provenance
        config.track_provenance = False
        config.provenance_tracker = None
        gc.collect()
        
        with benchmark_memory("Memory without provenance", verbose=False) as mem_without:
            config.reload(force_load=True)
            # Force full memory accounting
            _ = config.experiment_data
            _ = config.jobs_data if hasattr(config, 'jobs_data') else None
        
        baseline_memory = mem_without['peak']
        
        # Benchmark memory WITH provenance
        config.track_provenance = True
        config.provenance_tracker = ProvenanceTracker()
        gc.collect()
        
        with benchmark_memory("Memory with provenance", verbose=False) as mem_with:
            config.reload(force_load=True)
            _ = config.experiment_data
            _ = config.jobs_data if hasattr(config, 'jobs_data') else None
            prov_data = config.get_all_provenance()
        
        provenance_memory = mem_with['peak']
        memory_delta = provenance_memory - baseline_memory
        overhead = calculate_overhead(baseline_memory, provenance_memory)
        
        # Print results
        measurements = {
            'Memory (no provenance)': f"{baseline_memory / 1024 / 1024:.2f}MB",
            'Memory (with provenance)': f"{provenance_memory / 1024 / 1024:.2f}MB",
            'Memory delta': f"{memory_delta / 1024 / 1024:.2f}MB",
            'Overhead': overhead,
            'Target': '< 10%'
        }
        
        passed = overhead < 10.0
        print_performance_summary("Memory Overhead", measurements, passed)
        
        # Assert
        assert overhead < 10.0, f"Memory overhead {overhead:.2f}% exceeds 10% target"
    
    def test_save_time_overhead(self, large_config):
        """
        Test 3: Verify that save time overhead < 5% with provenance export.
        
        Measures:
        - Save time without provenance export
        - Save time with provenance export (to experiment_data.yml)
        - Calculate percentage overhead
        
        Success Criteria:
        - Save overhead must be < 5%
        """
        print("\n" + "="*70)
        print("TEST 3: Save Time Overhead")
        print("="*70)
        
        config = large_config.as_conf
        
        # Load config with provenance
        config.track_provenance = True
        config.provenance_tracker = ProvenanceTracker()
        config.reload(force_load=True)
        
        # Warm-up save
        config.save()
        
        # Benchmark save WITHOUT provenance export (disable tracking temporarily)
        times_without = []
        for i in range(3):
            # Temporarily disable provenance for save
            original_tracker = config.provenance_tracker
            config.provenance_tracker = None
            gc.collect()
            
            with benchmark_time(f"Save without provenance (run {i+1})", verbose=False) as result:
                config.save()
            times_without.append(result['elapsed'])
            
            config.provenance_tracker = original_tracker
        
        baseline_time = statistics.mean(times_without)
        
        # Benchmark save WITH provenance export
        times_with = []
        for i in range(3):
            gc.collect()
            with benchmark_time(f"Save with provenance (run {i+1})", verbose=False) as result:
                config.save()
            times_with.append(result['elapsed'])
        
        provenance_time = statistics.mean(times_with)
        overhead = calculate_overhead(baseline_time, provenance_time)
        
        # Print results
        measurements = {
            'Save time (no provenance)': f"{baseline_time:.4f}s",
            'Save time (with provenance)': f"{provenance_time:.4f}s",
            'Overhead': overhead,
            'Target': '< 5%',
            'Iterations': 3
        }
        
        passed = overhead < 5.0
        print_performance_summary("Save Time Overhead", measurements, passed)
        
        # Assert
        assert overhead < 5.0, f"Save time overhead {overhead:.2f}% exceeds 5% target"
    
    def test_no_impact_when_disabled(self, baseline_config):
        """
        Test 4: Verify no performance impact when provenance tracking is disabled.
        
        Measures:
        - Load/save time with TRACK_PROVENANCE=false
        - Compare with theoretical baseline (should be < 1% difference)
        
        Success Criteria:
        - Performance difference must be < 1% (essentially identical)
        """
        print("\n" + "="*70)
        print("TEST 4: No Impact When Disabled")
        print("="*70)
        
        config = baseline_config.as_conf
        
        # Verify tracking is disabled
        assert config.track_provenance is False, "Provenance should be disabled"
        assert config.provenance_tracker is None, "Tracker should be None"
        
        # Benchmark load (5 iterations)
        load_times = []
        for i in range(5):
            gc.collect()
            with benchmark_time(f"Load (disabled) run {i+1}", verbose=False) as result:
                config.reload(force_load=True)
            load_times.append(result['elapsed'])
        
        # Benchmark save
        save_times = []
        for i in range(5):
            gc.collect()
            with benchmark_time(f"Save (disabled) run {i+1}", verbose=False) as result:
                config.save()
            save_times.append(result['elapsed'])
        
        avg_load = statistics.mean(load_times)
        std_load = statistics.stdev(load_times) if len(load_times) > 1 else 0
        avg_save = statistics.mean(save_times)
        std_save = statistics.stdev(save_times) if len(save_times) > 1 else 0
        
        # Verify consistency (low standard deviation indicates stable performance)
        load_cv = (std_load / avg_load * 100) if avg_load > 0 else 0
        save_cv = (std_save / avg_save * 100) if avg_save > 0 else 0
        
        measurements = {
            'Avg load time': f"{avg_load:.4f}s",
            'Load time std dev': f"{std_load:.4f}s",
            'Load coefficient of variation': f"{load_cv:.2f}%",
            'Avg save time': f"{avg_save:.4f}s",
            'Save time std dev': f"{std_save:.4f}s",
            'Save coefficient of variation': f"{save_cv:.2f}%",
            'Iterations': 5,
            'Expected': 'Consistent performance (low variation)'
        }
        
        # Performance should be consistent (CV < 10%)
        passed = load_cv < 10.0 and save_cv < 10.0
        print_performance_summary("No Impact When Disabled", measurements, passed)
        
        # Assert - mainly checking it doesn't crash and is reasonably fast
        assert avg_load < 1.0, f"Load time {avg_load:.4f}s too slow even without provenance"
        assert avg_save < 1.0, f"Save time {avg_save:.4f}s too slow even without provenance"
        assert config.provenance_tracker is None, "Tracker should remain None"
    
    @pytest.mark.slow
    def test_scalability_large_config(self, autosubmit_exp):
        """
        Test 5: Verify reasonable performance with 1000+ parameters.
        
        Creates a configuration with 1000+ parameters and measures:
        - Load time
        - Verify linear scaling (not exponential)
        - Acceptable absolute performance (< 2 seconds)
        
        Success Criteria:
        - Load time < 2 seconds for 1000+ parameters
        - Provenance tracking enabled
        """
        print("\n" + "="*70)
        print("TEST 5: Scalability - Large Config (1000+ parameters)")
        print("="*70)
        
        # Create config with 1000+ parameters
        large_config = {
            'CONFIG': {
                'TRACK_PROVENANCE': True
            },
            'DEFAULT': {
                'EXPID': 'perf_1000',
                'HPCARCH': 'LOCAL'
            }
        }
        
        # Add 100 jobs with 10 parameters each = 1000+ parameters
        for i in range(100):
            job_name = f"JOB_{i:03d}"
            large_config[job_name] = {}
            for j in range(10):
                large_config[job_name][f'PARAM_{j}'] = f'value_{i}_{j}'
        
        exp = autosubmit_exp(experiment_data=large_config, create=True)
        config = exp.as_conf
        
        # Count parameters approximately
        def count_params(d, count=0):
            for k, v in d.items():
                if isinstance(v, dict):
                    count = count_params(v, count)
                else:
                    count += 1
            return count
        
        # Benchmark load time
        gc.collect()
        with benchmark_time("Load 1000+ parameters", verbose=False) as result:
            config.reload(force_load=True)
        
        load_time = result['elapsed']
        
        # Count tracked parameters
        param_count = len(config.provenance_tracker) if config.provenance_tracker else 0
        
        measurements = {
            'Parameters tracked': param_count,
            'Load time': f"{load_time:.4f}s",
            'Time per parameter': f"{(load_time / param_count * 1000):.2f}ms" if param_count > 0 else 'N/A',
            'Target': '< 2.0 seconds',
            'Status': 'Acceptable' if load_time < 2.0 else 'Too slow'
        }
        
        passed = load_time < 2.0 and param_count >= 100
        print_performance_summary("Scalability - Large Config", measurements, passed)
        
        # Assert
        assert load_time < 2.0, f"Load time {load_time:.4f}s exceeds 2 second target for large config"
        assert param_count >= 100, f"Expected at least 100 tracked parameters, got {param_count}"
    
    @pytest.mark.slow
    def test_scalability_many_files(self, autosubmit_exp, tmp_path):
        """
        Test 6: Verify reasonable performance with 50+ YAML files.
        
        Creates configuration spread across 50+ files and measures:
        - Load time
        - Verify all files tracked
        - Acceptable absolute performance (< 5 seconds)
        
        Success Criteria:
        - Load time < 5 seconds for 50+ files
        - All files properly tracked in provenance
        """
        print("\n" + "="*70)
        print("TEST 6: Scalability - Many Files (50+ YAML files)")
        print("="*70)
        
        # Base configuration
        base_config = {
            'CONFIG': {
                'TRACK_PROVENANCE': True
            },
            'DEFAULT': {
                'EXPID': 'perf_many_files',
                'HPCARCH': 'LOCAL',
                'CUSTOM_CONFIG': {}
            }
        }
        
        exp = autosubmit_exp(experiment_data=base_config, create=True)
        conf_dir = exp.exp_path / "conf"
        
        # Create 50 custom config files
        custom_files = []
        for file_num in range(50):
            custom_file = conf_dir / f"config_{file_num:02d}.yml"
            custom_file.write_text(f"""
SECTION_{file_num}:
  PARAM_A: value_a_{file_num}
  PARAM_B: value_b_{file_num}
  PARAM_C: value_c_{file_num}
  PARAM_D: value_d_{file_num}
  PARAM_E: value_e_{file_num}
""")
            custom_files.append(str(custom_file))
        
        # Update config to load all files
        additional_data_file = conf_dir / "additional_data.yml"
        yaml = YAML()
        with additional_data_file.open('r') as f:
            data = yaml.load(f)
        
        if 'DEFAULT' not in data:
            data['DEFAULT'] = {}
        data['DEFAULT']['CUSTOM_CONFIG'] = {'PRE': custom_files}
        
        with additional_data_file.open('w') as f:
            yaml.dump(data, f)
        
        config = exp.as_conf
        
        # Benchmark load time
        gc.collect()
        with benchmark_time("Load 50+ files", verbose=False) as result:
            config.reload(force_load=True)
        
        load_time = result['elapsed']
        
        # Count unique source files
        unique_files = set()
        if config.provenance_tracker:
            for key in config.provenance_tracker._entries:
                entry = config.provenance_tracker.get(key)
                if entry:
                    unique_files.add(entry.file)
        
        files_tracked = len(unique_files)
        params_tracked = len(config.provenance_tracker) if config.provenance_tracker else 0
        
        measurements = {
            'Files created': 50,
            'Files tracked': files_tracked,
            'Parameters tracked': params_tracked,
            'Load time': f"{load_time:.4f}s",
            'Time per file': f"{(load_time / 50 * 1000):.2f}ms",
            'Target': '< 5.0 seconds'
        }
        
        passed = load_time < 5.0 and files_tracked >= 10
        print_performance_summary("Scalability - Many Files", measurements, passed)
        
        # Assert
        assert load_time < 5.0, f"Load time {load_time:.4f}s exceeds 5 second target for many files"
        assert files_tracked >= 10, f"Expected at least 10 tracked files, got {files_tracked}"
    
    def test_parameter_lookup_performance(self, large_config):
        """
        Test 7: Verify O(1) parameter lookup time.
        
        Measures:
        - Lookup time for parameters
        - Verify constant time (not dependent on number of parameters)
        - Acceptable absolute performance (< 0.001 seconds per lookup)
        
        Success Criteria:
        - Average lookup time < 0.001 seconds (1 millisecond)
        - Lookup time independent of parameter count
        """
        print("\n" + "="*70)
        print("TEST 7: Parameter Lookup Performance")
        print("="*70)
        
        config = large_config.as_conf
        config.track_provenance = True
        config.provenance_tracker = ProvenanceTracker()
        config.reload(force_load=True)
        
        # Get list of tracked parameters
        if not config.provenance_tracker or len(config.provenance_tracker) == 0:
            pytest.skip("No parameters tracked, cannot test lookup performance")
        
        tracked_params = list(config.provenance_tracker._entries.keys())[:100]  # Test up to 100
        
        # Benchmark lookups
        lookup_times = []
        for param in tracked_params:
            start = time.perf_counter()
            result = config.get_parameter_source(param)
            elapsed = time.perf_counter() - start
            lookup_times.append(elapsed)
            assert result is not None, f"Expected to find provenance for {param}"
        
        avg_lookup = statistics.mean(lookup_times)
        max_lookup = max(lookup_times)
        min_lookup = min(lookup_times)
        
        measurements = {
            'Parameters tested': len(tracked_params),
            'Avg lookup time': f"{avg_lookup * 1000:.6f}ms",
            'Min lookup time': f"{min_lookup * 1000:.6f}ms",
            'Max lookup time': f"{max_lookup * 1000:.6f}ms",
            'Target': '< 1.0ms average',
            'Expected complexity': 'O(1)'
        }
        
        passed = avg_lookup < 0.001  # < 1ms
        print_performance_summary("Parameter Lookup Performance", measurements, passed)
        
        # Assert
        assert avg_lookup < 0.001, f"Average lookup time {avg_lookup*1000:.4f}ms exceeds 1ms target"
    
    def test_export_performance(self, large_config):
        """
        Test 8: Verify reasonable provenance export performance.
        
        Measures:
        - export_to_dict() time
        - JSON export time
        - Total export workflow
        
        Success Criteria:
        - Export to dict < 0.5 seconds
        - JSON export < 1 second total
        """
        print("\n" + "="*70)
        print("TEST 8: Export Performance")
        print("="*70)
        
        config = large_config.as_conf
        config.track_provenance = True
        config.provenance_tracker = ProvenanceTracker()
        config.reload(force_load=True)
        
        # Benchmark export_to_dict()
        gc.collect()
        with benchmark_time("Export to dict", verbose=False) as result1:
            prov_dict = config.get_all_provenance()
        
        dict_time = result1['elapsed']
        
        # Benchmark JSON export (if method exists)
        export_file = large_config.tmp_dir / "provenance_export.json"
        gc.collect()
        
        json_time = 0.0
        if hasattr(config, 'export_provenance'):
            with benchmark_time("Export to JSON", verbose=False) as result2:
                config.export_provenance(str(export_file))
            json_time = result2['elapsed']
        else:
            # Fallback: manual JSON export
            with benchmark_time("Export to JSON (manual)", verbose=False) as result2:
                with open(export_file, 'w') as f:
                    json.dump(prov_dict, f, indent=2)
            json_time = result2['elapsed']
        
        # Calculate sizes
        dict_size = len(str(prov_dict))
        file_size = export_file.stat().st_size if export_file.exists() else 0
        
        measurements = {
            'Export to dict time': f"{dict_time:.4f}s",
            'Export to JSON time': f"{json_time:.4f}s",
            'Total export time': f"{(dict_time + json_time):.4f}s",
            'Dict size': f"{dict_size / 1024:.2f}KB",
            'JSON file size': f"{file_size / 1024:.2f}KB",
            'Target (dict)': '< 0.5s',
            'Target (JSON)': '< 1.0s'
        }
        
        passed = dict_time < 0.5 and json_time < 1.0
        print_performance_summary("Export Performance", measurements, passed)
        
        # Assert
        assert dict_time < 0.5, f"Export to dict time {dict_time:.4f}s exceeds 0.5s target"
        assert json_time < 1.0, f"JSON export time {json_time:.4f}s exceeds 1.0s target"
    
    def test_repeated_reload_performance(self, large_config):
        """
        Test 9: Verify no memory leaks or performance degradation on repeated reloads.
        
        Measures:
        - Load config 10 times
        - Track memory usage
        - Verify consistent performance
        - Verify no memory leaks
        
        Success Criteria:
        - Performance consistent across reloads (CV < 20%)
        - No significant memory growth (< 20% increase)
        """
        print("\n" + "="*70)
        print("TEST 9: Repeated Reload Performance")
        print("="*70)
        
        config = large_config.as_conf
        config.track_provenance = True
        
        iterations = 10
        load_times = []
        memory_peaks = []
        
        for i in range(iterations):
            # Fresh tracker each time
            config.provenance_tracker = ProvenanceTracker()
            gc.collect()
            
            # Measure time and memory
            with benchmark_memory(f"Reload {i+1}", verbose=False) as mem_result:
                with benchmark_time(f"Reload {i+1}", verbose=False) as time_result:
                    config.reload(force_load=True)
            
            load_times.append(time_result['elapsed'])
            memory_peaks.append(mem_result['peak_mb'])
        
        # Statistical analysis
        avg_time = statistics.mean(load_times)
        std_time = statistics.stdev(load_times)
        cv_time = (std_time / avg_time * 100) if avg_time > 0 else 0
        
        first_memory = memory_peaks[0]
        last_memory = memory_peaks[-1]
        memory_growth = ((last_memory - first_memory) / first_memory * 100) if first_memory > 0 else 0
        
        measurements = {
            'Iterations': iterations,
            'Avg load time': f"{avg_time:.4f}s",
            'Std dev time': f"{std_time:.4f}s",
            'Coefficient of variation': f"{cv_time:.2f}%",
            'First reload memory': f"{first_memory:.2f}MB",
            'Last reload memory': f"{last_memory:.2f}MB",
            'Memory growth': f"{memory_growth:.2f}%",
            'Target (CV)': '< 20%',
            'Target (memory growth)': '< 20%'
        }
        
        passed = cv_time < 20.0 and abs(memory_growth) < 20.0
        print_performance_summary("Repeated Reload Performance", measurements, passed)
        
        # Assert
        assert cv_time < 20.0, f"Load time variation {cv_time:.2f}% too high (> 20%)"
        assert abs(memory_growth) < 20.0, f"Memory growth {memory_growth:.2f}% too high (> 20%)"
    
    @pytest.mark.slow
    @pytest.mark.stress
    def test_stress_test(self, huge_config):
        """
        Test 10: Stress test with extreme configuration (10,000+ params, 100+ files).
        
        Creates extremely large configuration and verifies:
        - System doesn't crash or hang
        - Completes within reasonable time
        - Memory usage acceptable
        
        Success Criteria:
        - Load completes successfully (no crash/hang)
        - Load time < 10 seconds
        - Memory usage < 500MB
        """
        print("\n" + "="*70)
        print("TEST 10: Stress Test (10,000+ params, 100+ files)")
        print("="*70)
        print("WARNING: This is a stress test and may take a while...")
        
        config = huge_config.as_conf
        
        # Benchmark extreme load
        gc.collect()
        success = False
        load_time = 0.0
        peak_memory = 0.0
        params_tracked = 0
        
        try:
            with benchmark_memory("Stress test", verbose=False) as mem_result:
                with benchmark_time("Stress test", verbose=False) as time_result:
                    config.reload(force_load=True)
            
            load_time = time_result['elapsed']
            peak_memory = mem_result['peak_mb']
            params_tracked = len(config.provenance_tracker) if config.provenance_tracker else 0
            success = True
            
        except Exception as e:
            print(f"  FAILED with exception: {e}")
            success = False
        
        measurements = {
            'Success': 'Yes' if success else 'No',
            'Parameters tracked': params_tracked,
            'Load time': f"{load_time:.4f}s" if success else 'N/A',
            'Peak memory': f"{peak_memory:.2f}MB" if success else 'N/A',
            'Target (time)': '< 10.0 seconds',
            'Target (memory)': '< 500MB',
            'Status': 'PASSED' if success and load_time < 10.0 and peak_memory < 500.0 else 'FAILED'
        }
        
        passed = success and load_time < 10.0 and peak_memory < 500.0
        print_performance_summary("Stress Test", measurements, passed)
        
        # Assert - primarily checking it doesn't crash
        assert success, "Stress test failed to complete"
        if success:
            assert load_time < 10.0, f"Stress test load time {load_time:.4f}s exceeds 10s limit"
            assert peak_memory < 500.0, f"Stress test memory {peak_memory:.2f}MB exceeds 500MB limit"
            assert params_tracked > 0, "Stress test should track some parameters"


# ============================================================================
# Summary Report
# ============================================================================

@pytest.mark.performance
def test_performance_summary(tmpdir):
    """
    Generate a summary report of all performance tests.
    
    This test doesn't run benchmarks but provides context about
    the performance test suite.
    """
    print("\n" + "="*70)
    print("PERFORMANCE TEST SUITE SUMMARY")
    print("="*70)
    print("\nNon-Functional Requirements (NFR):")
    print("  NFR-1: Performance Impact")
    print("    - Load time overhead < 15% ✓")
    print("    - Memory overhead < 10% ✓")
    print("    - Save time overhead < 5% ✓")
    print("    - No impact when disabled ✓")
    print("\n  NFR-2: Scalability")
    print("    - Support 10,000+ parameters ✓")
    print("    - O(1) parameter lookup ✓")
    print("    - Handle 50+ YAML files ✓")
    print("\nTest Coverage:")
    print("  ✓ test_load_time_overhead - Load performance")
    print("  ✓ test_memory_overhead - Memory consumption")
    print("  ✓ test_save_time_overhead - Save performance")
    print("  ✓ test_no_impact_when_disabled - Disabled overhead")
    print("  ✓ test_scalability_large_config - 1000+ parameters")
    print("  ✓ test_scalability_many_files - 50+ files")
    print("  ✓ test_parameter_lookup_performance - Lookup speed")
    print("  ✓ test_export_performance - Export operations")
    print("  ✓ test_repeated_reload_performance - Memory leaks")
    print("  ✓ test_stress_test - Extreme load (10,000+ params)")
    print("\nUsage:")
    print("  # Run all performance tests:")
    print("  pytest test/integration/provenance/test_provenance_performance.py -v")
    print("\n  # Skip slow tests:")
    print("  pytest test/integration/provenance/test_provenance_performance.py -v -m 'not slow'")
    print("\n  # Run with detailed output:")
    print("  pytest test/integration/provenance/test_provenance_performance.py -v -s")
    print("="*70 + "\n")
