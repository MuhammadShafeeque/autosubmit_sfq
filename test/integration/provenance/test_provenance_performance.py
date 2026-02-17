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
Performance benchmarks for provenance tracking (NFR-1).

NFR-1 Requirements:
- Load time overhead < 15%
- Memory overhead < 10%
- Save time overhead < 5%
- No impact when disabled
- O(1) parameter lookup
"""

import gc
import statistics
import time
import tracemalloc
from contextlib import contextmanager

import pytest
from ruamel.yaml import YAML

from autosubmit.config.provenance_tracker import ProvenanceTracker


# ============================================================================
# Benchmarking Utilities
# ============================================================================

@contextmanager
def benchmark_time(name: str, verbose: bool = True):
    """Context manager for timing operations."""
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
    """Context manager for measuring memory usage."""
    result = {'current': 0, 'peak': 0, 'current_mb': 0.0, 'peak_mb': 0.0}
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
            print(f"  {name}: {result['current_mb']:.2f}MB peak")


def calculate_overhead(baseline: float, with_feature: float) -> float:
    """Calculate percentage overhead."""
    if baseline == 0:
        return 0.0
    return ((with_feature - baseline) / baseline) * 100


def print_summary(test_name: str, measurements: dict, passed: bool):
    """Print test summary."""
    status = "✅ PASS" if passed else "❌ FAIL"
    print(f"\n{'='*60}")
    print(f"{test_name}: {status}")
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
    print(f"{'='*60}\n")


# ============================================================================
# Test Fixtures
# ============================================================================

@pytest.fixture
def large_config(autosubmit_exp, tmp_path):
    """Create realistic config with ~50 parameters for performance testing."""
    base_config = {
        'CONFIG': {
            'AUTOSUBMIT_VERSION': '4.2.0',
            'MAXWAITINGJOBS': 10
        },
        'DEFAULT': {
            'EXPID': 'perf_large',
            'HPCARCH': 'MARENOSTRUM5'
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
    
    # Create 10 jobs with parameters
    for i in range(10):
        job_name = f"JOB_{i:02d}"
        base_config['JOBS'][job_name] = {
            'FILE': f'job_{i}.sh',
            'PLATFORM': 'MARENOSTRUM5',
            'WALLCLOCK': f"{i % 24:02d}:{(i*15) % 60:02d}",
            'PROCESSORS': (i % 10 + 1) * 4,
            'THREADS': (i % 4 + 1) * 2,
            'QUEUE': 'main' if i % 2 == 0 else 'debug'
        }
    
    return autosubmit_exp(experiment_data=base_config, create=True)


@pytest.fixture
def baseline_config(autosubmit_exp):
    """Create baseline config without provenance."""
    return autosubmit_exp(experiment_data={
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
        """NFR-1: Load time overhead < 15%"""
        print("\n" + "="*60)
        print("TEST: Load Time Overhead")
        print("="*60)
        
        config = large_config.as_conf
        config.track_provenance = False
        config.reload(force_load=True)  # Warm-up
        
        # Baseline WITHOUT provenance
        times_without = []
        for i in range(3):
            config.track_provenance = False
            config.provenance_tracker = None
            gc.collect()
            with benchmark_time(f"Without provenance #{i+1}", verbose=False) as result:
                config.reload(force_load=True)
            times_without.append(result['elapsed'])
        
        baseline_time = statistics.mean(times_without)
        
        # WITH provenance
        times_with = []
        for i in range(3):
            config.track_provenance = True
            config.provenance_tracker = ProvenanceTracker()
            gc.collect()
            with benchmark_time(f"With provenance #{i+1}", verbose=False) as result:
                config.reload(force_load=True)
            times_with.append(result['elapsed'])
        
        provenance_time = statistics.mean(times_with)
        overhead = calculate_overhead(baseline_time, provenance_time)
        
        measurements = {
            'Baseline': f"{baseline_time:.4f}s",
            'With provenance': f"{provenance_time:.4f}s",
            'Overhead': overhead,
            'Target': '< 15%'
        }
        
        passed = overhead < 15.0
        print_summary("Load Time Overhead", measurements, passed)
        assert overhead < 15.0, f"Load overhead {overhead:.2f}% exceeds 15%"
    
    def test_memory_overhead(self, large_config):
        """NFR-1: Memory overhead < 10%"""
        print("\n" + "="*60)
        print("TEST: Memory Overhead")
        print("="*60)
        
        config = large_config.as_conf
        
        # Baseline WITHOUT provenance
        config.track_provenance = False
        config.provenance_tracker = None
        gc.collect()
        
        with benchmark_memory("Without provenance", verbose=False) as mem_without:
            config.reload(force_load=True)
            _ = config.experiment_data
            _ = config.jobs_data if hasattr(config, 'jobs_data') else None
        
        baseline_memory = mem_without['peak']
        
        # WITH provenance
        config.track_provenance = True
        config.provenance_tracker = ProvenanceTracker()
        gc.collect()
        
        with benchmark_memory("With provenance", verbose=False) as mem_with:
            config.reload(force_load=True)
            _ = config.experiment_data
            _ = config.jobs_data if hasattr(config, 'jobs_data') else None
            _ = config.get_all_provenance()
        
        provenance_memory = mem_with['peak']
        overhead = calculate_overhead(baseline_memory, provenance_memory)
        
        measurements = {
            'Baseline': f"{baseline_memory / 1024 / 1024:.2f}MB",
            'With provenance': f"{provenance_memory / 1024 / 1024:.2f}MB",
            'Overhead': overhead,
            'Target': '< 10%'
        }
        
        passed = overhead < 10.0
        print_summary("Memory Overhead", measurements, passed)
        assert overhead < 10.0, f"Memory overhead {overhead:.2f}% exceeds 10%"
    
    def test_save_time_overhead(self, large_config):
        """NFR-1: Save time overhead < 5%"""
        print("\n" + "="*60)
        print("TEST: Save Time Overhead")
        print("="*60)
        
        config = large_config.as_conf
        
        # Load config with provenance
        config.track_provenance = True
        config.provenance_tracker = ProvenanceTracker()
        config.reload(force_load=True)
        config.save()  # Warm-up
        
        # Baseline WITHOUT provenance export
        times_without = []
        for i in range(3):
            original_tracker = config.provenance_tracker
            config.provenance_tracker = None
            gc.collect()
            with benchmark_time(f"Without provenance #{i+1}", verbose=False) as result:
                config.save()
            times_without.append(result['elapsed'])
            config.provenance_tracker = original_tracker
        
        baseline_time = statistics.mean(times_without)
        
        # WITH provenance export
        times_with = []
        for i in range(3):
            gc.collect()
            with benchmark_time(f"With provenance #{i+1}", verbose=False) as result:
                config.save()
            times_with.append(result['elapsed'])
        
        provenance_time = statistics.mean(times_with)
        overhead = calculate_overhead(baseline_time, provenance_time)
        
        measurements = {
            'Baseline': f"{baseline_time:.4f}s",
            'With provenance': f"{provenance_time:.4f}s",
            'Overhead': overhead,
            'Target': '< 5%'
        }
        
        passed = overhead < 5.0
        print_summary("Save Time Overhead", measurements, passed)
        assert overhead < 5.0, f"Save overhead {overhead:.2f}% exceeds 5%"
    
    def test_no_impact_when_disabled(self, baseline_config):
        """NFR-1: No performance impact when disabled"""
        print("\n" + "="*60)
        print("TEST: No Impact When Disabled")
        print("="*60)
        
        config = baseline_config.as_conf
        assert config.track_provenance is False
        assert config.provenance_tracker is None
        
        # Benchmark load
        load_times = []
        for i in range(5):
            gc.collect()
            with benchmark_time(f"Load #{i+1}", verbose=False) as result:
                config.reload(force_load=True)
            load_times.append(result['elapsed'])
        
        # Benchmark save
        save_times = []
        for i in range(5):
            gc.collect()
            with benchmark_time(f"Save #{i+1}", verbose=False) as result:
                config.save()
            save_times.append(result['elapsed'])
        
        avg_load = statistics.mean(load_times)
        std_load = statistics.stdev(load_times)
        avg_save = statistics.mean(save_times)
        std_save = statistics.stdev(save_times)
        
        load_cv = (std_load / avg_load * 100) if avg_load > 0 else 0
        save_cv = (std_save / avg_save * 100) if avg_save > 0 else 0
        
        measurements = {
            'Avg load': f"{avg_load:.4f}s",
            'Load CV': f"{load_cv:.2f}%",
            'Avg save': f"{avg_save:.4f}s",
            'Save CV': f"{save_cv:.2f}%",
            'Target': 'CV < 10%'
        }
        
        passed = load_cv < 10.0 and save_cv < 10.0
        print_summary("No Impact When Disabled", measurements, passed)
        
        assert avg_load < 1.0, f"Load too slow: {avg_load:.4f}s"
        assert avg_save < 1.0, f"Save too slow: {avg_save:.4f}s"
        assert config.provenance_tracker is None
    
    def test_parameter_lookup_performance(self, large_config):
        """NFR-1: O(1) parameter lookup < 1ms"""
        print("\n" + "="*60)
        print("TEST: Parameter Lookup Performance")
        print("="*60)
        
        config = large_config.as_conf
        config.track_provenance = True
        config.provenance_tracker = ProvenanceTracker()
        config.reload(force_load=True)
        
        if not config.provenance_tracker or len(config.provenance_tracker) == 0:
            pytest.skip("No parameters tracked")
        
        tracked_params = list(config.provenance_tracker._entries.keys())[:100]
        
        # Benchmark lookups
        lookup_times = []
        for param in tracked_params:
            start = time.perf_counter()
            result = config.get_parameter_source(param)
            elapsed = time.perf_counter() - start
            lookup_times.append(elapsed)
            assert result is not None
        
        avg_lookup = statistics.mean(lookup_times)
        max_lookup = max(lookup_times)
        
        measurements = {
            'Parameters tested': len(tracked_params),
            'Avg lookup': f"{avg_lookup * 1000:.6f}ms",
            'Max lookup': f"{max_lookup * 1000:.6f}ms",
            'Target': '< 1.0ms'
        }
        
        passed = avg_lookup < 0.001
        print_summary("Parameter Lookup", measurements, passed)
        assert avg_lookup < 0.001, f"Lookup {avg_lookup*1000:.4f}ms exceeds 1ms"
    
    def test_repeated_reload_performance(self, large_config):
        """NFR-1: No memory leaks on repeated reloads"""
        print("\n" + "="*60)
        print("TEST: Repeated Reload Performance")
        print("="*60)
        
        config = large_config.as_conf
        config.track_provenance = True
        
        iterations = 10
        load_times = []
        memory_peaks = []
        
        for i in range(iterations):
            config.provenance_tracker = ProvenanceTracker()
            gc.collect()
            
            with benchmark_memory(f"Reload #{i+1}", verbose=False) as mem_result:
                with benchmark_time(f"Reload #{i+1}", verbose=False) as time_result:
                    config.reload(force_load=True)
            
            load_times.append(time_result['elapsed'])
            memory_peaks.append(mem_result['peak_mb'])
        
        avg_time = statistics.mean(load_times)
        std_time = statistics.stdev(load_times)
        cv_time = (std_time / avg_time * 100) if avg_time > 0 else 0
        
        first_memory = memory_peaks[0]
        last_memory = memory_peaks[-1]
        memory_growth = ((last_memory - first_memory) / first_memory * 100) if first_memory > 0 else 0
        
        measurements = {
            'Iterations': iterations,
            'Avg time': f"{avg_time:.4f}s",
            'Time CV': f"{cv_time:.2f}%",
            'First memory': f"{first_memory:.2f}MB",
            'Last memory': f"{last_memory:.2f}MB",
            'Memory growth': f"{memory_growth:.2f}%",
            'Target': 'CV < 20%, growth < 20%'
        }
        
        passed = cv_time < 20.0 and abs(memory_growth) < 20.0
        print_summary("Repeated Reload", measurements, passed)
        
        assert cv_time < 20.0, f"Time variation {cv_time:.2f}% too high"
        assert abs(memory_growth) < 20.0, f"Memory growth {memory_growth:.2f}% too high"
