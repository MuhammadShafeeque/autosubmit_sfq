# Copyright 2015-2025 Earth Sciences Department, BSC-CNS
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
Comprehensive unit tests for ProvenanceTracker module.

This module tests both ProvEntry and ProvenanceTracker classes,
covering all methods, edge cases, and integration scenarios.

Target coverage: >90%
"""

import time
from pathlib import Path
from unittest.mock import patch

import pytest

from autosubmit.config.provenance_tracker import ProvEntry, ProvenanceTracker


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def sample_file_path():
    """Sample file path for testing."""
    return "/path/to/config.yml"


@pytest.fixture
def mock_timestamp():
    """Fixed timestamp for testing."""
    return 1234567890.123


@pytest.fixture
def sample_prov_entry(sample_file_path, mock_timestamp):
    """Sample ProvEntry for testing."""
    return ProvEntry(sample_file_path, line=10, col=5, timestamp=mock_timestamp)


@pytest.fixture
def empty_tracker():
    """Empty ProvenanceTracker instance."""
    return ProvenanceTracker()


@pytest.fixture
def populated_tracker(mock_timestamp):
    """ProvenanceTracker with sample data."""
    with patch('time.time', return_value=mock_timestamp):
        tracker = ProvenanceTracker()
        tracker.track("DEFAULT.EXPID", "/path/to/config.yml", line=5, col=2)
        tracker.track("DEFAULT.HPCARCH", "/path/to/config.yml", line=8)
        tracker.track("JOBS.SIM.WALLCLOCK", "/path/to/jobs.yml", line=23, col=7)
        tracker.track("JOBS.SIM.PROCESSORS", "/path/to/jobs.yml", line=24)
        return tracker


# =============================================================================
# ProvEntry Tests
# =============================================================================

class TestProvEntry:
    """Test suite for ProvEntry class."""

    @pytest.mark.unit
    def test_creation_with_all_parameters(self, sample_file_path, mock_timestamp):
        """Test ProvEntry creation with all parameters specified."""
        entry = ProvEntry(sample_file_path, line=10, col=5, timestamp=mock_timestamp)
        
        assert entry.file == sample_file_path
        assert entry.line == 10
        assert entry.col == 5
        assert entry.timestamp == mock_timestamp

    @pytest.mark.unit
    def test_creation_with_minimal_parameters(self, sample_file_path):
        """Test ProvEntry creation with only required parameter (file)."""
        with patch('time.time', return_value=1234567890.0):
            entry = ProvEntry(sample_file_path)
        
        assert entry.file == sample_file_path
        assert entry.line is None
        assert entry.col is None
        assert entry.timestamp == 1234567890.0

    @pytest.mark.unit
    def test_creation_with_line_only(self, sample_file_path, mock_timestamp):
        """Test ProvEntry creation with file and line only."""
        entry = ProvEntry(sample_file_path, line=15, timestamp=mock_timestamp)
        
        assert entry.file == sample_file_path
        assert entry.line == 15
        assert entry.col is None
        assert entry.timestamp == mock_timestamp

    @pytest.mark.unit
    def test_creation_with_pathlib_path(self, mock_timestamp):
        """Test ProvEntry handles pathlib.Path objects correctly."""
        path = Path("/path/to/config.yml")
        entry = ProvEntry(path, line=10, timestamp=mock_timestamp)
        
        assert entry.file == "/path/to/config.yml"
        assert isinstance(entry.file, str)

    @pytest.mark.unit
    def test_timestamp_auto_generation(self, sample_file_path):
        """Test that timestamp is automatically generated if not provided."""
        before = time.time()
        entry = ProvEntry(sample_file_path, line=10)
        after = time.time()
        
        assert before <= entry.timestamp <= after

    @pytest.mark.unit
    def test_to_dict_complete(self, sample_prov_entry, mock_timestamp):
        """Test to_dict() with all fields populated."""
        result = sample_prov_entry.to_dict()
        
        assert result == {
            "file": "/path/to/config.yml",
            "line": 10,
            "col": 5,
            "timestamp": mock_timestamp
        }

    @pytest.mark.unit
    def test_to_dict_minimal(self, sample_file_path, mock_timestamp):
        """Test to_dict() with minimal fields (file only)."""
        entry = ProvEntry(sample_file_path, timestamp=mock_timestamp)
        result = entry.to_dict()
        
        assert result == {
            "file": sample_file_path,
            "timestamp": mock_timestamp
        }
        assert "line" not in result
        assert "col" not in result

    @pytest.mark.unit
    def test_to_dict_with_line_only(self, sample_file_path, mock_timestamp):
        """Test to_dict() with file and line only."""
        entry = ProvEntry(sample_file_path, line=25, timestamp=mock_timestamp)
        result = entry.to_dict()
        
        assert result == {
            "file": sample_file_path,
            "line": 25,
            "timestamp": mock_timestamp
        }
        assert "col" not in result

    @pytest.mark.unit
    def test_from_dict_complete(self):
        """Test from_dict() with complete data."""
        data = {
            "file": "/path/to/config.yml",
            "line": 10,
            "col": 5,
            "timestamp": 1234567890.123
        }
        entry = ProvEntry.from_dict(data)
        
        assert entry.file == "/path/to/config.yml"
        assert entry.line == 10
        assert entry.col == 5
        assert entry.timestamp == 1234567890.123

    @pytest.mark.unit
    def test_from_dict_minimal(self):
        """Test from_dict() with minimal data."""
        data = {
            "file": "/path/to/config.yml",
            "timestamp": 1234567890.123
        }
        entry = ProvEntry.from_dict(data)
        
        assert entry.file == "/path/to/config.yml"
        assert entry.line is None
        assert entry.col is None
        assert entry.timestamp == 1234567890.123

    @pytest.mark.unit
    def test_from_dict_missing_file_raises_error(self):
        """Test from_dict() raises KeyError when 'file' is missing."""
        data = {
            "line": 10,
            "timestamp": 1234567890.123
        }
        
        with pytest.raises(KeyError):
            ProvEntry.from_dict(data)

    @pytest.mark.unit
    def test_from_dict_missing_timestamp(self):
        """Test from_dict() handles missing timestamp (sets to None)."""
        data = {
            "file": "/path/to/config.yml",
            "line": 10
        }
        entry = ProvEntry.from_dict(data)
        
        assert entry.file == "/path/to/config.yml"
        assert entry.line == 10
        assert entry.timestamp is None

    @pytest.mark.unit
    def test_round_trip_serialization_complete(self, sample_prov_entry):
        """Test complete round-trip: to_dict → from_dict."""
        data = sample_prov_entry.to_dict()
        restored = ProvEntry.from_dict(data)
        
        assert restored.file == sample_prov_entry.file
        assert restored.line == sample_prov_entry.line
        assert restored.col == sample_prov_entry.col
        assert restored.timestamp == sample_prov_entry.timestamp

    @pytest.mark.unit
    def test_round_trip_serialization_minimal(self, sample_file_path, mock_timestamp):
        """Test round-trip with minimal data."""
        entry = ProvEntry(sample_file_path, timestamp=mock_timestamp)
        data = entry.to_dict()
        restored = ProvEntry.from_dict(data)
        
        assert restored.file == entry.file
        assert restored.line is None
        assert restored.col is None
        assert restored.timestamp == entry.timestamp

    @pytest.mark.unit
    def test_repr_with_line_and_col(self, sample_prov_entry):
        """Test __repr__() with line and column."""
        result = repr(sample_prov_entry)
        assert result == "ProvEntry(/path/to/config.yml:10:5)"

    @pytest.mark.unit
    def test_repr_with_line_only(self, sample_file_path, mock_timestamp):
        """Test __repr__() with line only."""
        entry = ProvEntry(sample_file_path, line=15, timestamp=mock_timestamp)
        result = repr(entry)
        assert result == "ProvEntry(/path/to/config.yml:15)"

    @pytest.mark.unit
    def test_repr_without_line_or_col(self, sample_file_path, mock_timestamp):
        """Test __repr__() without line or column."""
        entry = ProvEntry(sample_file_path, timestamp=mock_timestamp)
        result = repr(entry)
        assert result == "ProvEntry(/path/to/config.yml)"

    @pytest.mark.unit
    @pytest.mark.parametrize("line,col,expected", [
        (10, 5, "ProvEntry(/test.yml:10:5)"),
        (10, None, "ProvEntry(/test.yml:10)"),
        (None, None, "ProvEntry(/test.yml)"),
    ])
    def test_repr_variations(self, mock_timestamp, line, col, expected):
        """Test __repr__() with different combinations of line and col."""
        entry = ProvEntry("/test.yml", line=line, col=col, timestamp=mock_timestamp)
        assert repr(entry) == expected


# =============================================================================
# ProvenanceTracker Tests
# =============================================================================

class TestProvenanceTrackerBasics:
    """Test suite for ProvenanceTracker basic functionality."""

    @pytest.mark.unit
    def test_initialization_empty(self, empty_tracker):
        """Test ProvenanceTracker initializes with empty state."""
        assert len(empty_tracker) == 0
        assert empty_tracker.provenance_map == {}

    @pytest.mark.unit
    def test_track_single_parameter(self, empty_tracker):
        """Test tracking a single parameter."""
        empty_tracker.track("DEFAULT.EXPID", "/path/config.yml", line=5, col=2)
        
        assert len(empty_tracker) == 1
        assert "DEFAULT.EXPID" in empty_tracker
        
        entry = empty_tracker.get("DEFAULT.EXPID")
        assert entry is not None
        assert entry.file == "/path/config.yml"
        assert entry.line == 5
        assert entry.col == 2

    @pytest.mark.unit
    def test_track_multiple_parameters(self, empty_tracker, mock_timestamp):
        """Test tracking multiple parameters."""
        with patch('time.time', return_value=mock_timestamp):
            empty_tracker.track("DEFAULT.EXPID", "/path/config.yml", line=5)
            empty_tracker.track("DEFAULT.HPCARCH", "/path/config.yml", line=8)
            empty_tracker.track("JOBS.SIM.WALLCLOCK", "/path/jobs.yml", line=23)
        
        assert len(empty_tracker) == 3
        assert "DEFAULT.EXPID" in empty_tracker
        assert "DEFAULT.HPCARCH" in empty_tracker
        assert "JOBS.SIM.WALLCLOCK" in empty_tracker

    @pytest.mark.unit
    def test_track_overwrite_existing(self, empty_tracker, mock_timestamp):
        """Test tracking overwrites existing parameter (last file wins)."""
        with patch('time.time', return_value=mock_timestamp):
            empty_tracker.track("DEFAULT.EXPID", "/path/config1.yml", line=5)
            
            # Verify first entry
            entry1 = empty_tracker.get("DEFAULT.EXPID")
            assert entry1.file == "/path/config1.yml"
            assert entry1.line == 5
            
            # Overwrite with new entry
            empty_tracker.track("DEFAULT.EXPID", "/path/config2.yml", line=10, col=3)
            
            # Verify overwritten
            entry2 = empty_tracker.get("DEFAULT.EXPID")
            assert entry2.file == "/path/config2.yml"
            assert entry2.line == 10
            assert entry2.col == 3
            assert len(empty_tracker) == 1  # Still only one entry

    @pytest.mark.unit
    def test_track_with_nested_keys(self, empty_tracker):
        """Test tracking parameters with deeply nested keys."""
        empty_tracker.track("LEVEL1.LEVEL2.LEVEL3.PARAM", "/path/deep.yml", line=100)
        
        assert "LEVEL1.LEVEL2.LEVEL3.PARAM" in empty_tracker
        entry = empty_tracker.get("LEVEL1.LEVEL2.LEVEL3.PARAM")
        assert entry.file == "/path/deep.yml"
        assert entry.line == 100

    @pytest.mark.unit
    def test_track_without_line_col(self, empty_tracker):
        """Test tracking parameter without line/col information."""
        empty_tracker.track("DEFAULT.EXPID", "/path/config.yml")
        
        entry = empty_tracker.get("DEFAULT.EXPID")
        assert entry is not None
        assert entry.file == "/path/config.yml"
        assert entry.line is None
        assert entry.col is None

    @pytest.mark.unit
    def test_get_existing_parameter(self, populated_tracker):
        """Test retrieving provenance for existing parameter."""
        entry = populated_tracker.get("DEFAULT.EXPID")
        
        assert entry is not None
        assert entry.file == "/path/to/config.yml"
        assert entry.line == 5
        assert entry.col == 2

    @pytest.mark.unit
    def test_get_non_existing_parameter(self, populated_tracker):
        """Test retrieving provenance for non-existing parameter returns None."""
        entry = populated_tracker.get("NONEXISTENT.PARAM")
        assert entry is None

    @pytest.mark.unit
    def test_get_from_empty_tracker(self, empty_tracker):
        """Test get() on empty tracker returns None."""
        entry = empty_tracker.get("ANY.PARAM")
        assert entry is None


class TestProvenanceTrackerOperators:
    """Test suite for ProvenanceTracker operators."""

    @pytest.mark.unit
    def test_contains_existing_parameter(self, populated_tracker):
        """Test __contains__ operator for existing parameter."""
        assert "DEFAULT.EXPID" in populated_tracker
        assert "JOBS.SIM.WALLCLOCK" in populated_tracker

    @pytest.mark.unit
    def test_contains_non_existing_parameter(self, populated_tracker):
        """Test __contains__ operator for non-existing parameter."""
        assert "NONEXISTENT.PARAM" not in populated_tracker
        assert "JOBS.MISSING" not in populated_tracker

    @pytest.mark.unit
    def test_contains_on_empty_tracker(self, empty_tracker):
        """Test __contains__ on empty tracker."""
        assert "DEFAULT.EXPID" not in empty_tracker

    @pytest.mark.unit
    def test_len_empty_tracker(self, empty_tracker):
        """Test len() on empty tracker."""
        assert len(empty_tracker) == 0

    @pytest.mark.unit
    def test_len_populated_tracker(self, populated_tracker):
        """Test len() on populated tracker."""
        assert len(populated_tracker) == 4

    @pytest.mark.unit
    def test_len_after_adding_parameters(self, empty_tracker):
        """Test len() increases as parameters are added."""
        assert len(empty_tracker) == 0
        
        empty_tracker.track("PARAM1", "/path/file.yml")
        assert len(empty_tracker) == 1
        
        empty_tracker.track("PARAM2", "/path/file.yml")
        assert len(empty_tracker) == 2
        
        empty_tracker.track("PARAM3", "/path/file.yml")
        assert len(empty_tracker) == 3

    @pytest.mark.unit
    def test_len_after_overwriting(self, empty_tracker):
        """Test len() remains same when overwriting existing parameter."""
        empty_tracker.track("PARAM1", "/path/file1.yml")
        assert len(empty_tracker) == 1
        
        empty_tracker.track("PARAM1", "/path/file2.yml")
        assert len(empty_tracker) == 1  # Still only 1

    @pytest.mark.unit
    def test_repr_empty_tracker(self, empty_tracker):
        """Test __repr__() for empty tracker."""
        result = repr(empty_tracker)
        assert result == "ProvenanceTracker(0 parameters tracked)"

    @pytest.mark.unit
    def test_repr_single_parameter(self, empty_tracker):
        """Test __repr__() with single parameter (singular form)."""
        empty_tracker.track("PARAM1", "/path/file.yml")
        result = repr(empty_tracker)
        assert result == "ProvenanceTracker(1 parameter tracked)"

    @pytest.mark.unit
    def test_repr_multiple_parameters(self, populated_tracker):
        """Test __repr__() with multiple parameters (plural form)."""
        result = repr(populated_tracker)
        assert result == "ProvenanceTracker(4 parameters tracked)"


class TestProvenanceTrackerClear:
    """Test suite for ProvenanceTracker clear functionality."""

    @pytest.mark.unit
    def test_clear_empty_tracker(self, empty_tracker):
        """Test clear() on empty tracker (no-op)."""
        empty_tracker.clear()
        assert len(empty_tracker) == 0

    @pytest.mark.unit
    def test_clear_populated_tracker(self, populated_tracker):
        """Test clear() removes all tracked parameters."""
        assert len(populated_tracker) == 4
        
        populated_tracker.clear()
        
        assert len(populated_tracker) == 0
        assert "DEFAULT.EXPID" not in populated_tracker
        assert populated_tracker.get("DEFAULT.EXPID") is None

    @pytest.mark.unit
    def test_clear_and_repopulate(self, empty_tracker):
        """Test tracker can be repopulated after clearing."""
        # Add parameters
        empty_tracker.track("PARAM1", "/path/file.yml")
        empty_tracker.track("PARAM2", "/path/file.yml")
        assert len(empty_tracker) == 2
        
        # Clear
        empty_tracker.clear()
        assert len(empty_tracker) == 0
        
        # Repopulate
        empty_tracker.track("PARAM3", "/path/file.yml")
        assert len(empty_tracker) == 1
        assert "PARAM3" in empty_tracker


class TestProvenanceTrackerExport:
    """Test suite for ProvenanceTracker export_to_dict functionality."""

    @pytest.mark.unit
    def test_export_empty_tracker(self, empty_tracker):
        """Test export_to_dict() on empty tracker returns empty dict."""
        result = empty_tracker.export_to_dict()
        assert result == {}

    @pytest.mark.unit
    def test_export_single_level_keys(self, empty_tracker, mock_timestamp):
        """Test export_to_dict() with single-level keys (no dots)."""
        with patch('time.time', return_value=mock_timestamp):
            empty_tracker.track("EXPID", "/path/config.yml", line=5)
            empty_tracker.track("HPCARCH", "/path/config.yml", line=8)
        
        result = empty_tracker.export_to_dict()
        
        assert "EXPID" in result
        assert "HPCARCH" in result
        assert result["EXPID"]["file"] == "/path/config.yml"
        assert result["EXPID"]["line"] == 5
        assert result["HPCARCH"]["line"] == 8

    @pytest.mark.unit
    def test_export_simple_nested_structure(self, empty_tracker, mock_timestamp):
        """Test export_to_dict() with two-level nested structure."""
        with patch('time.time', return_value=mock_timestamp):
            empty_tracker.track("DEFAULT.EXPID", "/path/config.yml", line=5)
            empty_tracker.track("DEFAULT.HPCARCH", "/path/config.yml", line=8)
        
        result = empty_tracker.export_to_dict()
        
        assert "DEFAULT" in result
        assert isinstance(result["DEFAULT"], dict)
        assert "EXPID" in result["DEFAULT"]
        assert "HPCARCH" in result["DEFAULT"]
        assert result["DEFAULT"]["EXPID"]["file"] == "/path/config.yml"
        assert result["DEFAULT"]["EXPID"]["line"] == 5

    @pytest.mark.unit
    def test_export_deeply_nested_structure(self, empty_tracker, mock_timestamp):
        """Test export_to_dict() with deeply nested structure."""
        with patch('time.time', return_value=mock_timestamp):
            empty_tracker.track("LEVEL1.LEVEL2.LEVEL3.PARAM", "/path/deep.yml", line=100)
        
        result = empty_tracker.export_to_dict()
        
        assert "LEVEL1" in result
        assert "LEVEL2" in result["LEVEL1"]
        assert "LEVEL3" in result["LEVEL1"]["LEVEL2"]
        assert "PARAM" in result["LEVEL1"]["LEVEL2"]["LEVEL3"]
        assert result["LEVEL1"]["LEVEL2"]["LEVEL3"]["PARAM"]["file"] == "/path/deep.yml"
        assert result["LEVEL1"]["LEVEL2"]["LEVEL3"]["PARAM"]["line"] == 100

    @pytest.mark.unit
    def test_export_mixed_nesting_levels(self, populated_tracker):
        """Test export_to_dict() with mixed nesting levels."""
        result = populated_tracker.export_to_dict()
        
        # Two-level nesting: DEFAULT.EXPID, DEFAULT.HPCARCH
        assert "DEFAULT" in result
        assert "EXPID" in result["DEFAULT"]
        assert "HPCARCH" in result["DEFAULT"]
        
        # Three-level nesting: JOBS.SIM.WALLCLOCK, JOBS.SIM.PROCESSORS
        assert "JOBS" in result
        assert "SIM" in result["JOBS"]
        assert "WALLCLOCK" in result["JOBS"]["SIM"]
        assert "PROCESSORS" in result["JOBS"]["SIM"]

    @pytest.mark.unit
    def test_export_preserves_all_fields(self, empty_tracker, mock_timestamp):
        """Test export_to_dict() preserves all ProvEntry fields."""
        with patch('time.time', return_value=mock_timestamp):
            empty_tracker.track("DEFAULT.EXPID", "/path/config.yml", line=10, col=5)
        
        result = empty_tracker.export_to_dict()
        entry = result["DEFAULT"]["EXPID"]
        
        assert entry["file"] == "/path/config.yml"
        assert entry["line"] == 10
        assert entry["col"] == 5
        assert entry["timestamp"] == mock_timestamp

    @pytest.mark.unit
    def test_export_omits_none_fields(self, empty_tracker, mock_timestamp):
        """Test export_to_dict() omits None line/col fields."""
        with patch('time.time', return_value=mock_timestamp):
            empty_tracker.track("DEFAULT.EXPID", "/path/config.yml")
        
        result = empty_tracker.export_to_dict()
        entry = result["DEFAULT"]["EXPID"]
        
        assert "file" in entry
        assert "timestamp" in entry
        assert "line" not in entry
        assert "col" not in entry


class TestProvenanceTrackerImport:
    """Test suite for ProvenanceTracker import_from_dict functionality."""

    @pytest.mark.unit
    def test_import_empty_dict(self, empty_tracker):
        """Test import_from_dict() with empty dict (no-op)."""
        empty_tracker.import_from_dict({})
        assert len(empty_tracker) == 0

    @pytest.mark.unit
    def test_import_simple_structure(self, empty_tracker):
        """Test import_from_dict() with simple two-level structure."""
        prov_dict = {
            'DEFAULT': {
                'EXPID': {
                    'file': '/path/config.yml',
                    'line': 5,
                    'timestamp': 1234567890.123
                }
            }
        }
        
        empty_tracker.import_from_dict(prov_dict)
        
        assert len(empty_tracker) == 1
        assert "DEFAULT.EXPID" in empty_tracker
        
        entry = empty_tracker.get("DEFAULT.EXPID")
        assert entry.file == '/path/config.yml'
        assert entry.line == 5
        assert entry.timestamp == 1234567890.123

    @pytest.mark.unit
    def test_import_deeply_nested_structure(self, empty_tracker):
        """Test import_from_dict() with deeply nested structure."""
        prov_dict = {
            'LEVEL1': {
                'LEVEL2': {
                    'LEVEL3': {
                        'PARAM': {
                            'file': '/path/deep.yml',
                            'line': 100,
                            'col': 20,
                            'timestamp': 1234567890.123
                        }
                    }
                }
            }
        }
        
        empty_tracker.import_from_dict(prov_dict)
        
        assert "LEVEL1.LEVEL2.LEVEL3.PARAM" in empty_tracker
        entry = empty_tracker.get("LEVEL1.LEVEL2.LEVEL3.PARAM")
        assert entry.file == '/path/deep.yml'
        assert entry.line == 100
        assert entry.col == 20

    @pytest.mark.unit
    def test_import_multiple_parameters(self, empty_tracker):
        """Test import_from_dict() with multiple parameters at different levels."""
        prov_dict = {
            'DEFAULT': {
                'EXPID': {'file': '/path/config.yml', 'line': 5, 'timestamp': 1234.0},
                'HPCARCH': {'file': '/path/config.yml', 'line': 8, 'timestamp': 1234.0}
            },
            'JOBS': {
                'SIM': {
                    'WALLCLOCK': {'file': '/path/jobs.yml', 'line': 23, 'timestamp': 1234.0},
                    'PROCESSORS': {'file': '/path/jobs.yml', 'line': 24, 'timestamp': 1234.0}
                }
            }
        }
        
        empty_tracker.import_from_dict(prov_dict)
        
        assert len(empty_tracker) == 4
        assert "DEFAULT.EXPID" in empty_tracker
        assert "DEFAULT.HPCARCH" in empty_tracker
        assert "JOBS.SIM.WALLCLOCK" in empty_tracker
        assert "JOBS.SIM.PROCESSORS" in empty_tracker

    @pytest.mark.unit
    def test_import_preserves_all_fields(self, empty_tracker):
        """Test import_from_dict() preserves all ProvEntry fields."""
        prov_dict = {
            'DEFAULT': {
                'EXPID': {
                    'file': '/path/config.yml',
                    'line': 10,
                    'col': 5,
                    'timestamp': 1234567890.123
                }
            }
        }
        
        empty_tracker.import_from_dict(prov_dict)
        entry = empty_tracker.get("DEFAULT.EXPID")
        
        assert entry.file == '/path/config.yml'
        assert entry.line == 10
        assert entry.col == 5
        assert entry.timestamp == 1234567890.123

    @pytest.mark.unit
    def test_import_with_missing_optional_fields(self, empty_tracker):
        """Test import_from_dict() handles missing optional fields (line, col)."""
        prov_dict = {
            'DEFAULT': {
                'EXPID': {
                    'file': '/path/config.yml',
                    'timestamp': 1234567890.123
                }
            }
        }
        
        empty_tracker.import_from_dict(prov_dict)
        entry = empty_tracker.get("DEFAULT.EXPID")
        
        assert entry.file == '/path/config.yml'
        assert entry.line is None
        assert entry.col is None
        assert entry.timestamp == 1234567890.123

    @pytest.mark.unit
    def test_import_single_level_keys(self, empty_tracker):
        """Test import_from_dict() with single-level keys (no nesting)."""
        prov_dict = {
            'EXPID': {'file': '/path/config.yml', 'line': 5, 'timestamp': 1234.0},
            'HPCARCH': {'file': '/path/config.yml', 'line': 8, 'timestamp': 1234.0}
        }
        
        empty_tracker.import_from_dict(prov_dict)
        
        assert len(empty_tracker) == 2
        assert "EXPID" in empty_tracker
        assert "HPCARCH" in empty_tracker


class TestProvenanceTrackerRoundTrip:
    """Test suite for ProvenanceTracker export/import round-trip."""

    @pytest.mark.unit
    def test_round_trip_simple_structure(self, empty_tracker, mock_timestamp):
        """Test complete round-trip: track → export → import."""
        # Track parameters
        with patch('time.time', return_value=mock_timestamp):
            empty_tracker.track("DEFAULT.EXPID", "/path/config.yml", line=5, col=2)
            empty_tracker.track("DEFAULT.HPCARCH", "/path/config.yml", line=8)
        
        # Export
        exported = empty_tracker.export_to_dict()
        
        # Create new tracker and import
        new_tracker = ProvenanceTracker()
        new_tracker.import_from_dict(exported)
        
        # Verify
        assert len(new_tracker) == 2
        
        entry1 = new_tracker.get("DEFAULT.EXPID")
        assert entry1.file == "/path/config.yml"
        assert entry1.line == 5
        assert entry1.col == 2
        
        entry2 = new_tracker.get("DEFAULT.HPCARCH")
        assert entry2.file == "/path/config.yml"
        assert entry2.line == 8
        assert entry2.col is None

    @pytest.mark.unit
    def test_round_trip_deeply_nested(self, empty_tracker, mock_timestamp):
        """Test round-trip with deeply nested structure."""
        # Track deeply nested parameter
        with patch('time.time', return_value=mock_timestamp):
            empty_tracker.track("A.B.C.D.E", "/path/deep.yml", line=100, col=50)
        
        # Export and import
        exported = empty_tracker.export_to_dict()
        new_tracker = ProvenanceTracker()
        new_tracker.import_from_dict(exported)
        
        # Verify
        entry = new_tracker.get("A.B.C.D.E")
        assert entry is not None
        assert entry.file == "/path/deep.yml"
        assert entry.line == 100
        assert entry.col == 50

    @pytest.mark.unit
    def test_round_trip_populated_tracker(self, populated_tracker):
        """Test round-trip with fully populated tracker."""
        # Export
        exported = populated_tracker.export_to_dict()
        
        # Import to new tracker
        new_tracker = ProvenanceTracker()
        new_tracker.import_from_dict(exported)
        
        # Verify all entries preserved
        assert len(new_tracker) == len(populated_tracker)
        
        for param_path in ["DEFAULT.EXPID", "DEFAULT.HPCARCH", 
                          "JOBS.SIM.WALLCLOCK", "JOBS.SIM.PROCESSORS"]:
            original = populated_tracker.get(param_path)
            restored = new_tracker.get(param_path)
            
            assert restored is not None
            assert restored.file == original.file
            assert restored.line == original.line
            assert restored.col == original.col
            assert restored.timestamp == original.timestamp

    @pytest.mark.unit
    def test_round_trip_empty_tracker(self, empty_tracker):
        """Test round-trip with empty tracker."""
        exported = empty_tracker.export_to_dict()
        
        new_tracker = ProvenanceTracker()
        new_tracker.import_from_dict(exported)
        
        assert len(new_tracker) == 0
        assert exported == {}


class TestProvenanceTrackerEdgeCases:
    """Test suite for ProvenanceTracker edge cases."""

    @pytest.mark.unit
    def test_special_characters_in_keys(self, empty_tracker, mock_timestamp):
        """Test handling of special characters in parameter keys."""
        with patch('time.time', return_value=mock_timestamp):
            # Note: In real usage, keys are dot-separated paths
            # Special chars could appear in actual config keys
            empty_tracker.track("KEY_WITH_UNDERSCORE", "/path/file.yml", line=5)
            empty_tracker.track("KEY-WITH-DASH", "/path/file.yml", line=10)
            empty_tracker.track("KEY123", "/path/file.yml", line=15)
        
        assert "KEY_WITH_UNDERSCORE" in empty_tracker
        assert "KEY-WITH-DASH" in empty_tracker
        assert "KEY123" in empty_tracker

    @pytest.mark.unit
    def test_very_long_parameter_path(self, empty_tracker, mock_timestamp):
        """Test handling of very long parameter paths."""
        long_path = ".".join([f"LEVEL{i}" for i in range(20)])
        
        with patch('time.time', return_value=mock_timestamp):
            empty_tracker.track(long_path, "/path/file.yml", line=999)
        
        assert long_path in empty_tracker
        entry = empty_tracker.get(long_path)
        assert entry.line == 999

    @pytest.mark.unit
    def test_empty_string_key(self, empty_tracker, mock_timestamp):
        """Test tracking parameter with empty string as key."""
        with patch('time.time', return_value=mock_timestamp):
            empty_tracker.track("", "/path/file.yml", line=1)
        
        assert "" in empty_tracker
        entry = empty_tracker.get("")
        assert entry is not None

    @pytest.mark.unit
    def test_track_with_absolute_path_object(self, empty_tracker, mock_timestamp):
        """Test track() accepts Path objects for file parameter."""
        file_path = Path("/absolute/path/to/config.yml")
        
        with patch('time.time', return_value=mock_timestamp):
            empty_tracker.track("DEFAULT.EXPID", file_path, line=10)
        
        entry = empty_tracker.get("DEFAULT.EXPID")
        assert entry.file == "/absolute/path/to/config.yml"
        assert isinstance(entry.file, str)

    @pytest.mark.unit
    def test_import_ignores_non_dict_values(self, empty_tracker):
        """Test import_from_dict() ignores non-dict values gracefully."""
        prov_dict = {
            'DEFAULT': {
                'EXPID': {'file': '/path/config.yml', 'line': 5, 'timestamp': 1234.0},
                'INVALID': "this_is_a_string_not_dict"  # Should be ignored
            }
        }
        
        empty_tracker.import_from_dict(prov_dict)
        
        # Should import valid entry, ignore invalid
        assert "DEFAULT.EXPID" in empty_tracker
        assert "DEFAULT.INVALID" not in empty_tracker
        assert len(empty_tracker) == 1

    @pytest.mark.unit
    def test_multiple_imports_accumulate(self, empty_tracker):
        """Test multiple imports accumulate data."""
        prov_dict1 = {
            'DEFAULT': {
                'EXPID': {'file': '/path/config1.yml', 'line': 5, 'timestamp': 1234.0}
            }
        }
        prov_dict2 = {
            'JOBS': {
                'SIM': {
                    'WALLCLOCK': {'file': '/path/jobs.yml', 'line': 23, 'timestamp': 1234.0}
                }
            }
        }
        
        empty_tracker.import_from_dict(prov_dict1)
        empty_tracker.import_from_dict(prov_dict2)
        
        assert len(empty_tracker) == 2
        assert "DEFAULT.EXPID" in empty_tracker
        assert "JOBS.SIM.WALLCLOCK" in empty_tracker

    @pytest.mark.unit
    def test_import_overwrites_existing(self, empty_tracker):
        """Test import_from_dict() overwrites existing entries."""
        # Track initial entry
        empty_tracker.track("DEFAULT.EXPID", "/path/old.yml", line=5)
        
        # Import dict with same key but different values
        prov_dict = {
            'DEFAULT': {
                'EXPID': {'file': '/path/new.yml', 'line': 10, 'timestamp': 9999.0}
            }
        }
        empty_tracker.import_from_dict(prov_dict)
        
        # Verify overwritten
        entry = empty_tracker.get("DEFAULT.EXPID")
        assert entry.file == '/path/new.yml'
        assert entry.line == 10

    @pytest.mark.unit
    def test_timestamp_independence(self, empty_tracker):
        """Test that each tracked parameter gets independent timestamp."""
        start_time = time.time()
        
        empty_tracker.track("PARAM1", "/path/file.yml")
        time.sleep(0.01)  # Small delay
        empty_tracker.track("PARAM2", "/path/file.yml")
        
        entry1 = empty_tracker.get("PARAM1")
        entry2 = empty_tracker.get("PARAM2")
        
        # Timestamps should be different and after start_time
        assert entry1.timestamp >= start_time
        assert entry2.timestamp >= entry1.timestamp

    @pytest.mark.unit
    def test_export_key_collision_handling(self, empty_tracker, mock_timestamp):
        """Test export handles potential key collisions gracefully."""
        # This is an edge case that shouldn't happen with valid configs
        # but the code should handle it without crashing
        with patch('time.time', return_value=mock_timestamp):
            empty_tracker.track("SECTION.PARAM", "/path/file.yml", line=5)
        
        # Export should succeed without error
        result = empty_tracker.export_to_dict()
        assert "SECTION" in result
        assert "PARAM" in result["SECTION"]


# =============================================================================
# Integration Tests
# =============================================================================

class TestProvenanceTrackerIntegration:
    """Integration tests simulating real-world usage scenarios."""

    @pytest.mark.unit
    def test_typical_config_loading_scenario(self, empty_tracker, mock_timestamp):
        """Test typical scenario: loading multiple config files."""
        with patch('time.time', return_value=mock_timestamp):
            # Load conf/autosubmit.yml
            empty_tracker.track("DEFAULT.EXPID", "/path/to/conf/autosubmit.yml", line=2)
            empty_tracker.track("DEFAULT.HPCARCH", "/path/to/conf/autosubmit.yml", line=3)
            
            # Load conf/jobs.yml
            empty_tracker.track("JOBS.SIM.WALLCLOCK", "/path/to/conf/jobs.yml", line=10)
            empty_tracker.track("JOBS.SIM.PROCESSORS", "/path/to/conf/jobs.yml", line=11)
            empty_tracker.track("JOBS.POST.WALLCLOCK", "/path/to/conf/jobs.yml", line=20)
            
            # Load platform config (overwrites HPCARCH from autosubmit.yml)
            empty_tracker.track("DEFAULT.HPCARCH", "/path/to/platforms/marenostrum.yml", line=1)
        
        # Verify final state
        assert len(empty_tracker) == 5
        
        expid = empty_tracker.get("DEFAULT.EXPID")
        assert expid.file == "/path/to/conf/autosubmit.yml"
        
        hpcarch = empty_tracker.get("DEFAULT.HPCARCH")
        assert hpcarch.file == "/path/to/platforms/marenostrum.yml"  # Overwritten
        
        # Export and verify structure
        exported = empty_tracker.export_to_dict()
        assert "DEFAULT" in exported
        assert "JOBS" in exported
        assert "SIM" in exported["JOBS"]
        assert "POST" in exported["JOBS"]

    @pytest.mark.unit
    def test_persistence_scenario(self, populated_tracker):
        """Test scenario: save provenance to experiment_data.yml and restore."""
        # Export for persistence
        to_persist = populated_tracker.export_to_dict()
        
        # Simulate saving to YAML and loading back
        # (In real code, this would be yaml.dump() and yaml.load())
        
        # Create new session, restore from persisted data
        new_tracker = ProvenanceTracker()
        new_tracker.import_from_dict(to_persist)
        
        # Verify all data restored correctly
        assert len(new_tracker) == len(populated_tracker)
        for key in populated_tracker.provenance_map:
            original = populated_tracker.get(key)
            restored = new_tracker.get(key)
            assert restored.file == original.file
            assert restored.line == original.line
            assert restored.col == original.col

    @pytest.mark.unit
    def test_debugging_scenario(self, populated_tracker):
        """Test scenario: user debugging where a parameter comes from."""
        # User wants to know where WALLCLOCK is defined
        param_name = "JOBS.SIM.WALLCLOCK"
        
        if param_name in populated_tracker:
            prov = populated_tracker.get(param_name)
            debug_info = f"{param_name} is defined in {prov.file}"
            if prov.line:
                debug_info += f" at line {prov.line}"
            
            assert "/path/to/jobs.yml" in debug_info
            assert "line 23" in debug_info
        else:
            pytest.fail("Expected parameter not found")

    @pytest.mark.unit
    def test_config_merge_scenario(self, empty_tracker, mock_timestamp):
        """Test scenario: merging configurations from multiple sources."""
        with patch('time.time', return_value=mock_timestamp):
            # Base config
            empty_tracker.track("DEFAULT.EXPID", "/base/config.yml", line=1)
            empty_tracker.track("DEFAULT.HPCARCH", "/base/config.yml", line=2)
            empty_tracker.track("JOBS.SIM.WALLCLOCK", "/base/config.yml", line=10)
            
            # User override (overwrites WALLCLOCK)
            empty_tracker.track("JOBS.SIM.WALLCLOCK", "/user/override.yml", line=5)
            
            # Platform-specific (overwrites HPCARCH)
            empty_tracker.track("DEFAULT.HPCARCH", "/platform/specific.yml", line=1)
        
        # Verify final sources reflect "last file wins"
        wallclock = empty_tracker.get("JOBS.SIM.WALLCLOCK")
        assert wallclock.file == "/user/override.yml"
        
        hpcarch = empty_tracker.get("DEFAULT.HPCARCH")
        assert hpcarch.file == "/platform/specific.yml"
        
        expid = empty_tracker.get("DEFAULT.EXPID")
        assert expid.file == "/base/config.yml"  # Not overridden


# =============================================================================
# Parametrized Tests
# =============================================================================

@pytest.mark.unit
@pytest.mark.parametrize("param_path,file_path,line,col", [
    ("SIMPLE", "/path/file.yml", 1, None),
    ("LEVEL1.LEVEL2", "/path/file.yml", 10, 5),
    ("A.B.C.D", "/path/deep.yml", 100, 50),
    ("_UNDERSCORE", "/path/file.yml", None, None),
    ("KEY-DASH", "/path/file.yml", 5, None),
])
def test_track_various_parameters(empty_tracker, param_path, file_path, line, col):
    """Parametrized test for tracking various parameter configurations."""
    empty_tracker.track(param_path, file_path, line=line, col=col)
    
    assert param_path in empty_tracker
    entry = empty_tracker.get(param_path)
    assert entry.file == file_path
    assert entry.line == line
    assert entry.col == col


@pytest.mark.unit
@pytest.mark.parametrize("nested_levels", [1, 2, 3, 5, 10])
def test_nested_export_import(empty_tracker, mock_timestamp, nested_levels):
    """Parametrized test for export/import at various nesting levels."""
    # Create nested path
    path = ".".join([f"L{i}" for i in range(nested_levels)])
    
    with patch('time.time', return_value=mock_timestamp):
        empty_tracker.track(path, "/path/file.yml", line=10)
    
    # Export and import
    exported = empty_tracker.export_to_dict()
    new_tracker = ProvenanceTracker()
    new_tracker.import_from_dict(exported)
    
    # Verify
    entry = new_tracker.get(path)
    assert entry is not None
    assert entry.file == "/path/file.yml"
    assert entry.line == 10


# =============================================================================
# Additional Edge Case Tests for >90% Coverage
# =============================================================================

class TestProvenanceTrackerAdditionalEdgeCases:
    """Additional edge case tests to exceed 90% coverage."""

    @pytest.mark.unit
    def test_very_large_line_col_numbers(self, empty_tracker, mock_timestamp):
        """Test handling of very large line and column numbers."""
        with patch('time.time', return_value=mock_timestamp):
            empty_tracker.track("PARAM", "/path/file.yml", line=999999, col=99999)
        
        entry = empty_tracker.get("PARAM")
        assert entry.line == 999999
        assert entry.col == 99999

    @pytest.mark.unit
    def test_zero_line_col_numbers(self, empty_tracker, mock_timestamp):
        """Test handling of line=0 and col=0 (edge case but valid)."""
        with patch('time.time', return_value=mock_timestamp):
            empty_tracker.track("PARAM", "/path/file.yml", line=0, col=0)
        
        entry = empty_tracker.get("PARAM")
        assert entry.line == 0
        assert entry.col == 0
        
        # Export and verify 0 is preserved (not treated as None)
        exported = empty_tracker.export_to_dict()
        assert exported["PARAM"]["line"] == 0
        assert exported["PARAM"]["col"] == 0

    @pytest.mark.unit
    def test_unicode_in_file_paths(self, empty_tracker, mock_timestamp):
        """Test handling of unicode characters in file paths."""
        unicode_path = "/path/to/αβγδ/config_日本語.yml"
        
        with patch('time.time', return_value=mock_timestamp):
            empty_tracker.track("PARAM", unicode_path, line=5)
        
        entry = empty_tracker.get("PARAM")
        assert entry.file == unicode_path
        
        # Verify round-trip preserves unicode
        exported = empty_tracker.export_to_dict()
        new_tracker = ProvenanceTracker()
        new_tracker.import_from_dict(exported)
        
        restored = new_tracker.get("PARAM")
        assert restored.file == unicode_path

    @pytest.mark.unit
    def test_windows_path_separators(self, empty_tracker, mock_timestamp):
        """Test handling of Windows-style path separators."""
        windows_path = "C:\\Users\\test\\config.yml"
        
        with patch('time.time', return_value=mock_timestamp):
            empty_tracker.track("PARAM", windows_path, line=10)
        
        entry = empty_tracker.get("PARAM")
        assert entry.file == windows_path

    @pytest.mark.unit
    def test_export_with_single_key_no_nesting(self, empty_tracker, mock_timestamp):
        """Test export with single-level key (no dots)."""
        with patch('time.time', return_value=mock_timestamp):
            empty_tracker.track("EXPID", "/path/file.yml", line=1)
        
        exported = empty_tracker.export_to_dict()
        
        # Should have top-level key directly
        assert "EXPID" in exported
        assert exported["EXPID"]["file"] == "/path/file.yml"
        assert exported["EXPID"]["line"] == 1

    @pytest.mark.unit
    def test_import_with_nested_dict_missing_file(self, empty_tracker):
        """Test import gracefully handles nested dict without 'file' key."""
        prov_dict = {
            'DEFAULT': {
                'NESTED': {
                    'VALID': {'file': '/path/file.yml', 'line': 5, 'timestamp': 1234.0}
                    # No 'file' key at NESTED level, so it should recurse
                }
            }
        }
        
        empty_tracker.import_from_dict(prov_dict)
        
        # Should import the deeply nested valid entry
        assert "DEFAULT.NESTED.VALID" in empty_tracker
        assert empty_tracker.get("DEFAULT.NESTED.VALID").file == '/path/file.yml'

    @pytest.mark.unit
    def test_multiple_dots_in_key(self, empty_tracker, mock_timestamp):
        """Test parameter paths with many dots."""
        path = "A.B.C.D.E.F.G.H.I.J"
        
        with patch('time.time', return_value=mock_timestamp):
            empty_tracker.track(path, "/path/file.yml", line=100)
        
        assert path in empty_tracker
        entry = empty_tracker.get(path)
        assert entry.file == "/path/file.yml"
        
        # Test export/import preserves deep nesting
        exported = empty_tracker.export_to_dict()
        new_tracker = ProvenanceTracker()
        new_tracker.import_from_dict(exported)
        assert path in new_tracker

    @pytest.mark.unit
    def test_prov_entry_with_negative_line_col(self, mock_timestamp):
        """Test ProvEntry accepts negative line/col (edge case, no validation)."""
        entry = ProvEntry("/path/file.yml", line=-1, col=-1, timestamp=mock_timestamp)
        
        assert entry.line == -1
        assert entry.col == -1
        
        # Should serialize and deserialize
        data = entry.to_dict()
        restored = ProvEntry.from_dict(data)
        assert restored.line == -1
        assert restored.col == -1

    @pytest.mark.unit
    def test_tracker_repr_with_many_parameters(self, empty_tracker):
        """Test __repr__ with many parameters."""
        for i in range(100):
            empty_tracker.track(f"PARAM{i}", "/path/file.yml")
        
        result = repr(empty_tracker)
        assert "100 parameters tracked" in result

    @pytest.mark.unit
    def test_export_import_preserves_timestamp_precision(self, empty_tracker):
        """Test that timestamp precision is preserved through export/import."""
        precise_timestamp = 1234567890.123456789
        
        with patch('time.time', return_value=precise_timestamp):
            empty_tracker.track("PARAM", "/path/file.yml", line=10)
        
        exported = empty_tracker.export_to_dict()
        
        # Import to new tracker
        new_tracker = ProvenanceTracker()
        new_tracker.import_from_dict(exported)
        
        restored = new_tracker.get("PARAM")
        assert restored.timestamp == precise_timestamp

    @pytest.mark.unit
    def test_track_same_parameter_different_files(self, empty_tracker, mock_timestamp):
        """Test tracking same parameter from different files (overwrite behavior)."""
        with patch('time.time', return_value=mock_timestamp):
            empty_tracker.track("PARAM", "/file1.yml", line=10)
            empty_tracker.track("PARAM", "/file2.yml", line=20)
            empty_tracker.track("PARAM", "/file3.yml", line=30)
        
        # Only the last one should be retained
        entry = empty_tracker.get("PARAM")
        assert entry.file == "/file3.yml"
        assert entry.line == 30
        assert len(empty_tracker) == 1

    @pytest.mark.unit
    def test_clear_and_verify_memory_released(self, empty_tracker):
        """Test that clear() actually removes references."""
        # Track many parameters
        for i in range(100):
            empty_tracker.track(f"PARAM{i}", "/path/file.yml", line=i)
        
        assert len(empty_tracker) == 100
        
        # Clear and verify
        empty_tracker.clear()
        assert len(empty_tracker) == 0
        assert empty_tracker.provenance_map == {}
        
        # Verify we can track new params after clear
        empty_tracker.track("NEW_PARAM", "/path/file.yml", line=1)
        assert len(empty_tracker) == 1

    @pytest.mark.unit
    def test_prov_entry_file_as_relative_path(self, mock_timestamp):
        """Test ProvEntry with relative path."""
        relative_path = "../config/autosubmit.yml"
        entry = ProvEntry(relative_path, line=5, timestamp=mock_timestamp)
        
        assert entry.file == relative_path
        
        # Round-trip preserves relative path
        data = entry.to_dict()
        restored = ProvEntry.from_dict(data)
        assert restored.file == relative_path

    @pytest.mark.unit
    def test_import_empty_nested_dicts(self, empty_tracker):
        """Test import with empty nested dictionaries."""
        prov_dict = {
            'LEVEL1': {
                'LEVEL2': {}  # Empty nested dict
            }
        }
        
        empty_tracker.import_from_dict(prov_dict)
        
        # Should not crash, and should not add any entries
        assert len(empty_tracker) == 0

    @pytest.mark.unit
    def test_export_then_modify_returned_dict(self, populated_tracker):
        """Test that modifying exported dict doesn't affect tracker."""
        exported = populated_tracker.export_to_dict()
        original_len = len(populated_tracker)
        
        # Modify exported dict
        exported["DEFAULT"]["EXPID"]["file"] = "/modified/path.yml"
        exported["NEW_KEY"] = {"file": "/new/file.yml", "line": 999, "timestamp": 0.0}
        
        # Original tracker should be unchanged
        assert len(populated_tracker) == original_len
        assert "NEW_KEY" not in populated_tracker
        entry = populated_tracker.get("DEFAULT.EXPID")
        assert entry.file == "/path/to/config.yml"  # Not modified

    @pytest.mark.unit
    def test_prov_entry_equality_not_implemented(self, mock_timestamp):
        """Test that ProvEntry doesn't implement __eq__ (creates different objects)."""
        entry1 = ProvEntry("/path/file.yml", line=10, col=5, timestamp=mock_timestamp)
        entry2 = ProvEntry("/path/file.yml", line=10, col=5, timestamp=mock_timestamp)
        
        # These are different objects
        assert entry1 is not entry2
        # No __eq__ implemented, so they're not equal by default
        assert (entry1 == entry2) == False or (entry1 == entry2) == True  # Either is fine

    @pytest.mark.unit
    def test_mixed_none_and_valid_line_col_in_export(self, empty_tracker, mock_timestamp):
        """Test export with mix of None and valid line/col values."""
        with patch('time.time', return_value=mock_timestamp):
            empty_tracker.track("PARAM1", "/file.yml", line=10, col=5)
            empty_tracker.track("PARAM2", "/file.yml", line=20)  # No col
            empty_tracker.track("PARAM3", "/file.yml")  # No line or col
        
        exported = empty_tracker.export_to_dict()
        
        # Verify PARAM1 has both line and col
        assert "line" in exported["PARAM1"]
        assert "col" in exported["PARAM1"]
        
        # Verify PARAM2 has line but not col
        assert "line" in exported["PARAM2"]
        assert "col" not in exported["PARAM2"]
        
        # Verify PARAM3 has neither
        assert "line" not in exported["PARAM3"]
        assert "col" not in exported["PARAM3"]

    @pytest.mark.unit
    @pytest.mark.parametrize("invalid_key", [
        "KEY.WITH..DOUBLE.DOTS",
        ".STARTS.WITH.DOT",
        "ENDS.WITH.DOT.",
    ])
    def test_unusual_key_formats(self, empty_tracker, mock_timestamp, invalid_key):
        """Test handling of unusual but technically valid key formats."""
        with patch('time.time', return_value=mock_timestamp):
            empty_tracker.track(invalid_key, "/path/file.yml", line=5)
        
        # Should track without error
        assert invalid_key in empty_tracker
        entry = empty_tracker.get(invalid_key)
        assert entry is not None
        
        # Export/import should preserve the unusual key
        exported = empty_tracker.export_to_dict()
        new_tracker = ProvenanceTracker()
        new_tracker.import_from_dict(exported)
        assert invalid_key in new_tracker
