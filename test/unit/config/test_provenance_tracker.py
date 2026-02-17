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

@pytest.mark.unit
def test_prov_entry_creation_with_all_parameters(sample_file_path, mock_timestamp):
    """Test ProvEntry creation with all parameters."""
    entry = ProvEntry(sample_file_path, line=10, col=5, timestamp=mock_timestamp)
    
    assert entry.file == sample_file_path
    assert entry.line == 10
    assert entry.col == 5
    assert entry.timestamp == mock_timestamp


@pytest.mark.unit
def test_prov_entry_creation_minimal(sample_file_path):
    """Test ProvEntry creation with only file parameter."""
    with patch('time.time', return_value=1234567890.0):
        entry = ProvEntry(sample_file_path)
    
    assert entry.file == sample_file_path
    assert entry.line is None
    assert entry.col is None
    assert entry.timestamp == 1234567890.0


@pytest.mark.unit
def test_prov_entry_handles_pathlib_path(mock_timestamp):
    """Test ProvEntry converts pathlib.Path to string."""
    path = Path("/path/to/config.yml")
    entry = ProvEntry(path, line=10, timestamp=mock_timestamp)
    
    assert entry.file == "/path/to/config.yml"
    assert isinstance(entry.file, str)


@pytest.mark.unit
def test_prov_entry_timestamp_auto_generation(sample_file_path):
    """Test timestamp is auto-generated if not provided."""
    before = time.time()
    entry = ProvEntry(sample_file_path, line=10)
    after = time.time()
    
    assert before <= entry.timestamp <= after


@pytest.mark.unit
def test_prov_entry_to_dict_complete(sample_prov_entry, mock_timestamp):
    """Test to_dict() with all fields."""
    result = sample_prov_entry.to_dict()
    
    assert result == {
        "file": "/path/to/config.yml",
        "line": 10,
        "col": 5,
        "timestamp": mock_timestamp
    }


@pytest.mark.unit
def test_prov_entry_to_dict_minimal(sample_file_path, mock_timestamp):
    """Test to_dict() with minimal fields."""
    entry = ProvEntry(sample_file_path, timestamp=mock_timestamp)
    result = entry.to_dict()
    
    assert result == {
        "file": sample_file_path,
        "timestamp": mock_timestamp
    }
    assert "line" not in result
    assert "col" not in result


@pytest.mark.unit
def test_prov_entry_from_dict_complete():
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
def test_prov_entry_from_dict_minimal():
    """Test from_dict() with minimal data."""
    data = {
        "file": "/path/to/config.yml",
        "timestamp": 1234567890.123
    }
    entry = ProvEntry.from_dict(data)
    
    assert entry.file == "/path/to/config.yml"
    assert entry.line is None
    assert entry.col is None


@pytest.mark.unit
def test_prov_entry_from_dict_missing_file_raises_error():
    """Test from_dict() raises KeyError when file missing."""
    data = {"line": 10, "timestamp": 1234567890.123}
    
    with pytest.raises(KeyError):
        ProvEntry.from_dict(data)


@pytest.mark.unit
def test_prov_entry_round_trip_complete(sample_prov_entry):
    """Test round-trip: to_dict → from_dict."""
    data = sample_prov_entry.to_dict()
    restored = ProvEntry.from_dict(data)
    
    assert restored.file == sample_prov_entry.file
    assert restored.line == sample_prov_entry.line
    assert restored.col == sample_prov_entry.col
    assert restored.timestamp == sample_prov_entry.timestamp


@pytest.mark.unit
def test_prov_entry_repr_with_line_and_col(sample_prov_entry):
    """Test __repr__() with line and column."""
    assert repr(sample_prov_entry) == "ProvEntry(/path/to/config.yml:10:5)"


@pytest.mark.unit
def test_prov_entry_repr_with_line_only(sample_file_path, mock_timestamp):
    """Test __repr__() with line only."""
    entry = ProvEntry(sample_file_path, line=15, timestamp=mock_timestamp)
    assert repr(entry) == "ProvEntry(/path/to/config.yml:15)"


@pytest.mark.unit
def test_prov_entry_repr_without_line_or_col(sample_file_path, mock_timestamp):
    """Test __repr__() without line or column."""
    entry = ProvEntry(sample_file_path, timestamp=mock_timestamp)
    assert repr(entry) == "ProvEntry(/path/to/config.yml)"


# =============================================================================
# ProvenanceTracker Tests
# =============================================================================

@pytest.mark.unit
def test_tracker_initialization_empty(empty_tracker):
    """Test tracker initializes empty."""
    assert len(empty_tracker) == 0
    assert empty_tracker.provenance_map == {}


@pytest.mark.unit
def test_tracker_track_single_parameter(empty_tracker):
    """Test tracking single parameter."""
    empty_tracker.track("DEFAULT.EXPID", "/path/config.yml", line=5, col=2)
    
    assert len(empty_tracker) == 1
    assert "DEFAULT.EXPID" in empty_tracker
    
    entry = empty_tracker.get("DEFAULT.EXPID")
    assert entry.file == "/path/config.yml"
    assert entry.line == 5
    assert entry.col == 2


@pytest.mark.unit
def test_tracker_track_multiple_parameters(empty_tracker, mock_timestamp):
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
def test_tracker_track_overwrite_existing(empty_tracker, mock_timestamp):
    """Test tracking overwrites existing parameter."""
    with patch('time.time', return_value=mock_timestamp):
        empty_tracker.track("DEFAULT.EXPID", "/path/config1.yml", line=5)
        entry1 = empty_tracker.get("DEFAULT.EXPID")
        assert entry1.file == "/path/config1.yml"
        
        empty_tracker.track("DEFAULT.EXPID", "/path/config2.yml", line=10, col=3)
        entry2 = empty_tracker.get("DEFAULT.EXPID")
        assert entry2.file == "/path/config2.yml"
        assert entry2.line == 10
        assert len(empty_tracker) == 1


@pytest.mark.unit
def test_tracker_track_nested_keys(empty_tracker):
    """Test tracking deeply nested keys."""
    empty_tracker.track("LEVEL1.LEVEL2.LEVEL3.PARAM", "/path/deep.yml", line=100)
    
    assert "LEVEL1.LEVEL2.LEVEL3.PARAM" in empty_tracker
    entry = empty_tracker.get("LEVEL1.LEVEL2.LEVEL3.PARAM")
    assert entry.file == "/path/deep.yml"
    assert entry.line == 100


@pytest.mark.unit
def test_tracker_get_existing_parameter(populated_tracker):
    """Test get() returns entry for existing parameter."""
    entry = populated_tracker.get("DEFAULT.EXPID")
    
    assert entry is not None
    assert entry.file == "/path/to/config.yml"
    assert entry.line == 5
    assert entry.col == 2


@pytest.mark.unit
def test_tracker_get_non_existing_parameter(populated_tracker):
    """Test get() returns None for non-existing parameter."""
    entry = populated_tracker.get("NONEXISTENT.PARAM")
    assert entry is None


@pytest.mark.unit
def test_tracker_contains_operator(populated_tracker):
    """Test __contains__ operator."""
    assert "DEFAULT.EXPID" in populated_tracker
    assert "JOBS.SIM.WALLCLOCK" in populated_tracker
    assert "NONEXISTENT.PARAM" not in populated_tracker


@pytest.mark.unit
def test_tracker_len_operator(empty_tracker):
    """Test len() operator."""
    assert len(empty_tracker) == 0
    
    empty_tracker.track("PARAM1", "/path/file.yml")
    assert len(empty_tracker) == 1
    
    empty_tracker.track("PARAM2", "/path/file.yml")
    assert len(empty_tracker) == 2


@pytest.mark.unit
def test_tracker_repr(empty_tracker):
    """Test __repr__() output."""
    assert repr(empty_tracker) == "ProvenanceTracker(0 parameters tracked)"
    
    empty_tracker.track("PARAM1", "/path/file.yml")
    assert repr(empty_tracker) == "ProvenanceTracker(1 parameter tracked)"
    
    empty_tracker.track("PARAM2", "/path/file.yml")
    assert repr(empty_tracker) == "ProvenanceTracker(2 parameters tracked)"


@pytest.mark.unit
def test_tracker_clear(populated_tracker):
    """Test clear() removes all tracked parameters."""
    assert len(populated_tracker) == 4
    
    populated_tracker.clear()
    
    assert len(populated_tracker) == 0
    assert "DEFAULT.EXPID" not in populated_tracker


@pytest.mark.unit
def test_tracker_clear_and_repopulate(empty_tracker):
    """Test tracker can be repopulated after clear."""
    empty_tracker.track("PARAM1", "/path/file.yml")
    empty_tracker.track("PARAM2", "/path/file.yml")
    assert len(empty_tracker) == 2
    
    empty_tracker.clear()
    assert len(empty_tracker) == 0
    
    empty_tracker.track("PARAM3", "/path/file.yml")
    assert len(empty_tracker) == 1
    assert "PARAM3" in empty_tracker


# =============================================================================
# Export/Import Tests
# =============================================================================

@pytest.mark.unit
def test_export_empty_tracker(empty_tracker):
    """Test export_to_dict() on empty tracker."""
    result = empty_tracker.export_to_dict()
    assert result == {}


@pytest.mark.unit
def test_export_simple_nested_structure(empty_tracker, mock_timestamp):
    """Test export_to_dict() with nested structure."""
    with patch('time.time', return_value=mock_timestamp):
        empty_tracker.track("DEFAULT.EXPID", "/path/config.yml", line=5)
        empty_tracker.track("DEFAULT.HPCARCH", "/path/config.yml", line=8)
    
    result = empty_tracker.export_to_dict()
    
    assert "DEFAULT" in result
    assert "EXPID" in result["DEFAULT"]
    assert "HPCARCH" in result["DEFAULT"]
    assert result["DEFAULT"]["EXPID"]["file"] == "/path/config.yml"
    assert result["DEFAULT"]["EXPID"]["line"] == 5


@pytest.mark.unit
def test_export_deeply_nested_structure(empty_tracker, mock_timestamp):
    """Test export_to_dict() with deep nesting."""
    with patch('time.time', return_value=mock_timestamp):
        empty_tracker.track("LEVEL1.LEVEL2.LEVEL3.PARAM", "/path/deep.yml", line=100)
    
    result = empty_tracker.export_to_dict()
    
    assert "LEVEL1" in result
    assert "LEVEL2" in result["LEVEL1"]
    assert "LEVEL3" in result["LEVEL1"]["LEVEL2"]
    assert "PARAM" in result["LEVEL1"]["LEVEL2"]["LEVEL3"]


@pytest.mark.unit
def test_export_preserves_all_fields(empty_tracker, mock_timestamp):
    """Test export_to_dict() preserves all fields."""
    with patch('time.time', return_value=mock_timestamp):
        empty_tracker.track("DEFAULT.EXPID", "/path/config.yml", line=10, col=5)
    
    result = empty_tracker.export_to_dict()
    entry = result["DEFAULT"]["EXPID"]
    
    assert entry["file"] == "/path/config.yml"
    assert entry["line"] == 10
    assert entry["col"] == 5
    assert entry["timestamp"] == mock_timestamp


@pytest.mark.unit
def test_import_empty_dict(empty_tracker):
    """Test import_from_dict() with empty dict."""
    empty_tracker.import_from_dict({})
    assert len(empty_tracker) == 0


@pytest.mark.unit
def test_import_simple_structure(empty_tracker):
    """Test import_from_dict() with simple structure."""
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


@pytest.mark.unit
def test_import_multiple_parameters(empty_tracker):
    """Test import_from_dict() with multiple parameters."""
    prov_dict = {
        'DEFAULT': {
            'EXPID': {'file': '/path/config.yml', 'line': 5, 'timestamp': 1234.0},
            'HPCARCH': {'file': '/path/config.yml', 'line': 8, 'timestamp': 1234.0}
        },
        'JOBS': {
            'SIM': {
                'WALLCLOCK': {'file': '/path/jobs.yml', 'line': 23, 'timestamp': 1234.0}
            }
        }
    }
    
    empty_tracker.import_from_dict(prov_dict)
    
    assert len(empty_tracker) == 3
    assert "DEFAULT.EXPID" in empty_tracker
    assert "DEFAULT.HPCARCH" in empty_tracker
    assert "JOBS.SIM.WALLCLOCK" in empty_tracker


@pytest.mark.unit
def test_import_overwrites_existing(empty_tracker):
    """Test import_from_dict() overwrites existing."""
    empty_tracker.track("DEFAULT.EXPID", "/path/old.yml", line=5)
    
    prov_dict = {
        'DEFAULT': {
            'EXPID': {'file': '/path/new.yml', 'line': 10, 'timestamp': 9999.0}
        }
    }
    empty_tracker.import_from_dict(prov_dict)
    
    entry = empty_tracker.get("DEFAULT.EXPID")
    assert entry.file == '/path/new.yml'
    assert entry.line == 10


# =============================================================================
# Round-trip Tests
# =============================================================================

@pytest.mark.unit
def test_round_trip_simple(empty_tracker, mock_timestamp):
    """Test round-trip: track → export → import."""
    with patch('time.time', return_value=mock_timestamp):
        empty_tracker.track("DEFAULT.EXPID", "/path/config.yml", line=5, col=2)
        empty_tracker.track("DEFAULT.HPCARCH", "/path/config.yml", line=8)
    
    exported = empty_tracker.export_to_dict()
    
    new_tracker = ProvenanceTracker()
    new_tracker.import_from_dict(exported)
    
    assert len(new_tracker) == 2
    
    entry1 = new_tracker.get("DEFAULT.EXPID")
    assert entry1.file == "/path/config.yml"
    assert entry1.line == 5
    assert entry1.col == 2
    
    entry2 = new_tracker.get("DEFAULT.HPCARCH")
    assert entry2.file == "/path/config.yml"
    assert entry2.line == 8


@pytest.mark.unit
def test_round_trip_populated_tracker(populated_tracker):
    """Test round-trip with populated tracker."""
    exported = populated_tracker.export_to_dict()
    
    new_tracker = ProvenanceTracker()
    new_tracker.import_from_dict(exported)
    
    assert len(new_tracker) == len(populated_tracker)
    
    for param_path in ["DEFAULT.EXPID", "DEFAULT.HPCARCH", 
                      "JOBS.SIM.WALLCLOCK", "JOBS.SIM.PROCESSORS"]:
        original = populated_tracker.get(param_path)
        restored = new_tracker.get(param_path)
        
        assert restored.file == original.file
        assert restored.line == original.line
        assert restored.col == original.col


# =============================================================================
# Integration Tests
# =============================================================================

@pytest.mark.unit
def test_typical_config_loading_scenario(empty_tracker, mock_timestamp):
    """Test typical scenario: loading multiple config files."""
    with patch('time.time', return_value=mock_timestamp):
        # Load conf/autosubmit.yml
        empty_tracker.track("DEFAULT.EXPID", "/path/to/conf/autosubmit.yml", line=2)
        empty_tracker.track("DEFAULT.HPCARCH", "/path/to/conf/autosubmit.yml", line=3)
        
        # Load conf/jobs.yml
        empty_tracker.track("JOBS.SIM.WALLCLOCK", "/path/to/conf/jobs.yml", line=10)
        empty_tracker.track("JOBS.POST.WALLCLOCK", "/path/to/conf/jobs.yml", line=20)
        
        # Load platform config (overwrites HPCARCH)
        empty_tracker.track("DEFAULT.HPCARCH", "/path/to/platforms/marenostrum.yml", line=1)
    
    assert len(empty_tracker) == 4
    
    expid = empty_tracker.get("DEFAULT.EXPID")
    assert expid.file == "/path/to/conf/autosubmit.yml"
    
    hpcarch = empty_tracker.get("DEFAULT.HPCARCH")
    assert hpcarch.file == "/path/to/platforms/marenostrum.yml"
    
    exported = empty_tracker.export_to_dict()
    assert "DEFAULT" in exported
    assert "JOBS" in exported


@pytest.mark.unit
def test_debugging_scenario(populated_tracker):
    """Test scenario: debugging parameter source."""
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
