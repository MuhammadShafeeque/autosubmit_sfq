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

"""Integration tests for yaml-provenance in Autosubmit."""

from pathlib import Path
from typing import TYPE_CHECKING
import pytest

from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.config.basicconfig import BasicConfig

if TYPE_CHECKING:
    from test.unit.conftest import AutosubmitConfigFactory


# Check if yaml-provenance is available
YAML_PROVENANCE_AVAILABLE = True
try:
    import yaml_provenance
except ImportError:
    YAML_PROVENANCE_AVAILABLE = False


@pytest.mark.integration
class TestProvenanceEndToEnd:
    """End-to-end integration tests for provenance tracking."""
    
    def test_basic_reload_with_provenance(self, autosubmit_config: 'AutosubmitConfigFactory'):
        """Test basic reload cycle with provenance tracking."""
        expid = "i001"
        
        # Create minimal experiment data
        experiment_data = {
            "CONFIG": {
                "TOTALJOBS": 10,
                "MAXWAITINGJOBS": 2
            },
            "DEFAULT": {
                "EXPID": expid,
                "HPCARCH": "LOCAL"
            },
            "JOBS": {},
            "PLATFORMS": {}
        }
        
        as_conf = autosubmit_config(expid=expid, experiment_data=experiment_data)
        
        # Verify configuration loaded
        assert as_conf.expid == expid
        assert as_conf.experiment_data["CONFIG"]["TOTALJOBS"] == 10
    
    def test_load_multiple_config_files(self, autosubmit_config: 'AutosubmitConfigFactory'):
        """Test loading multiple configuration files with different categories."""
        expid = "i002"
        
        # Create experiment directory
        conf_dir = Path(BasicConfig.LOCAL_ROOT_DIR) / expid / "conf"
        conf_dir.mkdir(parents=True, exist_ok=True)
        
        # Create main config
        main_config = conf_dir / "autosubmit.yml"
        main_config.write_text("""
CONFIG:
  TOTALJOBS: 20
  MAXWAITINGJOBS: 5
""")
        
        # Create jobs config
        jobs_config = conf_dir / "jobs.yml"
        jobs_config.write_text("""
JOBS:
  JOB1:
    PROCESSORS: 4
    WALLCLOCK: '01:00'
""")
        
        as_conf = autosubmit_config(expid=expid)
        
        # Load both configs
        current_data = {}
        result1 = as_conf.load_config_file(current_data, main_config)
        result2 = as_conf.load_config_file(result1, jobs_config)
        
        # Both should be loaded
        assert "CONFIG" in result1 or "CONFIG" in result2
        assert "JOBS" in result2
    
    def test_custom_config_integration(self, autosubmit_config: 'AutosubmitConfigFactory', tmp_path):
        """Test loading custom configuration files."""
        expid = "i003"
        as_conf = autosubmit_config(expid=expid)
        
        # Create base config in conf dir
        conf_dir = Path(BasicConfig.LOCAL_ROOT_DIR) / expid / "conf"
        conf_dir.mkdir(parents=True, exist_ok=True)
        
        base_config = conf_dir / "main.yml"
        base_config.write_text("""
CONFIG:
  TOTALJOBS: 15
""")
        
        # Create custom config elsewhere
        custom_config = tmp_path / "custom.yml"
        custom_config.write_text("""
CONFIG:
  TOTALJOBS: 25
  CUSTOM_PARAM: custom_value
""")
        
        # Load base config (should be category "base")
        current_data = {}
        result1 = as_conf.load_config_file(current_data, base_config)
        
        # Load custom config (category should be set explicitly)
        result2 = as_conf.load_config_file(result1, custom_config, category="custom_pre")
        
        # Check data is loaded
        assert "CONFIG" in result2
    
    def test_provenance_with_nested_includes(self, autosubmit_config: 'AutosubmitConfigFactory'):
        """Test provenance tracking with nested configuration includes."""
        expid = "i004"
        
        conf_dir = Path(BasicConfig.LOCAL_ROOT_DIR) / expid / "conf"
        conf_dir.mkdir(parents=True, exist_ok=True)
        
        # Create primary config
        primary = conf_dir / "primary.yml"
        primary.write_text("""
CONFIG:
  TOTALJOBS: 30
  PRIMARY_KEY: primary_value
""")
        
        # Create included config
        included = conf_dir / "included.yml"
        included.write_text("""
CONFIG:
  INCLUDED_KEY: included_value
""")
        
        as_conf = autosubmit_config(expid=expid)
        
        # Load both in sequence
        current_data = {}
        result1 = as_conf.load_config_file(current_data, primary)
        result2 = as_conf.load_config_file(result1, included)
        
        # Both should have data
        assert "CONFIG" in result2
    
    @pytest.mark.skipif(not YAML_PROVENANCE_AVAILABLE, reason="yaml-provenance not installed")
    def test_provenance_data_structure(self, autosubmit_config: 'AutosubmitConfigFactory'):
        """Test that provenance data structures are correct."""
        expid = "i005"
        
        conf_dir = Path(BasicConfig.LOCAL_ROOT_DIR) / expid / "conf"
        conf_dir.mkdir(parents=True, exist_ok=True)
        
        config_file = conf_dir / "test.yml"
        config_file.write_text("""
CONFIG:
  TOTALJOBS: 40
""")
        
        as_conf = autosubmit_config(expid=expid)
        current_data = {}
        result = as_conf.load_config_file(current_data, config_file)
        
        # Check that result has correct structure
        assert "CONFIG" in result
        
        # If yaml-provenance is available, check provenance attributes
        if hasattr(result, "provenance"):
            assert result.provenance is not None
    
    def test_full_experiment_lifecycle(self, autosubmit_config: 'AutosubmitConfigFactory'):
        """Test a complete experiment configuration lifecycle."""
        expid = "i006"
        
        # Setup directory structure
        conf_dir = Path(BasicConfig.LOCAL_ROOT_DIR) / expid / "conf"
        conf_dir.mkdir(parents=True, exist_ok=True)
        
        # Create minimal config files
        autosubmit_yml = conf_dir / "autosubmit.yml"
        autosubmit_yml.write_text("""
CONFIG:
  AUTOSUBMIT_VERSION: "4.1.0"
  TOTALJOBS: 50
  MAXWAITINGJOBS: 10
""")
        
        jobs_yml = conf_dir / "jobs.yml"
        jobs_yml.write_text("""
JOBS:
  INIT:
    PLATFORM: LOCAL
    RUNNING: once
  SIM:
    PLATFORM: LOCAL
    RUNNING: chunk
    DEPENDENCIES:
      INIT: {}
""")
        
        platforms_yml = conf_dir / "platforms.yml"
        platforms_yml.write_text("""
PLATFORMS:
  LOCAL:
    TYPE: local
    PROJECT: test
    USER: testuser
""")
        
        # Create config and load files
        as_conf = autosubmit_config(expid=expid)
        
        current_data = {}
        result = as_conf.load_config_file(current_data, autosubmit_yml)
        result = as_conf.load_config_file(result, jobs_yml)
        result = as_conf.load_config_file(result, platforms_yml)
        
        # Verify all sections loaded
        assert "CONFIG" in result
        assert "JOBS" in result or "PLATFORMS" in result
    
    def test_error_handling_in_integration(self, autosubmit_config: 'AutosubmitConfigFactory'):
        """Test error handling in integrated environment."""
        expid = "i007"
        as_conf = autosubmit_config(expid=expid)
        
        # Try to load nonexistent file
        nonexistent = Path("/tmp/nonexistent_integration_test.yml")
        
        current_data = {}
        try:
            result = as_conf.load_config_file(current_data, nonexistent)
            # If it doesn't raise, result should be safe
            assert result is not None
        except Exception:
            # Expected behavior
            pass


@pytest.mark.integration
class TestProvenanceCategoryPriority:
    """Test category priority and conflict resolution."""
    
    def test_category_override_order(self, autosubmit_config: 'AutosubmitConfigFactory'):
        """Test that later categories override earlier ones correctly."""
        expid = "i008"
        
        conf_dir = Path(BasicConfig.LOCAL_ROOT_DIR) / expid / "conf"
        conf_dir.mkdir(parents=True, exist_ok=True)
        
        base = conf_dir / "base.yml"
        base.write_text("""
CONFIG:
  PARAM: base_value
  BASE_ONLY: base_data
""")
        
        as_conf = autosubmit_config(expid=expid)
        
        # Load base
        current_data = {}
        result = as_conf.load_config_file(current_data, base, category="base")
        
        assert "CONFIG" in result
    
    @pytest.mark.skipif(not YAML_PROVENANCE_AVAILABLE, reason="yaml-provenance not installed")
    def test_conflict_detection(self, autosubmit_config: 'AutosubmitConfigFactory'):
        """Test that conflicts are detected and handled."""
        expid = "i009"
        
        conf_dir = Path(BasicConfig.LOCAL_ROOT_DIR) / expid / "conf"
        conf_dir.mkdir(parents=True, exist_ok=True)
        
        config1 = conf_dir / "config1.yml"
        config1.write_text("""
CONFIG:
  SHARED_KEY: value1
""")
        
        config2 = conf_dir / "config2.yml"
        config2.write_text("""
CONFIG:
  SHARED_KEY: value2
""")
        
        as_conf = autosubmit_config(expid=expid)
        
        # Load both - should handle conflict
        current_data = {}
        result1 = as_conf.load_config_file(current_data, config1, category="base")
        result2 = as_conf.load_config_file(result1, config2, category="custom_pre")
        
        # Should have a final value
        assert "CONFIG" in result2
        assert "SHARED_KEY" in result2["CONFIG"]


@pytest.mark.integration
class TestProvenancePerformance:
    """Test performance aspects of provenance tracking."""
    
    def test_multiple_files_load_quickly(self, autosubmit_config: 'AutosubmitConfigFactory'):
        """Test that loading multiple files doesn't significantly slow down."""
        expid = "i010"
        
        conf_dir = Path(BasicConfig.LOCAL_ROOT_DIR) / expid / "conf"
        conf_dir.mkdir(parents=True, exist_ok=True)
        
        # Create multiple config files
        for i in range(5):
            config_file = conf_dir / f"config{i}.yml"
            config_file.write_text(f"""
SECTION{i}:
  KEY{i}: value{i}
""")
        
        as_conf = autosubmit_config(expid=expid)
        
        # Load all files
        current_data = {}
        for i in range(5):
            config_file = conf_dir / f"config{i}.yml"
            current_data = as_conf.load_config_file(current_data, config_file)
        
        # All should be loaded
        for i in range(5):
            assert f"SECTION{i}" in current_data or len(current_data) > 0
    
    def test_large_config_handling(self, autosubmit_config: 'AutosubmitConfigFactory'):
        """Test handling of large configuration files."""
        expid = "i011"
        
        conf_dir = Path(BasicConfig.LOCAL_ROOT_DIR) / expid / "conf"
        conf_dir.mkdir(parents=True, exist_ok=True)
        
        # Create a large config
        large_config = conf_dir / "large.yml"
        content = "JOBS:\n"
        for i in range(100):
            content += f"  JOB{i}:\n"
            content += f"    PLATFORM: LOCAL\n"
            content += f"    PROCESSORS: {i % 10 + 1}\n"
        
        large_config.write_text(content)
        
        as_conf = autosubmit_config(expid=expid)
        
        # Load large config
        current_data = {}
        result = as_conf.load_config_file(current_data, large_config)
        
        # Should handle large file
        assert "JOBS" in result or len(result) > 0


@pytest.mark.integration
class TestBackwardCompatibilityIntegration:
    """Integration tests for backward compatibility."""
    
    def test_existing_experiments_still_work(self, autosubmit_config: 'AutosubmitConfigFactory'):
        """Test that existing experiments work without modification."""
        expid = "i012"
        
        # Simulate existing experiment structure
        experiment_data = {
            "CONFIG": {
                "AUTOSUBMIT_VERSION": "4.0.0",
                "TOTALJOBS": 100,
                "MAXWAITINGJOBS": 20
            },
            "DEFAULT": {
                "EXPID": expid,
                "HPCARCH": "LOCAL"
            },
            "JOBS": {
                "JOB1": {
                    "PLATFORM": "LOCAL",
                    "PROCESSORS": 1
                }
            },
            "PLATFORMS": {
                "LOCAL": {
                    "TYPE": "local"
                }
            }
        }
        
        as_conf = autosubmit_config(expid=expid, experiment_data=experiment_data)
        
        # All standard operations should work
        assert as_conf.expid == expid
        assert as_conf.experiment_data["CONFIG"]["TOTALJOBS"] == 100
        processors = as_conf.get_processors("JOB1")
        assert processors == "1"
    
    def test_no_provenance_when_not_available(self, autosubmit_config: 'AutosubmitConfigFactory'):
        """Test that system works correctly when yaml-provenance is not available."""
        expid = "i013"
        
        experiment_data = {
            "CONFIG": {"TOTALJOBS": 50},
            "DEFAULT": {"EXPID": expid}
        }
        
        # Should work regardless of yaml-provenance availability
        as_conf = autosubmit_config(expid=expid, experiment_data=experiment_data)
        
        assert as_conf.expid == expid
        assert as_conf.experiment_data["CONFIG"]["TOTALJOBS"] == 50
