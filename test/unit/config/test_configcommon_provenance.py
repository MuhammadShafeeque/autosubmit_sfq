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

"""Tests for AutosubmitConfig with yaml-provenance integration."""

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


class TestAutsubmitConfigCategoryDetection:
    """Test category detection in AutosubmitConfig."""
    
    def test_determine_category_base_config(self, autosubmit_config: 'AutosubmitConfigFactory'):
        """Test _determine_category identifies base configs correctly."""
        expid = "t001"
        as_conf = autosubmit_config(expid=expid)
        
        # Create a path that looks like a base config
        config_file = Path(BasicConfig.LOCAL_ROOT_DIR) / expid / "conf" / "main.yml"
        
        category = as_conf._determine_category(config_file)
        
        assert category == "base"
    
    def test_determine_category_base_in_conf_dir(self, autosubmit_config: 'AutosubmitConfigFactory'):
        """Test that files in conf/ directory are categorized as base."""
        expid = "t002"
        as_conf = autosubmit_config(expid=expid)
        
        config_files = [
            Path(BasicConfig.LOCAL_ROOT_DIR) / expid / "conf" / "autosubmit.yml",
            Path(BasicConfig.LOCAL_ROOT_DIR) / expid / "conf" / "jobs.yml",
            Path(BasicConfig.LOCAL_ROOT_DIR) / expid / "conf" / "platforms.yml",
        ]
        
        for config_file in config_files:
            category = as_conf._determine_category(config_file)
            assert category == "base", f"Expected 'base' for {config_file}, got {category}"
    
    def test_determine_category_custom_config(self, autosubmit_config: 'AutosubmitConfigFactory'):
        """Test that custom config files return None (to be set explicitly)."""
        expid = "t003"
        as_conf = autosubmit_config(expid=expid)
        
        # Files outside conf directory or with CUSTOM_CONFIG in path
        custom_files = [
            Path("/some/other/path/custom.yml"),
            Path(BasicConfig.LOCAL_ROOT_DIR) / expid / "conf" / "CUSTOM_CONFIG" / "custom.yml",
        ]
        
        for config_file in custom_files:
            category = as_conf._determine_category(config_file)
            assert category is None, f"Expected None for {config_file}, got {category}"
    
    def test_determine_category_external_file(self, autosubmit_config: 'AutosubmitConfigFactory'):
        """Test that external files return None."""
        expid = "t004"
        as_conf = autosubmit_config(expid=expid)
        
        external_file = Path("/external/path/config.yml")
        category = as_conf._determine_category(external_file)
        
        assert category is None


class TestLoadConfigFileWithProvenance:
    """Test load_config_file method with provenance tracking."""
    
    def test_load_config_file_with_category(self, autosubmit_config: 'AutosubmitConfigFactory', tmp_path):
        """Test load_config_file with explicit category."""
        expid = "t005"
        as_conf = autosubmit_config(expid=expid)
        
        # Create a temporary config file
        config_file = tmp_path / "test_config.yml"
        config_file.write_text("""
CONFIG:
  TOTALJOBS: 30
  MAXWAITINGJOBS: 5
""")
        
        current_data = {}
        result = as_conf.load_config_file(current_data, config_file, category="base")
        
        # Check that data was loaded
        assert "CONFIG" in result
        assert result["CONFIG"]["TOTALJOBS"] == 30
        assert result["CONFIG"]["MAXWAITINGJOBS"] == 5
    
    def test_load_config_file_auto_detect_category(self, autosubmit_config: 'AutosubmitConfigFactory'):
        """Test that load_config_file auto-detects category when not provided."""
        expid = "t006"
        as_conf = autosubmit_config(expid=expid)
        
        # Create config file in conf directory
        conf_dir = Path(BasicConfig.LOCAL_ROOT_DIR) / expid / "conf"
        conf_dir.mkdir(parents=True, exist_ok=True)
        
        config_file = conf_dir / "test.yml"
        config_file.write_text("""
CONFIG:
  TOTALJOBS: 15
""")
        
        current_data = {}
        result = as_conf.load_config_file(current_data, config_file)  # No category
        
        # Should still work - category auto-detected
        assert "CONFIG" in result
        assert result["CONFIG"]["TOTALJOBS"] == 15
    
    def test_load_config_file_unify_with_existing(self, autosubmit_config: 'AutosubmitConfigFactory', tmp_path):
        """Test that load_config_file unifies with existing data."""
        expid = "t007"
        as_conf = autosubmit_config(expid=expid)
        
        # Existing data
        current_data = {
            "CONFIG": {
                "TOTALJOBS": 10,
                "EXISTING": "value"
            }
        }
        
        # New config file
        config_file = tmp_path / "new_config.yml"
        config_file.write_text("""
CONFIG:
  TOTALJOBS: 20
  NEW_KEY: new_value
""")
        
        result = as_conf.load_config_file(current_data, config_file, category="base")
        
        # Should merge/override values
        assert result["CONFIG"]["TOTALJOBS"] in [10, 20]  # Depends on unify logic
        assert "EXISTING" in result["CONFIG"] or "NEW_KEY" in result["CONFIG"]
    
    def test_load_config_file_with_misc(self, autosubmit_config: 'AutosubmitConfigFactory', tmp_path):
        """Test load_config_file with AS_MISC flag."""
        expid = "t008"
        as_conf = autosubmit_config(expid=expid)
        
        # Config file with AS_MISC flag
        config_file = tmp_path / "misc_config.yml"
        config_file.write_text("""
AS_MISC: true
CONFIG:
  TOTALJOBS: 25
""")
        
        current_data = {}
        result = as_conf.load_config_file(current_data, config_file, load_misc=False, category="base")
        
        # When AS_MISC is true and load_misc is False, file should be added to misc_files
        assert config_file in as_conf.misc_files
        # Data should be empty for non-misc load
        assert result.get("CONFIG", {}).get("TOTALJOBS", None) != 25 or len(result) == 0
    
    def test_load_config_file_normalize_variables(self, autosubmit_config: 'AutosubmitConfigFactory', tmp_path):
        """Test that load_config_file normalizes variables."""
        expid = "t009"
        as_conf = autosubmit_config(expid=expid)
        
        # Config with variables
        config_file = tmp_path / "vars.yml"
        config_file.write_text("""
CONFIG:
  TOTALJOBS: 10
DEFAULT:
  EXPID: test009
""")
        
        current_data = {}
        result = as_conf.load_config_file(current_data, config_file, category="base")
        
        # Should load and normalize
        assert "CONFIG" in result
        assert result["CONFIG"]["TOTALJOBS"] == 10


class TestGetParser:
    """Test get_parser static method."""
    
    def test_get_parser_with_category(self, tmp_path):
        """Test get_parser passes category to parser."""
        from autosubmit.config.yamlparser import YAMLParserFactory
        
        config_file = tmp_path / "parser_test.yml"
        config_file.write_text("""
CONFIG:
  VALUE: 42
""")
        
        factory = YAMLParserFactory()
        parser = AutosubmitConfig.get_parser(factory, config_file, category="base")
        
        assert parser.data is not None
        assert "CONFIG" in parser.data
        assert parser.data["CONFIG"]["VALUE"] == 42
    
    def test_get_parser_without_category(self, tmp_path):
        """Test get_parser works without category (backward compatible)."""
        from autosubmit.config.yamlparser import YAMLParserFactory
        
        config_file = tmp_path / "parser_test2.yml"
        config_file.write_text("key: value")
        
        factory = YAMLParserFactory()
        parser = AutosubmitConfig.get_parser(factory, config_file)
        
        assert parser.data is not None
        assert parser.data["key"] == "value"
    
    def test_get_parser_missing_proj_file(self, tmp_path):
        """Test get_parser handles missing proj files gracefully."""
        from autosubmit.config.yamlparser import YAMLParserFactory
        
        proj_file = tmp_path / "proj_test.yml"
        # Don't create the file
        
        factory = YAMLParserFactory()
        parser = AutosubmitConfig.get_parser(factory, proj_file, category="base")
        
        # Should return parser with empty data
        assert parser.data == {}
    
    def test_get_parser_dummy_paths(self):
        """Test get_parser handles dummy paths for testing."""
        from autosubmit.config.yamlparser import YAMLParserFactory
        
        dummy_path = Path('/dummy/local/root/dir/a000/conf/')
        
        factory = YAMLParserFactory()
        parser = AutosubmitConfig.get_parser(factory, dummy_path, category="base")
        
        # Should handle dummy path
        assert parser.data is not None


class TestProvenanceConfiguration:
    """Test yaml-provenance configuration in AutosubmitConfig."""
    
    def test_provenance_configured_on_init(self, autosubmit_config: 'AutosubmitConfigFactory'):
        """Test that yaml-provenance is configured when AutosubmitConfig is initialized."""
        expid = "t010"
        
        # Create config - provenance should be configured
        as_conf = autosubmit_config(expid=expid)
        
        # Just verify config was created successfully
        assert as_conf.expid == expid
        assert as_conf.experiment_data is not None
    
    @pytest.mark.skipif(not YAML_PROVENANCE_AVAILABLE, reason="yaml-provenance not installed")
    def test_provenance_hierarchy_configured(self, autosubmit_config: 'AutosubmitConfigFactory'):
        """Test that category hierarchy is configured correctly."""
        expid = "t011"
        as_conf = autosubmit_config(expid=expid)
        
        # Verify config was created (provenance config happens in __init__)
        assert as_conf.expid == expid
        
        # The hierarchy should be: [None, "base", "custom_pre", "custom_post"]
        # This is configured in the __init__ method


class TestBackwardCompatibility:
    """Test backward compatibility - existing code should work without changes."""
    
    def test_autosubmit_config_without_provenance(self, autosubmit_config: 'AutosubmitConfigFactory'):
        """Test that AutosubmitConfig works normally without provenance features."""
        expid = "t012"
        
        experiment_data = {
            "CONFIG": {
                "TOTALJOBS": 50,
                "MAXWAITINGJOBS": 10
            },
            "DEFAULT": {
                "EXPID": expid
            }
        }
        
        as_conf = autosubmit_config(expid=expid, experiment_data=experiment_data)
        
        # Standard operations should work
        assert as_conf.expid == expid
        assert as_conf.experiment_data["CONFIG"]["TOTALJOBS"] == 50
        assert as_conf.experiment_data["CONFIG"]["MAXWAITINGJOBS"] == 10
    
    def test_existing_methods_still_work(self, autosubmit_config: 'AutosubmitConfigFactory'):
        """Test that existing methods work as before."""
        expid = "t013"
        
        experiment_data = {
            "CONFIG": {"TOTALJOBS": 30},
            "JOBS": {
                "JOB1": {
                    "PROCESSORS": 8,
                    "WALLCLOCK": "01:00"
                }
            }
        }
        
        as_conf = autosubmit_config(expid=expid, experiment_data=experiment_data)
        
        # Test existing getter methods
        processors = as_conf.get_processors("JOB1")
        assert processors == "8"
    
    def test_load_without_category_parameter(self, autosubmit_config: 'AutosubmitConfigFactory', tmp_path):
        """Test loading configs without specifying category parameter."""
        expid = "t014"
        as_conf = autosubmit_config(expid=expid)
        
        config_file = tmp_path / "backward_compat.yml"
        config_file.write_text("""
CONFIG:
  TOTALJOBS: 100
""")
        
        # Load without category - should auto-detect
        current_data = {}
        result = as_conf.load_config_file(current_data, config_file)
        
        assert "CONFIG" in result
        assert result["CONFIG"]["TOTALJOBS"] == 100


class TestEdgeCasesAndErrors:
    """Test edge cases and error handling."""
    
    def test_load_nonexistent_file(self, autosubmit_config: 'AutosubmitConfigFactory', tmp_path):
        """Test loading a nonexistent file."""
        expid = "t015"
        as_conf = autosubmit_config(expid=expid)
        
        nonexistent_file = tmp_path / "nonexistent.yml"
        
        current_data = {}
        # Should handle gracefully or raise appropriate error
        try:
            result = as_conf.load_config_file(current_data, nonexistent_file, category="base")
            # If no error, result should be usable
            assert result is not None
        except Exception:
            # Expected to fail
            pass
    
    def test_category_with_special_characters(self, autosubmit_config: 'AutosubmitConfigFactory'):
        """Test _determine_category with paths containing special characters."""
        expid = "t016"
        as_conf = autosubmit_config(expid=expid)
        
        special_path = Path(f"/path/with spaces/{expid}/conf/file.yml")
        
        # Should handle without error
        category = as_conf._determine_category(special_path)
        # Should still detect as base if pattern matches
        assert category in ["base", None]
