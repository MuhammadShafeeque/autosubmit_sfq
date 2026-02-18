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

"""Tests for YAMLParser with yaml-provenance integration."""

from pathlib import Path
import pytest

from autosubmit.config.yamlparser import YAMLParser, YAMLParserFactory


# Check if yaml-provenance is available
YAML_PROVENANCE_AVAILABLE = True
try:
    import yaml_provenance
except ImportError:
    YAML_PROVENANCE_AVAILABLE = False


class TestYAMLParserProvenance:
    """Test suite for YAMLParser with provenance tracking."""

    def test_yaml_parser_loads_with_provenance(self, tmp_path):
        """Test that YAMLParser.load returns DictWithProvenance when category provided."""
        config_file = tmp_path / "test.yml"
        config_file.write_text("""
CONFIG:
  TOTALJOBS: 20
  MAXWAITINGJOBS: 10
""")
        
        parser = YAMLParser()
        result = parser.load(str(config_file), category="base")
        
        # Check that data was loaded correctly
        assert result is not None
        assert "CONFIG" in result
        assert result["CONFIG"]["TOTALJOBS"] == 20
        assert result["CONFIG"]["MAXWAITINGJOBS"] == 10
        
        # Check provenance tracking if available
        if YAML_PROVENANCE_AVAILABLE:
            # The result should be a DictWithProvenance
            assert hasattr(result, "provenance") or "DictWithProvenance" in str(type(result))
    
    def test_yaml_parser_backward_compatible(self, tmp_path):
        """Test that YAMLParser works without category (backward compatible)."""
        config_file = tmp_path / "test.yml"
        config_file.write_text("""
key: value
nested:
  item: 123
""")
        
        parser = YAMLParser()
        result = parser.load(str(config_file))  # No category
        
        # Should still work without category
        assert result["key"] == "value"
        assert result["nested"]["item"] == 123
    
    def test_yaml_parser_with_file_object(self, tmp_path):
        """Test that YAMLParser works with file objects."""
        config_file = tmp_path / "test.yml"
        config_file.write_text("""
DATA:
  VALUE: 42
""")
        
        parser = YAMLParser()
        with open(config_file) as f:
            result = parser.load(f, category="base")
        
        assert result["DATA"]["VALUE"] == 42
    
    def test_yaml_parser_stores_category(self, tmp_path):
        """Test that YAMLParser stores the category for later use."""
        config_file = tmp_path / "test.yml"
        config_file.write_text("test: data")
        
        parser = YAMLParser()
        assert parser.category is None
        
        parser.load(str(config_file), category="custom_pre")
        assert parser.category == "custom_pre"
    
    def test_yaml_parser_empty_file(self, tmp_path):
        """Test that YAMLParser handles empty files gracefully."""
        config_file = tmp_path / "empty.yml"
        config_file.write_text("")
        
        parser = YAMLParser()
        result = parser.load(str(config_file), category="base")
        
        # Empty YAML should return None or empty dict
        assert result is None or result == {}
    
    def test_yaml_parser_nested_structures(self, tmp_path):
        """Test that YAMLParser handles nested YAML structures."""
        config_file = tmp_path / "nested.yml"
        config_file.write_text("""
CONFIG:
  LEVEL1:
    LEVEL2:
      LEVEL3:
        VALUE: deep_value
    ANOTHER: test
  SIMPLE: value
""")
        
        parser = YAMLParser()
        result = parser.load(str(config_file), category="base")
        
        assert result["CONFIG"]["LEVEL1"]["LEVEL2"]["LEVEL3"]["VALUE"] == "deep_value"
        assert result["CONFIG"]["LEVEL1"]["ANOTHER"] == "test"
        assert result["CONFIG"]["SIMPLE"] == "value"
    
    def test_yaml_parser_with_lists(self, tmp_path):
        """Test that YAMLParser handles lists in YAML."""
        config_file = tmp_path / "lists.yml"
        config_file.write_text("""
ITEMS:
  - first
  - second
  - third
NESTED:
  LIST:
    - value1
    - value2
""")
        
        parser = YAMLParser()
        result = parser.load(str(config_file), category="base")
        
        assert len(result["ITEMS"]) == 3
        assert result["ITEMS"][0] == "first"
        assert result["ITEMS"][2] == "third"
        assert result["NESTED"]["LIST"][1] == "value2"
    
    def test_yaml_parser_factory_creates_parser(self):
        """Test that YAMLParserFactory creates YAMLParser instances."""
        factory = YAMLParserFactory()
        parser = factory.create_parser()
        
        assert isinstance(parser, YAMLParser)
        assert parser.category is None
        assert parser.data == []
    
    def test_yaml_parser_multiple_categories(self, tmp_path):
        """Test that YAMLParser can load files with different categories."""
        config1 = tmp_path / "config1.yml"
        config1.write_text("data1: value1")
        
        config2 = tmp_path / "config2.yml"
        config2.write_text("data2: value2")
        
        parser = YAMLParser()
        
        result1 = parser.load(str(config1), category="base")
        assert result1["data1"] == "value1"
        assert parser.category == "base"
        
        result2 = parser.load(str(config2), category="custom_pre")
        assert result2["data2"] == "value2"
        assert parser.category == "custom_pre"
    
    def test_yaml_parser_with_special_characters(self, tmp_path):
        """Test that YAMLParser handles special characters in values."""
        config_file = tmp_path / "special.yml"
        config_file.write_text("""
PATHS:
  PROJECT: "/path/to/project"
  SCRATCH: "/scratch/user001"
VALUES:
  PERCENT: "100%"
  SPECIAL: "value-with-dash"
""")
        
        parser = YAMLParser()
        result = parser.load(str(config_file), category="base")
        
        assert result["PATHS"]["PROJECT"] == "/path/to/project"
        assert result["VALUES"]["PERCENT"] == "100%"
        assert result["VALUES"]["SPECIAL"] == "value-with-dash"


class TestYAMLParserEdgeCases:
    """Test edge cases and error handling in YAMLParser."""
    
    def test_yaml_parser_nonexistent_file(self):
        """Test that YAMLParser handles nonexistent files."""
        parser = YAMLParser()
        
        # This should raise an exception or return empty
        with pytest.raises(Exception):
            parser.load("/nonexistent/path/to/file.yml", category="base")
    
    def test_yaml_parser_invalid_yaml(self, tmp_path):
        """Test that YAMLParser handles invalid YAML syntax."""
        config_file = tmp_path / "invalid.yml"
        config_file.write_text("""
key: value
  invalid indentation
    more invalid
""")
        
        parser = YAMLParser()
        
        # Invalid YAML should raise an exception
        with pytest.raises(Exception):
            parser.load(str(config_file), category="base")
    
    def test_yaml_parser_none_category(self, tmp_path):
        """Test that YAMLParser works correctly with None category."""
        config_file = tmp_path / "test.yml"
        config_file.write_text("key: value")
        
        parser = YAMLParser()
        result = parser.load(str(config_file), category=None)
        
        assert result["key"] == "value"
        # Category should remain None if explicitly set to None
        assert parser.category is None


@pytest.mark.skipif(not YAML_PROVENANCE_AVAILABLE, reason="yaml-provenance not installed")
class TestYAMLParserProvenanceFeatures:
    """Tests specific to yaml-provenance features (only run if yaml-provenance is installed)."""
    
    def test_yaml_parser_provenance_type(self, tmp_path):
        """Test that loaded data has correct provenance type."""
        config_file = tmp_path / "test.yml"
        config_file.write_text("CONFIG:\n  TOTALJOBS: 20")
        
        parser = YAMLParser()
        result = parser.load(str(config_file), category="base")
        
        # Should return DictWithProvenance
        assert "DictWithProvenance" in str(type(result))
    
    def test_yaml_parser_category_resolver(self, tmp_path):
        """Test that category resolver is used correctly."""
        config_file = tmp_path / "test.yml"
        config_file.write_text("data: value")
        
        parser = YAMLParser()
        result = parser.load(str(config_file), category="custom_post")
        
        # Data should be loaded with provenance
        assert hasattr(result, "provenance") or "DictWithProvenance" in str(type(result))
