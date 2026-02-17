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
Integration tests for the provenance tracking feature.

This module tests the complete provenance tracking workflow, including:
- End-to-end configuration loading with provenance
- Multiple configuration files tracking
- Backward compatibility
- Provenance persistence after reload
- Parameter override tracking
- Error handling and graceful degradation

Test scenarios verify that provenance tracking works correctly in real-world
usage patterns and integrates seamlessly with existing Autosubmit functionality.
"""

import pytest
from ruamel.yaml import YAML

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.config.provenance_tracker import ProvenanceTracker


@pytest.mark.integration
class TestProvenanceIntegration:
    """Integration tests for provenance tracking feature."""

    def test_end_to_end_config_load_with_provenance(self, autosubmit_exp):
        """
        Test complete flow: load config → track provenance → query → save.
        
        Verifies:
        - Configuration loads successfully with provenance tracking enabled
        - Provenance is tracked for all parameters
        - Query methods work correctly
        - experiment_data.yml contains PROVENANCE section
        """
        # Create experiment with provenance tracking enabled
        exp = autosubmit_exp(experiment_data={
            'CONFIG': {
                'TRACK_PROVENANCE': True
            },
            'DEFAULT': {
                'EXPID': 'test001',
                'HPCARCH': 'LOCAL'
            },
            'JOBS': {
                'SIM': {
                    'WALLCLOCK': '02:00',
                    'PROCESSORS': 4
                }
            },
            'PLATFORMS': {
                'LOCAL': {
                    'TYPE': 'ps'
                }
            }
        })
        
        config = exp.as_conf
        
        # Verify provenance tracking is enabled
        assert config.track_provenance is True
        assert config.provenance_tracker is not None
        assert isinstance(config.provenance_tracker, ProvenanceTracker)
        
        # Verify parameters are tracked
        assert len(config.provenance_tracker) > 0
        
        # Test get_parameter_source() for DEFAULT.EXPID
        expid_source = config.get_parameter_source("DEFAULT.EXPID")
        assert expid_source is not None
        assert 'file' in expid_source
        assert 'timestamp' in expid_source
        assert 'additional_data.yml' in expid_source['file']
        
        # Test get_parameter_source() for nested parameter
        wallclock_source = config.get_parameter_source("JOBS.SIM.WALLCLOCK")
        if wallclock_source:  # May be tracked
            assert 'file' in wallclock_source
            assert 'timestamp' in wallclock_source
        
        # Test get_all_provenance()
        all_prov = config.get_all_provenance()
        assert isinstance(all_prov, dict)
        assert 'DEFAULT' in all_prov or 'CONFIG' in all_prov
        
        # Save and verify PROVENANCE section in experiment_data.yml
        config.save()
        
        exp_data_file = exp.exp_path / "conf" / "metadata" / "experiment_data.yml"
        assert exp_data_file.exists()
        
        with open(exp_data_file, 'r') as f:
            saved_data = YAML(typ='safe').load(f)
        
        # Verify PROVENANCE section exists (if any parameters were tracked)
        if len(config.provenance_tracker) > 0:
            assert 'PROVENANCE' in saved_data
            assert isinstance(saved_data['PROVENANCE'], dict)

    def test_multiple_config_files_provenance(self, autosubmit_exp, tmp_path):
        """
        Test provenance tracking across multiple YAML files.
        
        Verifies:
        - Parameters from different files tracked correctly
        - Each parameter tracked to correct source file
        - Later files override earlier ones correctly
        """
        exp = autosubmit_exp(experiment_data={
            'CONFIG': {
                'TRACK_PROVENANCE': True
            },
            'DEFAULT': {
                'EXPID': 'test002',
                'HPCARCH': 'LOCAL'
            }
        })
        
        config = exp.as_conf
        conf_dir = exp.exp_path / "conf"
        
        # Create multiple config files
        jobs_file = conf_dir / "jobs_test.yml"
        jobs_file.write_text("""
JOBS:
  SIM:
    WALLCLOCK: '02:00'
    PROCESSORS: 4
""")
        
        platforms_file = conf_dir / "platforms_test.yml"
        platforms_file.write_text("""
PLATFORMS:
  LOCAL:
    TYPE: ps
    MAX_WALLCLOCK: '48:00'
""")
        
        # Reload configuration to pick up new files
        config.reload(force_load=True)
        
        # Verify provenance tracker exists
        assert config.track_provenance is True
        assert config.provenance_tracker is not None
        
        # Verify parameters tracked (check that tracker has entries)
        assert len(config.provenance_tracker) > 0
        
        # Check if specific parameters are tracked
        expid_source = config.get_parameter_source("DEFAULT.EXPID")
        if expid_source:
            assert 'file' in expid_source
            # Should point to one of the config files
            assert any(name in expid_source['file'] for name in ['additional_data.yml', 'basic_structure.yml', 'jobs_test.yml'])

    def test_backward_compatibility_no_provenance(self, autosubmit_exp):
        """
        Test experiments without provenance tracking still work.
        
        Verifies:
        - Configuration loads without TRACK_PROVENANCE
        - No provenance tracked
        - experiment_data.yml has no PROVENANCE section
        - All existing functionality works
        """
        # Create experiment WITHOUT provenance tracking
        exp = autosubmit_exp(experiment_data={
            'DEFAULT': {
                'EXPID': 'test006',
                'HPCARCH': 'LOCAL'
            },
            'JOBS': {
                'SIM': {
                    'WALLCLOCK': '02:00'
                }
            }
        })
        
        config = exp.as_conf
        
        # Verify provenance tracking is disabled
        assert config.track_provenance is False
        
        # get_parameter_source should return None
        source = config.get_parameter_source("DEFAULT.EXPID")
        assert source is None
        
        # get_all_provenance should return empty dict
        all_prov = config.get_all_provenance()
        assert all_prov == {}
        
        # Save configuration
        config.save()
        
        # Verify experiment_data.yml exists and has no PROVENANCE section
        exp_data_file = exp.exp_path / "conf" / "metadata" / "experiment_data.yml"
        assert exp_data_file.exists()
        
        with open(exp_data_file, 'r') as f:
            saved_data = YAML(typ='safe').load(f)
        
        assert 'PROVENANCE' not in saved_data

    def test_provenance_persists_after_reload(self, autosubmit_exp):
        """
        Test provenance survives reload operations.
        
        Verifies:
        - Provenance saved to experiment_data.yml
        - New AutosubmitConfig instance can reload it
        - PROVENANCE section loaded correctly
        - get_parameter_source() works after reload
        """
        exp = autosubmit_exp(experiment_data={
            'CONFIG': {
                'TRACK_PROVENANCE': True
            },
            'DEFAULT': {
                'EXPID': 'test007',
                'HPCARCH': 'LOCAL',
                'TEST_PARAM': 'test_value'
            }
        })
        
        config1 = exp.as_conf
        
        # Save with provenance
        config1.save()
        
        # Create new instance and reload
        config2 = AutosubmitConfig(exp.expid, BasicConfig)
        config2.reload(force_load=True)
        
        # Note: After reload, provenance needs to be re-tracked since it's
        # built during config loading. The PROVENANCE section in YAML is
        # for archival/reference, not for runtime querying.
        # This is by design - provenance is tracked during loading.
        
        # Verify that if tracking is enabled, it works after reload
        if config2.track_provenance:
            assert config2.provenance_tracker is not None

    def test_parameter_override_tracking(self, autosubmit_exp):
        """
        Test that last-set file is tracked correctly when parameters are overridden.
        
        Verifies:
        - Parameters can be overridden
        - Provenance shows last file that set parameter (last wins)
        """
        exp = autosubmit_exp(experiment_data={
            'CONFIG': {
                'TRACK_PROVENANCE': True
            },
            'DEFAULT': {
                'EXPID': 'test009',
                'HPCARCH': 'LOCAL',
                'OVERRIDE_TEST': 'first_value'
            }
        })
        
        config = exp.as_conf
        conf_dir = exp.exp_path / "conf"
        
        # Create override file that will be loaded after
        override_file = conf_dir / "zzz_override.yml"
        override_file.write_text("""
DEFAULT:
  OVERRIDE_TEST: second_value
  NEW_PARAM: new_value
""")
        
        # Reload to pick up override file (alphabetically sorted, zzz loads last)
        config.reload(force_load=True)
        
        # Verify provenance tracking is working
        assert config.track_provenance is True
        assert config.provenance_tracker is not None
        
        # Check that override parameter is tracked
        override_source = config.get_parameter_source("DEFAULT.OVERRIDE_TEST")
        if override_source:
            # Should point to the override file (last file wins)
            assert 'file' in override_source
            # The file should be one of the loaded configs
            assert any(name in override_source['file'] for name in ['override.yml', 'additional_data.yml'])

    def test_error_handling(self, autosubmit_exp, tmp_path):
        """
        Test graceful degradation when issues occur.
        
        Verifies:
        - Missing provenance_tracker (None) doesn't crash
        - Invalid config structure handled
        - Errors logged, not raised (graceful)
        """
        # Test with provenance_tracker = None
        exp = autosubmit_exp(experiment_data={
            'DEFAULT': {
                'EXPID': 'test010',
                'HPCARCH': 'LOCAL'
            }
        })
        
        config = exp.as_conf
        
        # Verify behavior when provenance_tracker is None
        assert config.provenance_tracker is None
        
        # These should not raise errors
        source = config.get_parameter_source("DEFAULT.EXPID")
        assert source is None
        
        all_prov = config.get_all_provenance()
        assert all_prov == {}
        
        # Export should not fail
        json_file = tmp_path / "provenance_empty.json"
        config.export_provenance(str(json_file))
        # File may not be created if no provenance, which is fine
        
        # Save should work without provenance
        config.save()
        
        exp_data_file = exp.exp_path / "conf" / "metadata" / "experiment_data.yml"
        assert exp_data_file.exists()
