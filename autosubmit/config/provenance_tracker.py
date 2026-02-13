"""
Provenance tracker for configuration parameters.

This module provides functionality to track the source (file, line, column)
of configuration parameters loaded from YAML files. It enables users to
understand where each configuration value comes from, which is essential
for debugging, troubleshooting, and maintaining reproducibility.

The module consists of two main classes:

- ProvEntry: Represents provenance information for a single parameter
  (source file, line number, column number, and timestamp).

- ProvenanceTracker: Manages provenance for all parameters using a flat
  dictionary with dot-separated keys for efficient O(1) access.

Example Usage:
    >>> from autosubmit.config.provenance_tracker import ProvenanceTracker
    >>> 
    >>> # Create tracker and record parameter sources
    >>> tracker = ProvenanceTracker()
    >>> tracker.track("DEFAULT.EXPID", "/path/to/config.yml", line=5)
    >>> tracker.track("JOBS.SIM.WALLCLOCK", "/path/to/jobs.yml", line=23, col=7)
    >>> 
    >>> # Query parameter source
    >>> prov = tracker.get("DEFAULT.EXPID")
    >>> print(f"EXPID from {prov.file} at line {prov.line}")
    >>> 
    >>> # Export to nested dict for YAML serialization
    >>> nested = tracker.export_to_dict()
    >>> # {'DEFAULT': {'EXPID': {'file': '...', 'line': 5, ...}}, ...}
    >>> 
    >>> # Import from nested dict (e.g., from experiment_data.yml)
    >>> tracker.import_from_dict(nested)

Design Principles:
    - Separation of concerns: Standalone module with no dependencies on
      AutosubmitConfig or other autosubmit modules
    - Efficient storage: Flat dict with dot-separated keys for O(1) access
    - Flexible export: Convert to nested dict format for YAML serialization
    - Type safety: Comprehensive type hints for all methods
    - Simplicity: Clear, maintainable code with comprehensive docstrings

Author: Autosubmit Development Team
Date: February 13, 2026
Version: 1.0
"""

from typing import Dict, Optional, Any, Union
from pathlib import Path
import time


class ProvEntry:
    """
    Provenance entry for a single parameter.
    
    Stores the source file, line number, column number, and timestamp
    for when a configuration parameter was loaded.
    
    Attributes:
        file (str): Absolute path to source YAML file
        line (Optional[int]): Line number in file (1-indexed), None if unavailable
        col (Optional[int]): Column number in file (1-indexed), None if unavailable
        timestamp (float): Unix timestamp when parameter was loaded
    
    Example:
        >>> entry = ProvEntry("/path/to/config.yml", line=10, col=5)
        >>> print(entry)
        ProvEntry(/path/to/config.yml:10:5)
        >>> entry.to_dict()
        {'file': '/path/to/config.yml', 'line': 10, 'col': 5, 'timestamp': 1234567890.123}
    """
    
    def __init__(self, file: str, line: Optional[int] = None,
                 col: Optional[int] = None, timestamp: Optional[float] = None):
        """
        Initialize a provenance entry.
        
        Args:
            file: Absolute path to source YAML file
            line: Line number in file (1-indexed, optional)
            col: Column number in file (1-indexed, optional)
            timestamp: Unix timestamp (defaults to current time)
        
        Example:
            >>> entry = ProvEntry("/path/to/config.yml", line=5)
            >>> entry = ProvEntry("/path/to/config.yml", line=10, col=3, timestamp=1234567890.0)
        """
        self.file = str(file)  # Ensure string (handles Path objects)
        self.line = line
        self.col = col
        self.timestamp = timestamp if timestamp is not None else time.time()
    
    def to_dict(self) -> dict:
        """
        Convert to dictionary format for serialization.
        
        Returns:
            dict: Dictionary with keys: file, timestamp, line (optional), col (optional)
        
        Example:
            >>> entry = ProvEntry("/path/file.yml", line=10)
            >>> entry.to_dict()
            {'file': '/path/file.yml', 'line': 10, 'timestamp': 1734567890.123}
        """
        result = {
            "file": self.file,
            "timestamp": self.timestamp
        }
        if self.line is not None:
            result["line"] = self.line
        if self.col is not None:
            result["col"] = self.col
        return result
    
    @classmethod
    def from_dict(cls, data: dict) -> 'ProvEntry':
        """
        Create ProvEntry from dictionary.
        
        Args:
            data: Dictionary with keys: file, timestamp, line (optional), col (optional)
        
        Returns:
            ProvEntry: New instance created from dictionary
        
        Raises:
            KeyError: If required key "file" is missing from data
        
        Example:
            >>> data = {'file': '/path/file.yml', 'line': 10, 'timestamp': 1734567890.0}
            >>> entry = ProvEntry.from_dict(data)
            >>> entry.file
            '/path/file.yml'
        """
        return cls(
            file=data["file"],
            line=data.get("line"),
            col=data.get("col"),
            timestamp=data.get("timestamp")
        )
    
    def __repr__(self):
        """
        Human-readable string representation.
        
        Returns:
            str: Format: ProvEntry(/path/file.yml:line:col) or ProvEntry(/path/file.yml)
        
        Example:
            >>> entry = ProvEntry("/path/file.yml", line=10, col=5)
            >>> repr(entry)
            'ProvEntry(/path/file.yml:10:5)'
            >>> entry2 = ProvEntry("/path/file.yml")
            >>> repr(entry2)
            'ProvEntry(/path/file.yml)'
        """
        loc = f":{self.line}" if self.line is not None else ""
        if self.col is not None:
            loc += f":{self.col}"
        return f"ProvEntry({self.file}{loc})"


class ProvenanceTracker:
    """
    Tracks configuration parameter sources.
    
    Stores provenance information in a flat dictionary using dot-separated
    parameter paths as keys (e.g., "DEFAULT.EXPID", "JOBS.SIM.WALLCLOCK").
    
    This design provides O(1) access time and simple implementation while
    allowing export to nested dict format for YAML serialization.
    
    Example:
        >>> tracker = ProvenanceTracker()
        >>> tracker.track("DEFAULT.EXPID", "/path/to/file.yml", line=2)
        >>> tracker.track("JOBS.SIM.WALLCLOCK", "/path/to/jobs.yml", line=45)
        >>> 
        >>> # Query provenance
        >>> prov = tracker.get("DEFAULT.EXPID")
        >>> print(prov.file, prov.line)
        /path/to/file.yml 2
        >>> 
        >>> # Check if parameter is tracked
        >>> "DEFAULT.EXPID" in tracker
        True
        >>> 
        >>> # Get count of tracked parameters
        >>> len(tracker)
        2
        >>> 
        >>> # Export to nested dict
        >>> nested = tracker.export_to_dict()
        >>> # {'DEFAULT': {'EXPID': {...}}, 'JOBS': {'SIM': {'WALLCLOCK': {...}}}}
    
    Attributes:
        provenance_map (Dict[str, ProvEntry]): Flat storage of provenance data
    """
    
    def __init__(self):
        """
        Initialize an empty provenance tracker.
        
        Creates an empty dictionary to store provenance entries.
        
        Example:
            >>> tracker = ProvenanceTracker()
            >>> len(tracker)
            0
        """
        self.provenance_map: Dict[str, ProvEntry] = {}
    
    def track(self, param_path: str, file: str,
              line: Optional[int] = None, col: Optional[int] = None) -> None:
        """
        Record parameter source.
        
        If the parameter already exists, this overwrites the previous entry
        (implements "last file wins" strategy).
        
        Args:
            param_path: Dot-separated parameter path (e.g., "DEFAULT.EXPID")
            file: Absolute path to source YAML file
            line: Line number in file (1-indexed, optional)
            col: Column number in file (1-indexed, optional)
        
        Example:
            >>> tracker = ProvenanceTracker()
            >>> tracker.track("DEFAULT.EXPID", "/path/config.yml", line=5)
            >>> tracker.track("JOBS.SIM.WALLCLOCK", "/path/jobs.yml", line=23, col=7)
            >>> 
            >>> # Overwrite existing entry
            >>> tracker.track("DEFAULT.EXPID", "/path/other.yml", line=10)
        
        Note:
            - Overwrites existing entry if parameter already tracked
            - Timestamp automatically set to current time
            - No validation of param_path format (caller's responsibility)
        """
        self.provenance_map[param_path] = ProvEntry(
            file=file,
            line=line,
            col=col
        )
    
    def get(self, param_path: str) -> Optional[ProvEntry]:
        """
        Get provenance for a specific parameter.
        
        Args:
            param_path: Dot-separated parameter path
        
        Returns:
            ProvEntry: Provenance entry if found
            None: If parameter not tracked
        
        Example:
            >>> tracker = ProvenanceTracker()
            >>> tracker.track("DEFAULT.EXPID", "/path/config.yml", line=5)
            >>> 
            >>> prov = tracker.get("DEFAULT.EXPID")
            >>> if prov:
            ...     print(f"EXPID from {prov.file} at line {prov.line}")
            ... else:
            ...     print("EXPID not tracked")
            EXPID from /path/config.yml at line 5
            >>> 
            >>> tracker.get("UNKNOWN.PARAM")
            None
        """
        return self.provenance_map.get(param_path)
    
    def export_to_dict(self) -> dict:
        """
        Export provenance as nested dict matching config structure.
        
        Converts flat representation (dot-separated keys) to nested dicts
        suitable for YAML serialization.
        
        Returns:
            dict: Nested dict where each leaf contains provenance info
        
        Example:
            >>> tracker = ProvenanceTracker()
            >>> tracker.track("DEFAULT.EXPID", "/path/file.yml", line=2)
            >>> tracker.track("JOBS.SIM.FILE", "/path/jobs.yml", line=10)
            >>> 
            >>> nested = tracker.export_to_dict()
            >>> nested
            {
              'DEFAULT': {'EXPID': {'file': '/path/file.yml', 'line': 2, ...}},
              'JOBS': {'SIM': {'FILE': {'file': '/path/jobs.yml', 'line': 10, ...}}}
            }
        
        Implementation Details:
            - Splits each key by dots to create nested structure
            - Uses dict.get() to avoid KeyError on nested access
            - Handles key collisions by skipping conflicting entries
        
        Edge Cases:
            - Empty tracker → Returns {}
            - Single-level key (no dots) → Returns {"key": {...}}
            - Key collision → Skips entry (shouldn't happen with valid config)
        """
        result = {}
        
        for path, prov in self.provenance_map.items():
            keys = path.split(".")
            current = result
            
            # Navigate/create nested structure
            for key in keys[:-1]:
                if key not in current:
                    current[key] = {}
                elif not isinstance(current[key], dict):
                    # Key collision: non-dict value at intermediate level
                    # This shouldn't happen with valid config, but skip if it does
                    break
                current = current[key]
            else:
                # Set leaf value (only if we didn't break out of loop)
                current[keys[-1]] = prov.to_dict()
        
        return result
    
    def import_from_dict(self, nested_dict: dict, prefix: str = "") -> None:
        """
        Import provenance from nested dict format.
        
        Converts nested dict (from experiment_data.yml PROVENANCE section)
        back to flat format for internal storage.
        
        This is the inverse operation of export_to_dict().
        
        Args:
            nested_dict: Nested provenance dict
            prefix: Current path prefix (used internally for recursion)
        
        Example:
            >>> prov_dict = {
            ...     'DEFAULT': {
            ...         'EXPID': {'file': '/path/file.yml', 'line': 2, 'timestamp': 1234.5}
            ...     },
            ...     'JOBS': {
            ...         'SIM': {'FILE': {'file': '/path/jobs.yml', 'line': 10, 'timestamp': 1234.5}}
            ...     }
            ... }
            >>> 
            >>> tracker = ProvenanceTracker()
            >>> tracker.import_from_dict(prov_dict)
            >>> tracker.get("DEFAULT.EXPID").file
            '/path/file.yml'
            >>> tracker.get("JOBS.SIM.FILE").line
            10
        
        Implementation Details:
            - Recursively traverses nested dict
            - Detects ProvEntry by presence of "file" key
            - Builds dot-separated paths during recursion
        
        Edge Cases:
            - Empty dict → No-op
            - Dict with "file" key → Treat as ProvEntry
            - Dict without "file" key → Treat as nested dict, recurse
        """
        for key, value in nested_dict.items():
            # Build parameter path
            param_path = f"{prefix}.{key}" if prefix else key
            
            if isinstance(value, dict):
                # Check if this is a ProvEntry (has "file" key)
                if "file" in value:
                    # This is a leaf node (ProvEntry)
                    self.provenance_map[param_path] = ProvEntry.from_dict(value)
                else:
                    # This is a nested dict - recurse
                    self.import_from_dict(value, param_path)
            # Note: Non-dict values are ignored (shouldn't exist in valid provenance)
    
    def clear(self) -> None:
        """
        Clear all provenance data.
        
        Removes all tracked parameters, resetting the tracker to empty state.
        
        Example:
            >>> tracker = ProvenanceTracker()
            >>> tracker.track("DEFAULT.EXPID", "/path/file.yml")
            >>> len(tracker)
            1
            >>> tracker.clear()
            >>> len(tracker)
            0
        
        Use Cases:
            - Before reloading configuration
            - When switching between experiments
            - In test cleanup
        """
        self.provenance_map.clear()
    
    def __contains__(self, param_path: str) -> bool:
        """
        Check if a parameter is tracked.
        
        Enables the 'in' operator for ProvenanceTracker.
        
        Args:
            param_path: Dot-separated parameter path
        
        Returns:
            bool: True if parameter is tracked, False otherwise
        
        Example:
            >>> tracker = ProvenanceTracker()
            >>> tracker.track("DEFAULT.EXPID", "/path/file.yml")
            >>> "DEFAULT.EXPID" in tracker
            True
            >>> "DEFAULT.UNKNOWN" in tracker
            False
        """
        return param_path in self.provenance_map
    
    def __len__(self) -> int:
        """
        Get count of tracked parameters.
        
        Enables the len() function for ProvenanceTracker.
        
        Returns:
            int: Number of parameters tracked
        
        Example:
            >>> tracker = ProvenanceTracker()
            >>> tracker.track("DEFAULT.EXPID", "/path/file.yml")
            >>> tracker.track("DEFAULT.HPCARCH", "/path/file.yml")
            >>> len(tracker)
            2
        """
        return len(self.provenance_map)
    
    def __repr__(self) -> str:
        """
        Human-readable string representation.
        
        Returns:
            str: Format: ProvenanceTracker(N parameters tracked)
        
        Example:
            >>> tracker = ProvenanceTracker()
            >>> tracker.track("DEFAULT.EXPID", "/path/file.yml")
            >>> repr(tracker)
            'ProvenanceTracker(1 parameter tracked)'
            >>> tracker.track("DEFAULT.HPCARCH", "/path/file.yml")
            >>> repr(tracker)
            'ProvenanceTracker(2 parameters tracked)'
        """
        count = len(self.provenance_map)
        plural = "parameter" if count == 1 else "parameters"
        return f"ProvenanceTracker({count} {plural} tracked)"
