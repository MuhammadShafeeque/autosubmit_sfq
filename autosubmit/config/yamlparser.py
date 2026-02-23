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


from ruamel.yaml import YAML
from yaml_provenance import load_yaml, ProvenanceConfig, configure


class YAMLParserFactory:
    def __init__(self):
        pass

    def create_parser(self):
        return YAMLParser()


class YAMLParser(YAML):

    def __init__(self):
        self.data = []
        self.category = None
        super(YAMLParser, self).__init__(typ="safe")
    
    def load(self, stream, category=None):
        """
        Load YAML with provenance tracking.
        
        Parameters
        ----------
        stream : str, Path, or file-like object
            The YAML file path or file object to load
        category : str, optional
            Category for provenance tracking
            
        Returns
        -------
        DictWithProvenance
            The loaded data with provenance
        """
        # Store category for potential later use
        if category is not None:
            self.category = category
        
        # If stream is a file-like object, get its file path
        # Use 'read' attribute to distinguish file objects from Path objects
        # (Path objects have .name but it only returns the filename, not the full path)
        if hasattr(stream, 'read'):
            filepath = stream.name
        else:
            # Ensure Path objects are converted to strings for yaml_provenance
            filepath = str(stream)
        
        # Simple category resolver that uses the provided category
        def category_resolver(path):
            return (self.category, None)
        
        # Use yaml_provenance to load the file
        return load_yaml(filepath, category_resolver=category_resolver if self.category else None)
