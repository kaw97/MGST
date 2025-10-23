"""Dynamic configuration loader for modular search profiles."""

import importlib
import importlib.util
from pathlib import Path
from typing import Dict, Type, Any, Optional
import inspect

from .base import BaseConfig


class LegacyConfigWrapper(BaseConfig):
    """Wrapper for legacy configuration modules that can be pickled."""
    
    def __init__(self, module_name: str, module_doc: str, module_path: str, output_columns: list[str]):
        super().__init__(
            name=f"legacy-{module_name}",
            description=module_doc.strip()
        )
        self.module_path = module_path
        self.output_columns = output_columns
        self._module = None
    
    def _get_module(self):
        """Lazy load module to avoid pickle issues."""
        if self._module is None:
            if self.module_path:
                spec = importlib.util.spec_from_file_location("user_config", self.module_path)
                if spec and spec.loader:
                    self._module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(self._module)
        return self._module
    
    def filter_system(self, system_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Use the legacy filter_system function."""
        mod = self._get_module()
        return mod.filter_system(system_data)
    
    def get_output_columns(self) -> list[str]:
        """Return cached output columns."""
        return self.output_columns


class ConfigurationLoader:
    """Loads and manages search configurations dynamically."""
    
    def __init__(self):
        self._loaded_configs: Dict[str, Type[BaseConfig]] = {}
        self._register_builtin_configs()
    
    def _register_builtin_configs(self):
        """Register built-in configurations."""
        from .exobiology import ExobiologyConfig
        from .high_value_exobiology import HighValueExobiologyConfig
        from .rule_based_exobiology_10m_selective import RuleBasedExobiologySelectiveConfig
        from .binary_body_search import BinaryBodySearchConfig
        from .faction_search import FactionSearchConfig
        from .biological_landmarks import BiologicalLandmarksConfig

        self._loaded_configs['exobiology'] = ExobiologyConfig
        self._loaded_configs['high-value-exobiology'] = HighValueExobiologyConfig
        self._loaded_configs['rule-based-exobiology-selective'] = RuleBasedExobiologySelectiveConfig
        self._loaded_configs['binary-body-search'] = BinaryBodySearchConfig
        self._loaded_configs['faction-search'] = FactionSearchConfig
        self._loaded_configs['biological-landmarks'] = BiologicalLandmarksConfig
    
    def load_config_from_file(self, config_path: Path) -> BaseConfig:
        """Load configuration from a Python file.
        
        Args:
            config_path: Path to Python configuration file
            
        Returns:
            Instantiated configuration object
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If config file is invalid
        """
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        # Load the module dynamically
        spec = importlib.util.spec_from_file_location("user_config", config_path)
        if spec is None or spec.loader is None:
            raise ValueError(f"Could not load configuration from: {config_path}")
        
        module = importlib.util.module_from_spec(spec)
        
        try:
            spec.loader.exec_module(module)
        except Exception as e:
            raise ValueError(f"Error executing configuration file: {e}")
        
        # Look for a configuration class that inherits from BaseConfig
        config_class = None
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if (obj != BaseConfig and 
                issubclass(obj, BaseConfig) and 
                obj.__module__ == module.__name__):
                config_class = obj
                break
        
        # Fallback: look for legacy filter_system function (for backward compatibility)
        if config_class is None:
            if hasattr(module, 'filter_system') and callable(module.filter_system):
                # Create a wrapper instance for legacy configs
                return self._create_legacy_wrapper(module)
            else:
                raise ValueError(
                    f"Configuration file must either:\n"
                    f"1. Define a class that inherits from BaseConfig, or\n"
                    f"2. Define a filter_system(system_data) function (legacy mode)"
                )
        
        # Instantiate and return the configuration
        return config_class()
    
    def load_config_by_name(self, config_name: str) -> BaseConfig:
        """Load a built-in configuration by name.
        
        Args:
            config_name: Name of the built-in configuration
            
        Returns:
            Instantiated configuration object
            
        Raises:
            ValueError: If configuration name is not found
        """
        if config_name not in self._loaded_configs:
            raise ValueError(f"Unknown configuration: {config_name}")
        
        config_class = self._loaded_configs[config_name]
        return config_class()
    
    def register_config(self, name: str, config_class: Type[BaseConfig]):
        """Register a new configuration class.
        
        Args:
            name: Name to register the configuration under
            config_class: Configuration class that inherits from BaseConfig
        """
        if not issubclass(config_class, BaseConfig):
            raise ValueError("Configuration class must inherit from BaseConfig")
        
        self._loaded_configs[name] = config_class
    
    def list_available_configs(self) -> Dict[str, str]:
        """List all available built-in configurations.
        
        Returns:
            Dictionary mapping config names to descriptions
        """
        configs = {}
        for name, config_class in self._loaded_configs.items():
            try:
                instance = config_class()
                configs[name] = instance.get_description()
            except Exception as e:
                configs[name] = f"Error loading description: {e}"
        
        return configs
    
    def _create_legacy_wrapper(self, module) -> BaseConfig:
        """Create a wrapper instance for legacy configuration modules.
        
        Args:
            module: Module with filter_system function and optionally OUTPUT_COLUMNS
            
        Returns:
            Wrapper instance that inherits from BaseConfig
        """
        # Store module attributes to avoid pickle issues with closures
        module_name = getattr(module, '__name__', 'unknown')
        module_doc = getattr(module, '__doc__', 'Legacy configuration') or 'Legacy configuration'
        module_path = getattr(module, '__file__', None)
        
        # Get output columns
        output_columns = []
        if hasattr(module, 'OUTPUT_COLUMNS') and module.OUTPUT_COLUMNS:
            output_columns = [col[0] for col in module.OUTPUT_COLUMNS]
        else:
            output_columns = [
                'system_name',
                'coords_x',
                'coords_y', 
                'coords_z',
                'total_bodies',
                'distance_from_sol'
            ]
        
        # Return a direct instance of LegacyConfigWrapper
        return LegacyConfigWrapper(module_name, module_doc, module_path, output_columns)


# Global configuration loader instance
config_loader = ConfigurationLoader()