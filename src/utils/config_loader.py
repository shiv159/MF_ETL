"""Configuration loader for YAML config files"""

import os
import yaml
from typing import Any, Dict


def load_config(config_path: str = "config/config.yaml") -> Dict[str, Any]:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        Dictionary containing configuration
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        yaml.YAMLError: If config file is invalid
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    return config if config else {}


def get_validation_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Extract validation configuration"""
    return config.get('validation', {})


def get_logging_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Extract logging configuration"""
    return config.get('logging', {})
