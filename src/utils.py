"""
Utility functions for ETL logger.
"""

from typing import Dict, Any
import yaml
import os


def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """
    Load configuration from YAML file.

    Args:
        config_path: Path to configuration file

    Returns:
        Dict containing configuration
    """
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    # Replace environment variables
    config = _replace_env_vars(config)

    return config


def _replace_env_vars(config: Any) -> Any:
    """
    Recursively replace environment variable placeholders.

    Args:
        config: Configuration object (dict, list, or str)

    Returns:
        Configuration with environment variables resolved
    """
    if isinstance(config, dict):
        return {k: _replace_env_vars(v) for k, v in config.items()}
    elif isinstance(config, list):
        return [_replace_env_vars(item) for item in config]
    elif isinstance(config, str) and config.startswith('${') and config.endswith('}'):
        env_var = config[2:-1]
        return os.getenv(env_var, config)
    else:
        return config


def validate_status_code(status: str, config: Dict[str, Any]) -> bool:
    """
    Validate status code against configured values.

    Args:
        status: Status code to validate
        config: Configuration dictionary

    Returns:
        bool: True if valid, False otherwise
    """
    valid_statuses = set(config.get('status_codes', {}).values())
    return status in valid_statuses


def validate_process_step(step: str, config: Dict[str, Any]) -> bool:
    """
    Validate process step against configured values.

    Args:
        step: Process step to validate
        config: Configuration dictionary

    Returns:
        bool: True if valid, False otherwise
    """
    valid_steps = set(config.get('process_steps', {}).values())
    return step in valid_steps