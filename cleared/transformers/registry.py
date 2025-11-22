"""Transformer Registry for managing and instantiating transformers."""

from __future__ import annotations

import inspect
import importlib
import os
import logging
from omegaconf import DictConfig
from .base import BaseTransformer
from cleared.config.structure import DeIDConfig, IdentifierConfig

# Set up logger for this module
logger = logging.getLogger(__name__)


def get_expected_transformer_names() -> list[str]:
    """
    Get the list of expected transformer names that should be auto-discovered.

    This function performs the same auto-discovery logic as _register_default_transformers
    but returns just the names for testing purposes.

    Returns:
        List of transformer class names that should be auto-discovered

    """
    transformer_names = []

    try:
        # Get the current package directory
        current_dir = os.path.dirname(__file__)

        # Get all Python files in the transformers package
        for filename in os.listdir(current_dir):
            if filename.endswith(".py") and not filename.startswith("__"):
                module_name = filename[:-3]  # Remove .py extension

                try:
                    # Import the module
                    module = importlib.import_module(
                        f"cleared.transformers.{module_name}"
                    )

                    # Get all classes from the module
                    for name, obj in inspect.getmembers(module, inspect.isclass):
                        # Check if the class is defined in this module (not imported)
                        if obj.__module__ == f"cleared.transformers.{module_name}":
                            # Check if it's a subclass of BaseTransformer and not abstract
                            if (
                                issubclass(obj, BaseTransformer)
                                and not inspect.isabstract(obj)
                                and obj is not BaseTransformer
                            ):
                                transformer_names.append(name)

                except ImportError:
                    # Skip modules that can't be imported
                    continue

    except Exception:
        # Return empty list if auto-discovery fails
        pass

    return sorted(transformer_names)


class TransformerRegistry:
    """
    Registry for managing transformer classes and their instantiation.

    This class provides a centralized way to register, manage, and instantiate
    transformer classes. It supports both default built-in transformers and
    custom user-defined transformers.

    Attributes:
        _registry: Dictionary mapping transformer names to their classes

    """

    def __init__(
        self,
        use_defaults: bool = True,
        custom_transformers: dict[str, type[BaseTransformer]] | None = None,
    ):
        """
        Initialize the transformer registry.

        Args:
            use_defaults: Whether to register default built-in transformers
            custom_transformers: Optional dictionary of custom transformer classes
                                to register initially

        """
        self._registry: dict[str, type[BaseTransformer]] = {}

        if use_defaults:
            self._register_default_transformers()

        if custom_transformers:
            for name, transformer_class in custom_transformers.items():
                self.register(name, transformer_class)

    def _register_default_transformers(self) -> None:
        """Register all non-abstract classes that extend BaseTransformer from the transformers package."""
        try:
            # Get the current package directory
            current_dir = os.path.dirname(__file__)

            # Get all Python files in the transformers package
            for filename in os.listdir(current_dir):
                if filename.endswith(".py") and not filename.startswith("__"):
                    module_name = filename[:-3]  # Remove .py extension

                    try:
                        # Import the module
                        module = importlib.import_module(
                            f"cleared.transformers.{module_name}"
                        )

                        # Get all classes from the module
                        for name, obj in inspect.getmembers(module, inspect.isclass):
                            # Check if the class is defined in this module (not imported)
                            if obj.__module__ == f"cleared.transformers.{module_name}":
                                # Check if it's a subclass of BaseTransformer and not abstract
                                if (
                                    issubclass(obj, BaseTransformer)
                                    and not inspect.isabstract(obj)
                                    and obj is not BaseTransformer
                                ):
                                    # Register the class using its name
                                    self._registry[name] = obj

                    except ImportError as e:
                        # Handle case where some modules might not be available
                        logger.warning(f"Could not import module {module_name}: {e}")

        except Exception as e:
            # Handle any other errors during auto-discovery
            logger.warning(f"Error during transformer auto-discovery: {e}")

    def register(self, name: str, transformer_class: type[BaseTransformer]) -> None:
        """
        Register a transformer class.

        Args:
            name: Name to register the transformer under
            transformer_class: The transformer class to register

        Raises:
            TypeError: If transformer_class is not a subclass of BaseTransformer
            ValueError: If name is already registered

        """
        if not issubclass(transformer_class, BaseTransformer):
            error_msg = f"transformer_class must be a subclass of BaseTransformer, got {type(transformer_class)}"
            logger.error(f"Registry {error_msg}")
            raise TypeError(error_msg)

        if name in self._registry:
            error_msg = f"Transformer '{name}' is already registered"
            logger.error(f"Registry {error_msg}")
            raise ValueError(error_msg)

        self._registry[name] = transformer_class
        logger.debug(f"Registry registered transformer: {name}")

    def unregister(self, name: str) -> None:
        """
        Unregister a transformer class.

        Args:
            name: Name of the transformer to unregister

        Raises:
            KeyError: If transformer is not registered

        """
        if name not in self._registry:
            error_msg = f"Transformer '{name}' is not registered"
            logger.error(f"Registry {error_msg}")
            raise KeyError(error_msg)

        del self._registry[name]
        logger.debug(f"Registry unregistered transformer: {name}")

    def instantiate(
        self,
        name: str,
        configs: DictConfig,
        uid: str | None = None,
        global_deid_config: DeIDConfig | None = None,
    ) -> BaseTransformer:
        """
        Instantiate a transformer from its name and configuration.

        Args:
            name: Name of the transformer to instantiate
            configs: Hydra DictConfig object containing transformer configuration
            uid: Unique identifier for the transformer
            global_deid_config: Global de-identification configuration to pass to transformers

        Returns:
            An instance of the requested transformer class

        Raises:
            KeyError: If transformer name is not found in registry
            TypeError: If the transformer cannot be instantiated with the given configs

        Example:
            >>> from omegaconf import DictConfig
            >>> registry = TransformerRegistry()
            >>> config = DictConfig({"column": "patient_id"})
            >>> transformer = registry.instantiate("IDDeidentifier", config, global_deid_config)

        """
        if name not in self._registry:
            available_transformers = list(self._registry.keys())
            error_msg = f"Unknown transformer '{name}'. Available transformers: {available_transformers}"
            logger.error(f"Registry {error_msg}")
            raise KeyError(error_msg)

        transformer_class = self._registry[name]

        try:
            # Get the transformer's __init__ signature to see what parameters it accepts
            sig = inspect.signature(transformer_class.__init__)
            accepted_params = set(sig.parameters.keys()) - {"self"}  # Exclude 'self'

            # Check if the transformer accepts **kwargs (var_keyword parameter)
            accepts_kwargs = any(
                param.kind == inspect.Parameter.VAR_KEYWORD
                for param in sig.parameters.values()
            )

            # Convert DictConfig to dict for transformer constructors
            if hasattr(configs, "_content"):
                # It's a DictConfig, convert to dict
                config_dict = dict(configs) if configs is not None else {}
            else:
                # It's already a dict or None
                config_dict = configs if configs is not None else {}

            # Special handling for transformers that expect IdentifierConfig objects
            if "idconfig" in config_dict:
                # Handle both dict and DictConfig cases
                if isinstance(config_dict["idconfig"], dict):
                    config_dict["idconfig"] = IdentifierConfig(
                        **config_dict["idconfig"]
                    )
                elif hasattr(config_dict["idconfig"], "_content"):
                    # It's a DictConfig, convert to dict first
                    config_dict["idconfig"] = IdentifierConfig(
                        **dict(config_dict["idconfig"])
                    )

            # Check for unexpected parameters in config_dict (excluding special ones we handle)
            # Only check if transformer doesn't accept **kwargs
            if not accepts_kwargs:
                special_params = {"uid", "deid_config"}
                unexpected_params = [
                    key
                    for key in config_dict.keys()
                    if key not in special_params and key not in accepted_params
                ]
                if unexpected_params:
                    # Match Python's error message format for unexpected keyword arguments
                    if len(unexpected_params) == 1:
                        raise TypeError(
                            f"{transformer_class.__name__}.__init__() got an unexpected keyword argument '{unexpected_params[0]}'"
                        )

                    elif len(unexpected_params) > 1:
                        param_str = ", ".join(f"'{p}'" for p in unexpected_params)
                        raise TypeError(
                            f"{transformer_class.__name__}.__init__() got unexpected keyword arguments: {param_str}"
                        )

            # Build the argument dictionary based on what the transformer accepts
            init_kwargs = {}

            # Add uid if transformer accepts it and uid is provided
            if "uid" in accepted_params and uid is not None:
                init_kwargs["uid"] = uid

            # Add global_deid_config if transformer accepts it and it's provided
            if (
                "global_deid_config" in accepted_params
                and global_deid_config is not None
            ):
                init_kwargs["global_deid_config"] = global_deid_config

            # Add all other config parameters
            # If transformer accepts **kwargs, include all params (except special ones)
            # Otherwise, only include params that are explicitly accepted
            special_params = {"uid", "deid_config"}
            for key, value in config_dict.items():
                if key not in special_params:
                    if accepts_kwargs or key in accepted_params:
                        init_kwargs[key] = value

            # Instantiate the transformer with the built arguments
            return transformer_class(**init_kwargs)

        except Exception as e:
            error_msg = f"Failed to create transformer '{name}' with configs: {e}"
            logger.error(f"Registry {error_msg}")
            raise TypeError(error_msg) from e

    def get_class(self, name: str) -> type[BaseTransformer]:
        """
        Get the transformer class by name.

        Args:
            name: Name of the transformer

        Returns:
            The transformer class

        Raises:
            KeyError: If transformer name is not found in registry

        """
        if name not in self._registry:
            available_transformers = list(self._registry.keys())
            error_msg = f"Unknown transformer '{name}'. Available transformers: {available_transformers}"
            logger.error(f"Registry {error_msg}")
            raise KeyError(error_msg)

        return self._registry[name]

    def list_available(self) -> list[str]:
        """
        Get a list of all available transformer names.

        Returns:
            List of transformer names that can be used with instantiate

        """
        return list(self._registry.keys())

    def is_registered(self, name: str) -> bool:
        """
        Check if a transformer is registered.

        Args:
            name: Name of the transformer to check

        Returns:
            True if the transformer is registered, False otherwise

        """
        return name in self._registry

    def get_registry_info(self) -> dict[str, str]:
        """
        Get information about all registered transformers.

        Returns:
            Dictionary mapping transformer names to their class names

        """
        return {name: cls.__name__ for name, cls in self._registry.items()}

    def clear(self) -> None:
        """Clear all registered transformers."""
        self._registry.clear()

    def __len__(self) -> int:
        """Return the number of registered transformers."""
        return len(self._registry)

    def __contains__(self, name: str) -> bool:
        """Check if a transformer is registered."""
        return name in self._registry

    def __repr__(self) -> str:
        """Return string representation of the registry."""
        return f"TransformerRegistry({len(self._registry)} transformers: {list(self._registry.keys())})"
