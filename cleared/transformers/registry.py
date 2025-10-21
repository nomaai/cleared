"""Transformer Registry for managing and instantiating transformers."""

from omegaconf import DictConfig
from .base import BaseTransformer


def get_expected_transformer_names() -> list[str]:
    """
    Get the list of expected transformer names that should be auto-discovered.

    This function performs the same auto-discovery logic as _register_default_transformers
    but returns just the names for testing purposes.

    Returns:
        List of transformer class names that should be auto-discovered

    """
    import inspect
    import importlib
    import os

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
        import inspect
        import importlib
        import os

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
                        print(f"Warning: Could not import module {module_name}: {e}")

        except Exception as e:
            # Handle any other errors during auto-discovery
            print(f"Warning: Error during transformer auto-discovery: {e}")

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
            raise TypeError(
                f"transformer_class must be a subclass of BaseTransformer, "
                f"got {type(transformer_class)}"
            )

        if name in self._registry:
            raise ValueError(f"Transformer '{name}' is already registered")

        self._registry[name] = transformer_class

    def unregister(self, name: str) -> None:
        """
        Unregister a transformer class.

        Args:
            name: Name of the transformer to unregister

        Raises:
            KeyError: If transformer is not registered

        """
        if name not in self._registry:
            raise KeyError(f"Transformer '{name}' is not registered")

        del self._registry[name]

    def instantiate(self, name: str, configs: DictConfig) -> BaseTransformer:
        """
        Instantiate a transformer from its name and configuration.

        Args:
            name: Name of the transformer to instantiate
            configs: Hydra DictConfig object containing transformer configuration

        Returns:
            An instance of the requested transformer class

        Raises:
            KeyError: If transformer name is not found in registry
            TypeError: If the transformer cannot be instantiated with the given configs

        Example:
            >>> from omegaconf import DictConfig
            >>> registry = TransformerRegistry()
            >>> config = DictConfig({"column": "patient_id"})
            >>> transformer = registry.instantiate("IDDeidentifier", config)

        """
        if name not in self._registry:
            available_transformers = list(self._registry.keys())
            raise KeyError(
                f"Unknown transformer '{name}'. "
                f"Available transformers: {available_transformers}"
            )

        transformer_class = self._registry[name]

        try:
            # Convert DictConfig to dict for transformer constructors
            if hasattr(configs, "_content"):
                # It's a DictConfig, convert to dict
                config_dict = dict(configs)
            else:
                # It's already a dict
                config_dict = configs

            # Special handling for transformers that expect IdentifierConfig objects
            if config_dict is not None and "idconfig" in config_dict:
                from cleared.config.structure import IdentifierConfig

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

            # Special handling for transformers that expect DeIDConfig objects
            if config_dict is not None and "deid_config" in config_dict:
                from cleared.config.structure import DeIDConfig, TimeShiftConfig

                # Handle both dict and DictConfig cases
                if isinstance(config_dict["deid_config"], dict):
                    deid_data = config_dict["deid_config"]
                    time_shift_data = deid_data.get("time_shift", {})
                    time_shift = (
                        TimeShiftConfig(**time_shift_data) if time_shift_data else None
                    )
                    config_dict["deid_config"] = DeIDConfig(time_shift=time_shift)
                elif hasattr(config_dict["deid_config"], "_content"):
                    # It's a DictConfig, convert to dict first
                    deid_data = dict(config_dict["deid_config"])
                    time_shift_data = deid_data.get("time_shift", {})
                    time_shift = (
                        TimeShiftConfig(**time_shift_data) if time_shift_data else None
                    )
                    config_dict["deid_config"] = DeIDConfig(time_shift=time_shift)

            # Handle None config case
            if config_dict is None:
                return transformer_class()
            else:
                return transformer_class(**config_dict)
        except Exception as e:
            raise TypeError(
                f"Failed to create transformer '{name}' with configs: {e}"
            ) from e

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
            raise KeyError(
                f"Unknown transformer '{name}'. "
                f"Available transformers: {available_transformers}"
            )

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
