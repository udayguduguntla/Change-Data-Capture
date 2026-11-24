"""Shared error types."""


class ConfigError(Exception):
    """Static configuration is invalid. Fail loudly at startup, never continue."""


class MappingError(ConfigError):
    """A mapping entry is structurally unusable."""


class CoercionError(Exception):
    """A single value could not be safely converted. Quarantine the event."""


class ProvisioningError(Exception):
    """Database or constraint provisioning failed."""


class PreflightError(Exception):
    """A source-system prerequisite is not met."""
