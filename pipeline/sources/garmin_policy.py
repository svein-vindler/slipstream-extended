"""Shared policy exception; transport/parser imports stay acyclic."""


class GarminReadOnlyViolation(PermissionError):
    """A blocked operation; errors deliberately omit URLs, bodies and tokens."""
