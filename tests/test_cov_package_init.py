"""Coverage for graphviper package __init__ warning-suppression helper."""

import sys
import types
import warnings

import graphviper


def test_suppress_zarr_unstable_warning_category_branch():
    """The normal path imports zarr.errors.UnstableSpecificationWarning and
    registers a category-based ignore filter."""
    with warnings.catch_warnings():
        warnings.resetwarnings()
        graphviper._suppress_zarr_unstable_warning()
        # A category-based ignore filter for the zarr warning must now exist.
        from zarr.errors import UnstableSpecificationWarning

        assert any(
            action == "ignore"
            and cat is not None
            and issubclass(UnstableSpecificationWarning, cat)
            for (action, _msg, cat, _mod, _ln) in warnings.filters
        )


def test_suppress_zarr_unstable_warning_fallback_branch(monkeypatch):
    """If zarr.errors can't provide the class, the helper falls back to a
    message-based filter instead of raising."""
    # Make `from zarr.errors import UnstableSpecificationWarning` raise
    # ImportError by swapping in a bare module without that attribute.
    fake_zarr_errors = types.ModuleType("zarr.errors")
    monkeypatch.setitem(sys.modules, "zarr.errors", fake_zarr_errors)

    with warnings.catch_warnings():
        warnings.resetwarnings()
        graphviper._suppress_zarr_unstable_warning()  # must not raise
        assert any(
            action == "ignore"
            and msg is not None
            and "Zarr V3 specification" in msg.pattern
            for (action, msg, _cat, _mod, _ln) in warnings.filters
        )
