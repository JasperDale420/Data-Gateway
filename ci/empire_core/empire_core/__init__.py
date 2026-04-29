"""empire_core — CI vendor stub for Data-Gateway.

Provides the subset of the real empire-core package that Data-Gateway
imports.  The full package lives at ../empire-core in the monorepo and is
installed via uv for local development.  This stub is only used by CI,
which checks out Data-Gateway as a standalone repo.
"""
