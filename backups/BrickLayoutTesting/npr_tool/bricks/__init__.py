"""
====================================================================
BRICKS PACKAGE INITIALIZER
====================================================================
Automatically imports all brick modules in this folder so that each
@registry.register() decorator executes and registers the brick.



❌ Do not turn schemas into logic
❌ Do not validate everything early
❌ Do not add branching runners yet
❌ Do not let bricks grow modes
❌ Do not chase “framework purity”
====================================================================
"""

import importlib
import pkgutil

# Discover all brick modules in this package and import them dynamically
for loader, module_name, is_pkg in pkgutil.walk_packages(__path__, prefix=__name__ + "."):
    importlib.import_module(module_name)