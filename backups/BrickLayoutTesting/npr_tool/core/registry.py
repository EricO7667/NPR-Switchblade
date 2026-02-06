"""
====================================================================
BRICK REGISTRY — The Brain That Knows All Available Bricks
====================================================================

PURPOSE:
- Keeps a mapping of brick names → brick classes.
- Allows the runner to dynamically instantiate bricks by name.
- Makes the system *self-extensible*: add new bricks by just importing.

Data Flow: Runner → Registry → Brick Constructor → Brick.run(Payload)
====================================================================
"""

class BrickRegistry:
    """Central registry for all brick classes."""

    def __init__(self):
        self._registry = {}

    # ==============================================================
    # register()
    # --------------------------------------------------------------
    # Used as a decorator:
    #   @registry.register("importer_excel")
    #   class ImporterExcel(Brick): ...
    #
    # This binds the string name to the class.
    # ==============================================================
    def register(self, name):
        def wrapper(cls):
            self._registry[name] = cls
            return cls
        return wrapper

    # ==============================================================
    # get()
    # --------------------------------------------------------------
    # Fetches a registered brick class by name. If not found, fail
    # loudly to prevent silent misconfigurations.
    # ==============================================================
    def get(self, name):
        if name not in self._registry:
            raise KeyError(f" Brick '{name}' not registered in registry.")
        return self._registry[name]

registry = BrickRegistry()
