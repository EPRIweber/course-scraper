prompt_registry = {}

def register(name):
    def decorator(cls):
        prompt_registry[name] = cls
        return cls
    return decorator
