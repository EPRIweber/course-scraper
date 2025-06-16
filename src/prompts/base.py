# src/prompts/base.py
class PromptBase:
    template: str

    def render(self, **kwargs) -> str:
        """Simple .format() wrapper so subclasses only build kwargs."""
        return self.template.format(**kwargs)
