# src/prompts/base.py
class PromptBase:
    sys_template: str
    user_template: str

    def render_sys(self, **kwargs) -> str:
        """Simple .format() wrapper so subclasses only build kwargs."""
        return self.sys_template.format(**kwargs)

    def render_user(self, **kwargs) -> str:
        """Simple .format() wrapper so subclasses only build kwargs."""
        return self.user_template.format(**kwargs)
