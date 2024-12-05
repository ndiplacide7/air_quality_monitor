from django import template
from django.forms.boundfield import BoundField

register = template.Library()


@register.filter(name='add_class')
def add_class(value, arg):
    """Adds a CSS class to a form field"""
    if isinstance(value, BoundField):
        existing_classes = value.field.widget.attrs.get('class', '')
        classes = f"{existing_classes} {arg}".strip()
        return value.as_widget(attrs={'class': classes})
    return value
