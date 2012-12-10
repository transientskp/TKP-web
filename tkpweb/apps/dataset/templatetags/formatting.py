from django import template
from django.utils.safestring import mark_safe
from tkp.utility.coordinates import (ratohms, dectodms)
register = template.Library()


@register.filter
def prefixformat(value, arg):
    prefixes = {'M': 1e6, 'k': 1e3, 'G': 1e9, 'T': 1e12, 'P': 1e15, 'E': 1e18,
                'm': 1e-3, 'u': 1e-6, 'n': 1e-9, 'p': 1e-12, 'f': 1e-15}
    try:
        value /= prefixes[arg]
    except Exception:
        pass
    return value

@register.filter
def format_angle(value, format_type):
    if format_type == "time":
        h, m, s = ratohms(value)
        result = "%d:%d:%.1f" % (h, m, s)
    if format_type == "dms":
        d, m, s = dectodms(value)
        if d > 0:
            sign = '+'
        else:
            sign = '-'
        result = "%s%d:%d:%.1f" % (sign, d, m, s)
    return mark_safe(result)
