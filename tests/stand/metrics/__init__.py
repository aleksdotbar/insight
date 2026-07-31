"""Metric-value assertions against the deployed stand, through the API only.

A package for the same reason `tests/stand/api/` is one: modules here are named
after what they measure, and those names recur across directories. Without
`__init__.py` pytest imports test modules by bare basename and the second of two
same-named files collides with the first.

No browser is used here. This suite asserts NUMBERS, and a number is verifiable
through the API — putting it in a journey would make it slower and less precise
for nothing.
"""
