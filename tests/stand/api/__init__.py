"""API assertions against the deployed stand, one module per service.

`test_analytics.py` and `test_identity.py` each own one gateway prefix and
assert both its refusal and its success against a single path constant, so
neither half can drift onto a url the other does not use.

Still a package, for one concrete reason: modules are now named after the
service they exercise, and a service reappears across directories — a
`metrics/test_analytics.py` next to this `api/test_analytics.py` is the natural
naming. Without `__init__.py` pytest imports test modules by bare basename and
the second of those two collides with the first.
"""
