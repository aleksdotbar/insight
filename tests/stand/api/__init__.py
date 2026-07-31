"""API assertions against the deployed stand.

A package so the 401 and 200 suites can share `routes.py` through a relative
import — which is what stops the two halves drifting onto different URLs and
quietly proving nothing about each other.
"""
