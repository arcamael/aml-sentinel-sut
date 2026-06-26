"""Mock third-party providers for AML-Sentinel (doc 01 §7).

Each mock is (a) seedable to the generated watchlists, (b) controllable via
``/_control/*`` for fault injection, and (c) observable via ``/health`` and
``/_state``. Mocks return data and faults only — never business logic that
belongs in the SUT (matching/scoring live in the screening worker).
"""
