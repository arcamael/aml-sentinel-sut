"""Country-name → ISO-3166-1 alpha-2 mapping.

Dependency-free and deterministic (hard rule #1): a curated table rather than a
library lookup, so the same input always yields the same ISO-2 code regardless
of the host's locale data. Keys are matched after transliteration + lowercasing
(see :func:`aml_sentinel.matching.normalize.normalize_country`), so both
``"Russia"`` and ``"russian federation"`` resolve to ``RU``.

Covers the countries exercised by the generated datasets (doc 04). Unknown
inputs return ``None`` and are recorded in ``fields_defaulted``.
"""

from __future__ import annotations

# Full / common names → ISO-2. Lowercase, ASCII (post-transliteration) keys.
COUNTRY_TO_ISO2: dict[str, str] = {
    # — Frequently used in the golden / profile data —
    "russia": "RU",
    "russian federation": "RU",
    "cyprus": "CY",
    "ukraine": "UA",
    "united kingdom": "GB",
    "great britain": "GB",
    "england": "GB",
    "united states": "US",
    "united states of america": "US",
    "usa": "US",
    "germany": "DE",
    "deutschland": "DE",
    "france": "FR",
    "spain": "ES",
    "italy": "IT",
    "netherlands": "NL",
    "switzerland": "CH",
    "china": "CN",
    "iran": "IR",
    "islamic republic of iran": "IR",
    "syria": "SY",
    "syrian arab republic": "SY",
    "north korea": "KP",
    "south korea": "KR",
    "korea": "KR",
    "belarus": "BY",
    "kazakhstan": "KZ",
    "turkey": "TR",
    "turkiye": "TR",
    "poland": "PL",
    "portugal": "PT",
    "greece": "GR",
    "austria": "AT",
    "belgium": "BE",
    "sweden": "SE",
    "norway": "NO",
    "finland": "FI",
    "denmark": "DK",
    "ireland": "IE",
    "india": "IN",
    "japan": "JP",
    "brazil": "BR",
    "canada": "CA",
    "australia": "AU",
    "mexico": "MX",
    "egypt": "EG",
    "saudi arabia": "SA",
    "united arab emirates": "AE",
    "uae": "AE",
    "israel": "IL",
    "south africa": "ZA",
    "nigeria": "NG",
    "argentina": "AR",
    "venezuela": "VE",
    "georgia": "GE",
    "armenia": "AM",
    "azerbaijan": "AZ",
    "moldova": "MD",
    "estonia": "EE",
    "latvia": "LV",
    "lithuania": "LT",
    "malta": "MT",
    "luxembourg": "LU",
    "romania": "RO",
    "bulgaria": "BG",
    "hungary": "HU",
    "czechia": "CZ",
    "czech republic": "CZ",
    "slovakia": "SK",
    "slovenia": "SI",
    "croatia": "HR",
    "serbia": "RS",
    "indonesia": "ID",
    "zimbabwe": "ZW",
}

# The set of ISO-2 codes we recognise as already-canonical pass-through input.
VALID_ISO2: frozenset[str] = frozenset(COUNTRY_TO_ISO2.values())
