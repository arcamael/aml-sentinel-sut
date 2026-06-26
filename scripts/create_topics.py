"""Create the five Kafka topics (doc 02 §1) — portable, idempotent.

Used by CI (where ``rpk`` isn't available) and re-runnable locally. Mirrors
``scripts/bootstrap_topics.sh``.
"""

from __future__ import annotations

import sys

from confluent_kafka.admin import AdminClient, NewTopic

from aml_sentinel.config import settings

TOPICS = {
    "client.submitted": 3,
    "profile.normalized": 3,
    "screening.completed": 3,
    "decision.made": 3,
    "watchlist.updated": 1,
}


def main() -> int:
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    existing = set(admin.list_topics(timeout=10).topics)
    to_create = [
        NewTopic(name, num_partitions=parts, replication_factor=1)
        for name, parts in TOPICS.items()
        if name not in existing
    ]
    if not to_create:
        print("all topics already exist")
        return 0
    for name, future in admin.create_topics(to_create).items():
        try:
            future.result()
            print(f"created {name}")
        except Exception as exc:  # noqa: BLE001
            print(f"topic {name}: {exc}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
