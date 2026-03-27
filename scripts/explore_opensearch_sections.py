"""List unique values for section_anchor, section_type, and doc_type in the specialite_sections index.

Usage:
    poetry run python scripts/explore_opensearch_sections.py
"""

import sys

sys.path.insert(0, "src")

from infomedicament_dataeng.opensearch.client import get_opensearch_client

INDEX = "specialite_sections"


def main() -> None:
    client = get_opensearch_client()

    resp = client.search(
        index=INDEX,
        body={
            "size": 0,
            "aggs": {
                "by_doc_type": {
                    "terms": {"field": "doc_type"},
                    "aggs": {
                        "anchors": {
                            "terms": {"field": "section_anchor", "size": 500},
                            "aggs": {"sample_titles": {"top_hits": {"size": 5, "_source": ["section_title"]}}},
                        },
                        "types": {"terms": {"field": "section_type", "size": 50}},
                    },
                }
            },
        },
    )

    for bucket in resp["aggregations"]["by_doc_type"]["buckets"]:
        doc_type = bucket["key"]
        total = bucket["doc_count"]
        print(f"\n=== {doc_type} ({total:,} docs) ===")

        print("\n  -- section_type --")
        for b in bucket["types"]["buckets"]:
            print(f"  {b['doc_count']:8,}  {b['key']}")

        print("\n  -- section_anchor --")
        for b in bucket["anchors"]["buckets"]:
            titles = [h["_source"]["section_title"] for h in b["sample_titles"]["hits"]["hits"]]
            unique_titles = list(dict.fromkeys(titles))  # deduplicate, preserve order
            print(f"  {b['doc_count']:8,}  {b['key']:<40}  {unique_titles}")


if __name__ == "__main__":
    main()
