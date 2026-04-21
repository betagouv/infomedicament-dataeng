"""Run a k-NN search against the notice_chunks index using the Albert API.

Usage:
    ALBERT_API_KEY=... poetry run python scripts/search_notice_chunks.py "puis-je boire de l'alcool ?"
"""

import sys

from infomedicament_dataeng.opensearch.client import get_opensearch_client
from infomedicament_dataeng.opensearch.notice_chunks import DEFAULT_INDEX, _embed_texts, _get_albert_client

QUERY = sys.argv[1] if len(sys.argv) > 1 else "puis-je boire de l'alcool ?"
TOP_K = 5


def main() -> None:
    os_client = get_opensearch_client()
    embed_client, embed_model = _get_albert_client()

    print(f"Query: {QUERY!r}\n")
    [embedding] = _embed_texts([QUERY], embed_client, embed_model)

    resp = os_client.search(
        index=DEFAULT_INDEX,
        body={
            "size": TOP_K,
            "query": {"knn": {"embedding": {"vector": embedding, "k": TOP_K}}},
            "_source": ["cis", "section_title", "sub_header", "text"],
        },
    )

    for hit in resp["hits"]["hits"]:
        s = hit["_source"]
        print(f"[{hit['_score']:.4f}] CIS {s['cis']} — {s['section_title']} > {s['sub_header']}")
        print(f"  {s['text'][:200]}")
        print()


if __name__ == "__main__":
    main()
