from __future__ import annotations

# Category weights for relevance scoring
CATEGORY_WEIGHTS: dict[str, float] = {
    "core": 0.30,
    "protocol": 0.25,
    "framework": 0.20,
    "concept": 0.15,
}


def score_post_relevance(
    content: str,
    keywords: list[dict],
) -> tuple[float, list[str]]:
    """Score how relevant a post is to our target topics.

    Args:
        content: The post text/title/description.
        keywords: List of keyword dicts with 'term' and 'category' fields.

    Returns:
        (score 0.0-1.0, list of matched keyword terms)
    """
    if not content:
        return 0.0, []

    content_lower = content.lower()
    matched: list[str] = []
    score = 0.0

    for kw in keywords:
        term = kw["term"]
        category = kw.get("category", "concept")

        if term.lower() in content_lower:
            matched.append(term)
            score += CATEGORY_WEIGHTS.get(category, 0.10)

    # Bonus for multiple keyword matches — indicates deep engagement
    if len(matched) >= 3:
        score += 0.20
    elif len(matched) >= 2:
        score += 0.10

    return min(score, 1.0), matched
