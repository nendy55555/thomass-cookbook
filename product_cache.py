"""
Product Cache — Ralphs ingredient → product mapping.

Eliminates re-searching for known ingredients across sessions.
Cache location: product_cache.json (same folder as this file)
Cache TTL:      30 days (prices change, sales rotate)

Schema per entry:
    {
      "ralphs_id":    "0001111010717",   # UPC from product URL (enables direct add)
      "display_name": "Kroger Plain Nonfat Greek Yogurt 32oz",
      "search_term":  "plain greek yogurt",
      "last_price":   4.59,
      "price_unit":   "each",            # each | lb | oz
      "store_brand":  true,
      "last_seen":    "2026-03-31T10:00:00"
    }

Cache key: normalized ingredient name from the pipeline (e.g. "greek yogurt", "chicken thigh")
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

CACHE_PATH = Path(__file__).parent / "product_cache.json"
CACHE_TTL_DAYS = 30

# ---------------------------------------------------------------------------
# Normalization — fuzzy key matching to boost cache hit rate
# ---------------------------------------------------------------------------

# Words that describe prep/form but don't change what product to buy.
# Stripping these lets "feta cheese crumbled" → "feta cheese" and
# "sweet paprika" → "paprika" hit existing cache entries.
_STRIP_WORDS = {
    # Prep/cut style (doesn't change the product)
    "crumbled", "chopped", "diced", "minced", "sliced", "shredded",
    "grated", "crushed", "halved", "quartered", "trimmed", "peeled",
    "seeded", "toasted",
    # Variety qualifiers where the base product is identical at Ralphs
    "sweet",   # sweet paprika → paprika
    "plain",   # plain greek yogurt → greek yogurt (search override handles this)
    # Delivery form that maps to same product (avocado oil spray → avocado oil)
    "spray",
}

# True synonyms — different words for the same product.
_SYNONYM_MAP = {
    "mayo":                    "mayonnaise",
    "scallion":                "green onion",
    "scallions":               "green onion",
    "spring onion":            "green onion",
    "extra virgin olive oil":  "olive oil",
    "ev olive oil":            "olive oil",
    "red pepper flakes":       "red pepper flake",
    "crushed red pepper":      "red pepper flake",
    "chilli flakes":           "red pepper flake",
    "heavy cream":             "heavy whipping cream",
    "heavy whipping cream":    "heavy cream",
    "parmesan":                "parmesan cheese",
    "parmigiano":              "parmesan cheese",
    "romano":                  "romano cheese",
    "half and half":           "half & half",
    "passata":                 "tomato puree",
    "double cream":            "heavy cream",
    "creme fraiche":           "sour cream",
    "aubergine":               "eggplant",
    "courgette":               "zucchini",
    "capsicum":                "bell pepper",
    "coriander":               "cilantro",   # leaf form (not seed/spice)
    "rocket":                  "arugula",
}


def normalize_key(key: str) -> str:
    """
    Normalize an ingredient key for fuzzy cache lookup.

    Steps:
      1. Lowercase + strip whitespace
      2. Apply synonym map (whole-key match)
      3. Strip form/prep words from individual tokens
      4. Collapse extra whitespace

    Examples:
      "sweet paprika"       → "paprika"
      "feta cheese crumbled"→ "feta cheese"
      "mayo"                → "mayonnaise"
      "avocado oil spray"   → "avocado oil"
    """
    k = key.lower().strip()

    # Whole-key synonym (highest priority)
    if k in _SYNONYM_MAP:
        return _SYNONYM_MAP[k]

    # Token-level prep word stripping
    tokens = [t for t in k.split() if t not in _STRIP_WORDS]
    k = " ".join(tokens).strip()

    # Try synonym again after stripping (e.g. "fresh coriander" → "coriander" → "cilantro")
    return _SYNONYM_MAP.get(k, k)


# ---------------------------------------------------------------------------
# Core I/O
# ---------------------------------------------------------------------------

def load_cache() -> dict:
    """Load cache from disk. Returns empty dict if file doesn't exist."""
    if CACHE_PATH.exists():
        try:
            return json.loads(CACHE_PATH.read_text())
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def save_cache(cache: dict) -> None:
    """Persist cache to disk."""
    CACHE_PATH.write_text(json.dumps(cache, indent=2, sort_keys=True))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _is_fresh(entry: dict) -> bool:
    """Return True if a cache entry is within TTL."""
    try:
        last_seen = datetime.fromisoformat(entry["last_seen"])
        return datetime.now() - last_seen <= timedelta(days=CACHE_TTL_DAYS)
    except (KeyError, ValueError):
        return False


def get(ingredient_key: str, cache: dict = None) -> Optional[dict]:
    """
    Return cached product for a normalized ingredient name, or None if
    missing/stale.

    Lookup order (stops at first hit):
      1. Exact key match
      2. Synonym-resolved key
      3. Prep-word-stripped key
      4. Normalized key matched against normalized versions of all cache keys
         (handles avocado oil → avocado oil spray, etc.)

    Args:
        ingredient_key: Normalized name from pipeline (e.g. "greek yogurt")
        cache: Pre-loaded cache dict (pass to avoid repeated disk reads)
    """
    if cache is None:
        cache = load_cache()

    raw = ingredient_key.lower().strip()
    norm = normalize_key(raw)

    # Pass 1: exact match
    entry = cache.get(raw)
    if entry and _is_fresh(entry):
        return entry

    # Pass 2: synonym / prep-stripped key
    if norm != raw:
        entry = cache.get(norm)
        if entry and _is_fresh(entry):
            return entry

    # Pass 3: normalize all cache keys and see if any match
    # (handles e.g. lookup "avocado oil" matching cached "avocado oil spray")
    for cache_key, cache_entry in cache.items():
        if normalize_key(cache_key) == norm and _is_fresh(cache_entry):
            return cache_entry

    return None


def put(ingredient_key: str, product: dict, cache: dict = None) -> dict:
    """
    Save a product selection and persist to disk.

    Args:
        ingredient_key: Normalized ingredient name
        product: Dict with keys: ralphs_id, display_name, search_term,
                 last_price, price_unit, store_brand
        cache: Pre-loaded cache dict (will be mutated and saved)

    Returns:
        Updated cache dict
    """
    if cache is None:
        cache = load_cache()

    cache[ingredient_key.lower().strip()] = {
        "ralphs_id":    product.get("ralphs_id", ""),
        "display_name": product.get("display_name", ""),
        "search_term":  product.get("search_term", ingredient_key),
        "last_price":   product.get("last_price", 0.0),
        "price_unit":   product.get("price_unit", "each"),
        "store_brand":  product.get("store_brand", False),
        "last_seen":    datetime.now().isoformat(),
    }
    save_cache(cache)
    return cache


def invalidate(ingredient_key: str) -> None:
    """Force a cache miss for one ingredient (e.g. if product was out of stock)."""
    cache = load_cache()
    cache.pop(ingredient_key.lower().strip(), None)
    save_cache(cache)


def get_product_url(entry: dict) -> Optional[str]:
    """Return direct Ralphs pickup URL for a cached entry, or None."""
    rid = entry.get("ralphs_id", "").strip()
    if rid:
        return f"https://www.ralphs.com/p/{rid}?fulfillment=PICKUP"
    return None


# ---------------------------------------------------------------------------
# Batch helpers for grocery_runner integration
# ---------------------------------------------------------------------------

def annotate_shopping_items(items: list[dict]) -> list[dict]:
    """
    Add cache hit/miss info to each shopping item from the pipeline.

    Adds keys to each item:
        cached (bool): True if a fresh cache entry exists
        cache_entry (dict|None): The cached product, or None

    Args:
        items: List of shopping items from pipeline_result["shopping_items"]

    Returns:
        Same list with cache fields added (mutates in place)
    """
    cache = load_cache()
    for item in items:
        key = item.get("item_normalized") or item.get("item", "")
        entry = get(key, cache)
        item["cached"] = entry is not None
        item["cache_entry"] = entry
    return items


def cache_summary(items: list[dict]) -> dict:
    """
    Return hit/miss counts for a list of annotated shopping items.

    Args:
        items: Items already passed through annotate_shopping_items()

    Returns:
        {"hits": int, "misses": int, "hit_rate": float, "miss_items": [str]}
    """
    hits = [i for i in items if i.get("cached")]
    misses = [i for i in items if not i.get("cached")]
    total = len(items)
    return {
        "hits": len(hits),
        "misses": len(misses),
        "hit_rate": round(len(hits) / total, 2) if total else 0.0,
        "miss_items": [i["item"] for i in misses],
    }


# ---------------------------------------------------------------------------
# CLI utility
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    cache = load_cache()

    if "--list" in sys.argv or len(sys.argv) == 1:
        if not cache:
            print("Cache is empty.")
        else:
            print(f"{'Ingredient':<35} {'Product':<40} {'Price':>7}  {'Last Seen'}")
            print("-" * 100)
            for key in sorted(cache):
                e = cache[key]
                age = (datetime.now() - datetime.fromisoformat(e["last_seen"])).days
                stale = " [STALE]" if age > CACHE_TTL_DAYS else ""
                print(f"{key:<35} {e['display_name']:<40} ${e['last_price']:>6.2f}  {e['last_seen'][:10]}{stale}")
            print(f"\n{len(cache)} entries")

    elif "--invalidate" in sys.argv:
        key = sys.argv[sys.argv.index("--invalidate") + 1]
        invalidate(key)
        print(f"Invalidated: {key}")

    elif "--get" in sys.argv:
        key = sys.argv[sys.argv.index("--get") + 1]
        entry = get(key)
        print(json.dumps(entry, indent=2) if entry else f"No cache entry for '{key}'")

    elif "--normalize" in sys.argv:
        key = sys.argv[sys.argv.index("--normalize") + 1]
        print(f"Input:      '{key}'")
        print(f"Normalized: '{normalize_key(key)}'")
        entry = get(key)
        print(f"Cache hit:  {entry['display_name'] if entry else 'MISS'}")
