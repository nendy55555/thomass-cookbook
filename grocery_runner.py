"""
Grocery Workflow Runner

Orchestrates: recipe input → pipeline → grocery list → cookbook save → Ralphs search terms

Usage (as module — called by Claude during workflow):
    from grocery_runner import GrocerySession

    session = GrocerySession()
    session.add_recipe("Lemon Chicken", ingredient_text, steps_text="...", source_url="...")
    session.run()                    # generates grocery list
    session.save_to_cookbook()        # POSTs recipes to cookbook API
    session.get_ralphs_search_terms()  # returns optimized search terms for Ralphs

Usage (CLI for testing):
    python grocery_runner.py
"""

import json
import re
import urllib.request
import urllib.error
from datetime import date
from pathlib import Path
from typing import Optional

CONFIG_PATH = Path(__file__).parent / "config.json"


def load_config() -> dict:
    """Load config.json from the project folder. Returns empty dict if missing."""
    if CONFIG_PATH.exists():
        try:
            return json.loads(CONFIG_PATH.read_text())
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def save_config(config: dict) -> None:
    CONFIG_PATH.write_text(json.dumps(config, indent=2))

from grocery_pipeline import (
    run_pipeline,
    build_cookbook_payload,
    parse_ingredients,
    parse_instagram_caption,
    normalize,
)
import product_cache as cache_module


COOKBOOK_API = "http://localhost:8742/api"


class GrocerySession:
    """Manages a single grocery shopping session (one or more recipes → one cart)."""

    def __init__(self, pantry_staples: set = None):
        self.recipes: dict[str, dict] = {}  # name → {ingredients, steps, source_url, ...}
        self.pipeline_result: Optional[dict] = None
        self.cookbook_ids: list[int] = []

        # Auto-load pantry staples from pantry.json; caller can override or extend
        file_staples = self._load_pantry_file()
        if pantry_staples is not None:
            self.pantry_staples = file_staples | set(pantry_staples)
        else:
            self.pantry_staples = file_staples

    @staticmethod
    def _load_pantry_file() -> set:
        """Load staples from pantry.json in the project folder. Returns empty set if missing."""
        pantry_path = Path(__file__).parent / "pantry.json"
        if pantry_path.exists():
            try:
                data = json.loads(pantry_path.read_text())
                return {item.lower().strip() for item in data.get("staples", [])}
            except (json.JSONDecodeError, OSError):
                pass
        return set()

    # ----- Recipe intake -----

    def add_recipe(self, name: str, ingredient_text: str, steps_text: str = "",
                   source_url: str = "", servings: int = 1, cuisine: str = "",
                   meal_type: str = ""):
        """Add a recipe to this session."""
        self.recipes[name] = {
            "ingredient_text": ingredient_text,
            "steps_text": steps_text,
            "source_url": source_url,
            "servings": servings,
            "cuisine": cuisine,
            "meal_type": meal_type,
        }
        # Reset pipeline results since input changed
        self.pipeline_result = None

    def add_recipe_from_url(self, url: str, caption_text: str,
                            servings: int = 1, cuisine: str = "",
                            meal_type: str = "") -> dict:
        """Add a recipe from an Instagram URL (or any URL with extracted caption text).

        The caller is responsible for fetching the caption text (via Claude in Chrome
        or other means). This method parses the caption into structured recipe data
        and adds it to the session.

        Args:
            url: Source URL (Instagram post URL, blog URL, etc.)
            caption_text: Raw caption/page text extracted from the URL
            servings: Number of servings (default 1)
            cuisine: Detected cuisine (optional, will be inferred if blank)
            meal_type: Detected meal type (optional)

        Returns:
            dict with keys: name, ingredients_found, steps_found, source_url
        """
        title, ingredient_text, steps_text = parse_instagram_caption(caption_text)

        if not ingredient_text.strip():
            return {
                "name": title,
                "ingredients_found": 0,
                "steps_found": 0,
                "source_url": url,
                "error": "No ingredients detected in caption. Try pasting the ingredient list manually.",
            }

        self.add_recipe(
            name=title,
            ingredient_text=ingredient_text,
            steps_text=steps_text,
            source_url=url,
            servings=servings,
            cuisine=cuisine,
            meal_type=meal_type,
        )

        ingredient_count = len([l for l in ingredient_text.strip().splitlines() if l.strip()])
        step_count = len([l for l in steps_text.strip().splitlines() if l.strip()])

        return {
            "name": title,
            "ingredients_found": ingredient_count,
            "steps_found": step_count,
            "source_url": url,
        }

    def remove_recipe(self, name: str):
        """Remove a recipe from this session."""
        self.recipes.pop(name, None)
        self.pipeline_result = None

    # ----- Pipeline -----

    def run(self) -> dict:
        """Run the grocery pipeline on all added recipes. Returns pipeline result."""
        recipe_texts = {
            name: data["ingredient_text"]
            for name, data in self.recipes.items()
        }
        self.pipeline_result = run_pipeline(
            recipe_texts,
            pantry_staples=self.pantry_staples,
            list_date=date.today().strftime("%B %d, %Y"),
        )
        return self.pipeline_result

    def get_grocery_list(self) -> str:
        """Get formatted grocery list markdown. Runs pipeline if needed."""
        if not self.pipeline_result:
            self.run()
        return self.pipeline_result["grocery_list_md"]

    def get_shopping_items(self) -> list[dict]:
        """Get flat list of items to buy."""
        if not self.pipeline_result:
            self.run()
        return self.pipeline_result["shopping_items"]

    # ----- Cookbook integration -----

    def save_to_cookbook(self, api_base: str = None) -> list[dict]:
        """POST each recipe to the cookbook API. Returns list of created recipes."""
        base = api_base or COOKBOOK_API
        results = []

        for name, data in self.recipes.items():
            payload = build_cookbook_payload(
                recipe_name=name,
                ingredient_text=data["ingredient_text"],
                steps_text=data["steps_text"],
                source_url=data["source_url"],
                servings=data["servings"],
                cuisine=data["cuisine"],
                meal_type=data["meal_type"],
            )

            try:
                req = urllib.request.Request(
                    f"{base}/recipes",
                    data=json.dumps(payload).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=10) as resp:
                    result = json.loads(resp.read().decode())
                    self.cookbook_ids.append(result.get("id"))
                    results.append({"recipe": name, "id": result.get("id"), "status": "saved"})
            except urllib.error.URLError as e:
                results.append({"recipe": name, "id": None, "status": f"error: {e}"})
            except Exception as e:
                results.append({"recipe": name, "id": None, "status": f"error: {e}"})

        return results

    # ----- Ralphs search term optimization -----

    def get_ralphs_search_terms(self) -> list[dict]:
        """Generate optimized Ralphs search terms for each shopping item.

        Now cache-aware: each term includes 'cached' (bool) and 'cache_entry'
        (dict|None). Cached items have a direct product URL and don't need a
        browser search — only misses require full search + selection.

        Returns list of dicts with keys:
            item, quantity, unit, search_term, category,
            cached, cache_entry, product_url
        """
        if not self.pipeline_result:
            self.run()

        # Common search term optimizations (fallback for cache misses)
        SEARCH_OVERRIDES = {
            "boneless skinless chicken thighs": "boneless skinless chicken thighs",
            "boneless skinless chicken breasts": "boneless skinless chicken breast",
            "chicken thigh": "chicken thighs",
            "chicken breast": "chicken breasts",
            "ground beef": "ground beef",
            "baby potatoes": "baby potatoes",
            "green onion": "green onions",
            "scallion": "green onions",
            "bell pepper": "bell pepper",
            "greek yogurt": "plain greek yogurt",
            "parmesan": "parmesan cheese",
            "parmigiano": "parmesan cheese",
            "coconut milk": "canned coconut milk",
            "jasmine rice": "jasmine rice",
            "miso paste": "white miso paste",
            "cilantro": "cilantro bunch",
            "fresh basil": "fresh basil",
            "fresh rosemary": "fresh rosemary",
            "fresh thyme": "fresh thyme",
        }

        # Load cache once for all items
        product_cache = cache_module.load_cache()

        terms = []
        for item in self.pipeline_result["shopping_items"]:
            name = item["item"]
            name_lower = name.lower()
            norm = item["item_normalized"]

            # Cache lookup (try normalized name first, then raw lowercase)
            entry = cache_module.get(norm, product_cache) or cache_module.get(name_lower, product_cache)
            cached = entry is not None

            # Search term: prefer cached search_term, then overrides, then name
            if cached:
                search = entry["search_term"]
            else:
                search = SEARCH_OVERRIDES.get(name_lower) or SEARCH_OVERRIDES.get(norm) or name_lower
                search = re.sub(r"\b(kroger|ralphs|simple truth|private selection)\b", "", search, flags=re.IGNORECASE).strip()

            terms.append({
                "item": name,
                "quantity": item["quantity"],
                "unit": item["unit"],
                "search_term": search,
                "category": item.get("category", ""),
                "cached": cached,
                "cache_entry": entry,
                "product_url": cache_module.get_product_url(entry) if cached else None,
            })

        return terms

    def get_cart_plan(self) -> dict:
        """
        Split shopping items into two groups for efficient cart filling:

        - 'direct':  Cached items with a known Ralphs product URL.
                     Add to cart via direct navigation — no search needed.
        - 'search':  New items that need a Ralphs search + product selection.
                     Claude only handles these (usually 0–3 per session).

        Returns:
            {
              "direct":  [{item, quantity, unit, product_url, display_name, last_price}],
              "search":  [{item, quantity, unit, search_term, category}],
              "summary": {"total": int, "direct": int, "search": int, "cache_hit_rate": float}
            }
        """
        terms = self.get_ralphs_search_terms()

        direct = []
        search = []

        for t in terms:
            if t["cached"] and t["product_url"]:
                entry = t["cache_entry"]
                direct.append({
                    "item":         t["item"],
                    "quantity":     t["quantity"],
                    "unit":         t["unit"],
                    "gtin":         entry["ralphs_id"],   # needed for batch cart API
                    "product_url":  t["product_url"],
                    "display_name": entry["display_name"],
                    "last_price":   entry["last_price"],
                    "price_unit":   entry["price_unit"],
                })
            else:
                search.append({
                    "item":        t["item"],
                    "quantity":    t["quantity"],
                    "unit":        t["unit"],
                    "search_term": t["search_term"],
                    "category":    t["category"],
                })

        config = load_config()
        cart_id = config.get("ralphs", {}).get("cart_id", "")

        total = len(terms)
        return {
            "direct":  direct,
            "search":  search,
            "cart_id": cart_id,   # pre-filled from config — skip interceptor dance
            "summary": {
                "total":          total,
                "direct":         len(direct),
                "search":         len(search),
                "cache_hit_rate": round(len(direct) / total, 2) if total else 0.0,
                "cart_id_known":  bool(cart_id),
            },
        }

    def save_cart_id(self, cart_id: str) -> None:
        """Persist a newly discovered Ralphs cart ID to config.json."""
        config = load_config()
        config.setdefault("ralphs", {})["cart_id"] = cart_id
        save_config(config)

    def save_product_to_cache(self, ingredient_key: str, product: dict) -> None:
        """
        Persist a product selection to the cache after a successful Ralphs search.

        Call this after each new item is found and added to cart, so future
        sessions won't need to search for it again.

        Args:
            ingredient_key: Normalized ingredient name (e.g. "greek yogurt")
            product: Dict with ralphs_id, display_name, search_term,
                     last_price, price_unit, store_brand
        """
        cache_module.put(ingredient_key, product)

    # ----- Summary -----

    def summary(self) -> str:
        """Return a text summary of the current session state."""
        lines = [f"Grocery Session — {date.today().strftime('%B %d, %Y')}"]
        lines.append(f"Recipes: {len(self.recipes)}")
        for name in self.recipes:
            lines.append(f"  - {name}")

        if self.pipeline_result:
            lines.append(f"Shopping items: {self.pipeline_result['item_count']}")
            lines.append(f"Pantry items skipped: {len(self.pipeline_result['pantry_items'])}")

        if self.cookbook_ids:
            lines.append(f"Saved to cookbook: {len(self.cookbook_ids)} recipes")

        return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    session = GrocerySession()

    session.add_recipe(
        "Lemon Herb Chicken",
        """
2 lbs boneless skinless chicken thighs
3 tablespoons olive oil
4 cloves garlic, minced
2 lemons, juiced
1 tablespoon fresh rosemary, chopped
1 tablespoon fresh thyme, chopped
1 teaspoon salt
1/2 teaspoon black pepper
1 lb baby potatoes, halved
1 bunch asparagus, trimmed
        """,
        steps_text="""
1. Preheat oven to 425°F
2. Mix olive oil, garlic, lemon juice, rosemary, and thyme
3. Toss chicken and potatoes in the marinade
4. Arrange on a sheet pan with asparagus
5. Bake 25-30 minutes until chicken reaches 165°F
        """,
        cuisine="American",
        meal_type="Dinner",
        servings=4,
    )

    session.add_recipe(
        "Spicy Miso Ramen",
        """
4 cups chicken broth
2 tablespoons white miso paste
1 tablespoon soy sauce
1 tablespoon sesame oil
2 teaspoons chili garlic sauce
2 packs ramen noodles
2 soft-boiled eggs
4 oz mushrooms, sliced
2 green onions, sliced
1 cup fresh spinach
1 tablespoon butter
        """,
        steps_text="""
1. Heat broth in a large pot
2. Whisk in miso paste, soy sauce, sesame oil, and chili garlic sauce
3. Cook ramen noodles according to package
4. Sauté mushrooms in butter until golden
5. Divide noodles into bowls, ladle broth over
6. Top with eggs, mushrooms, spinach, and green onions
        """,
        cuisine="Japanese",
        meal_type="Dinner",
        servings=2,
    )

    # Run pipeline
    result = session.run()
    print(session.get_grocery_list())
    print()

    # Show Ralphs search terms
    print("=" * 60)
    print("RALPHS SEARCH TERMS")
    print("=" * 60)
    for term in session.get_ralphs_search_terms():
        print(f"  {term['item']:35s} → search: \"{term['search_term']}\"")

    print()
    print(session.summary())

    # Cookbook save (only if server is running)
    print("\n--- Attempting cookbook save ---")
    results = session.save_to_cookbook()
    for r in results:
        print(f"  {r['recipe']}: {r['status']}" + (f" (id={r['id']})" if r['id'] else ""))
