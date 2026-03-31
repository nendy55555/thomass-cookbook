"""
Personal Cookbook — FastAPI + SQLite backend
Run: python server.py
Opens at http://localhost:8742
"""

import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

import httpx

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
import uvicorn

# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------

DB_DIR = Path(os.environ.get("COOKBOOK_DB_DIR", Path.home() / ".cookbook"))
DB_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DB_DIR / "cookbook.db"

def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init_db():
    conn = get_db()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS recipes (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        title           TEXT    NOT NULL,
        description     TEXT    DEFAULT '',
        source_url      TEXT    DEFAULT '',
        image_url       TEXT    DEFAULT '',
        prep_time_min   INTEGER DEFAULT 0,
        cook_time_min   INTEGER DEFAULT 0,
        servings        INTEGER DEFAULT 1,
        cuisine         TEXT    DEFAULT '',
        meal_type       TEXT    DEFAULT '',
        dietary_tags    TEXT    DEFAULT '[]',
        rating_taste    INTEGER DEFAULT 0 CHECK(rating_taste BETWEEN 0 AND 5),
        rating_ease     INTEGER DEFAULT 0 CHECK(rating_ease BETWEEN 0 AND 5),
        rating_health   INTEGER DEFAULT 0 CHECK(rating_health BETWEEN 0 AND 5),
        cook_count      INTEGER DEFAULT 0,
        date_added      TEXT    NOT NULL,
        date_last_cooked TEXT   DEFAULT '',
        notes           TEXT    DEFAULT ''
    );

    CREATE TABLE IF NOT EXISTS ingredients (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        recipe_id   INTEGER NOT NULL REFERENCES recipes(id) ON DELETE CASCADE,
        group_name  TEXT    DEFAULT '',
        name        TEXT    NOT NULL,
        quantity    TEXT    DEFAULT '',
        unit        TEXT    DEFAULT '',
        sort_order  INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS steps (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        recipe_id   INTEGER NOT NULL REFERENCES recipes(id) ON DELETE CASCADE,
        step_number INTEGER NOT NULL,
        instruction TEXT    NOT NULL,
        timer_min   INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS grocery_list (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        recipe_id   INTEGER REFERENCES recipes(id) ON DELETE SET NULL,
        recipe_title TEXT   DEFAULT '',
        name        TEXT    NOT NULL,
        quantity    TEXT    DEFAULT '',
        unit        TEXT    DEFAULT '',
        checked     INTEGER DEFAULT 0,
        date_added  TEXT    NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_ingredients_recipe ON ingredients(recipe_id);
    CREATE INDEX IF NOT EXISTS idx_steps_recipe ON steps(recipe_id);
    CREATE INDEX IF NOT EXISTS idx_grocery_list_recipe ON grocery_list(recipe_id);
    """)
    # Migration: add rating_health column if missing (for existing DBs)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(recipes)").fetchall()}
    if "rating_health" not in cols:
        conn.execute("ALTER TABLE recipes ADD COLUMN rating_health INTEGER DEFAULT 0 CHECK(rating_health BETWEEN 0 AND 5)")
    conn.commit()
    conn.close()

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class IngredientIn(BaseModel):
    group_name: str = ""
    name: str
    quantity: str = ""
    unit: str = ""
    sort_order: int = 0

class StepIn(BaseModel):
    step_number: int
    instruction: str
    timer_min: int = 0

class RecipeIn(BaseModel):
    title: str
    description: str = ""
    source_url: str = ""
    image_url: str = ""
    prep_time_min: int = 0
    cook_time_min: int = 0
    servings: int = 1
    cuisine: str = ""
    meal_type: str = ""
    dietary_tags: list[str] = []
    rating_taste: int = 0
    rating_ease: int = 0
    rating_health: int = 0
    notes: str = ""
    ingredients: list[IngredientIn] = []
    steps: list[StepIn] = []

class RecipeUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    source_url: Optional[str] = None
    image_url: Optional[str] = None
    prep_time_min: Optional[int] = None
    cook_time_min: Optional[int] = None
    servings: Optional[int] = None
    cuisine: Optional[str] = None
    meal_type: Optional[str] = None
    dietary_tags: Optional[list[str]] = None
    rating_taste: Optional[int] = None
    rating_ease: Optional[int] = None
    rating_health: Optional[int] = None
    notes: Optional[str] = None
    ingredients: Optional[list[IngredientIn]] = None
    steps: Optional[list[StepIn]] = None


class GroceryItemIn(BaseModel):
    name: str
    quantity: str = ""
    unit: str = ""
    recipe_id: Optional[int] = None
    recipe_title: str = ""

class GroceryItemUpdate(BaseModel):
    checked: Optional[bool] = None
    name: Optional[str] = None
    quantity: Optional[str] = None
    unit: Optional[str] = None


class IdentifyRequest(BaseModel):
    ingredients: list[str] = []
    instructions: list[str] = []


class IdentifyResponse(BaseModel):
    title: str
    description: str
    image_url: str
    cuisine: str = ""
    meal_type: str = ""
    confidence: str = "medium"  # low / medium / high


# ---------------------------------------------------------------------------
# Recipe identification engine
# ---------------------------------------------------------------------------

THEMEALDB_SEARCH = "https://www.themealdb.com/api/json/v1/1/search.php"
THEMEALDB_FILTER = "https://www.themealdb.com/api/json/v1/1/filter.php"

# Maps MealDB categories to our meal_type values
CATEGORY_TO_MEAL_TYPE = {
    "breakfast": "Breakfast", "starter": "Side", "side": "Side",
    "dessert": "Dessert", "snack": "Snack",
}

# Cooking-method keywords that help identify dishes from instructions
DISH_SIGNAL_KEYWORDS = {
    "stir fry": "Stir Fry", "stir-fry": "Stir Fry",
    "marinate": None, "braise": None, "roast": None,
    "bake": None, "grill": None, "simmer": None,
    "sauté": None, "saute": None, "deep fry": None,
    "fold": None, "knead": None, "proof": None,
}


async def _search_themealdb_by_name(query: str) -> list[dict]:
    """Search TheMealDB by dish name."""
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            resp = await client.get(THEMEALDB_SEARCH, params={"s": query})
            data = resp.json()
            return data.get("meals") or []
        except Exception:
            return []


async def _scrape_og_image(url: str) -> str:
    """Scrape the og:image meta tag from any URL. Returns '' on failure."""
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True,
                                     headers={"User-Agent": "Mozilla/5.0"}) as client:
            resp = await client.get(url)
            html = resp.text
        import re
        m = re.search(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\'](https?://[^"\']+)', html)
        if not m:
            m = re.search(r'<meta[^>]+content=["\'](https?://[^"\']+)["\'][^>]+property=["\']og:image["\']', html)
        return m.group(1) if m else ""
    except Exception:
        return ""


async def _fetch_image_by_title(title: str) -> str:
    """
    Find a food photo by recipe title.
    1. TheMealDB (free, no key)
    2. Scrape og:image from top recipe site search result
    """
    # 1. TheMealDB
    meals = await _search_themealdb_by_name(title)
    if meals:
        img = meals[0].get("strMealThumb", "")
        if img:
            return img

    # 2. Search trusted recipe sites and scrape og:image from first result
    RECIPE_SITES = [
        "site:allrecipes.com", "site:simplyrecipes.com",
        "site:foodnetwork.com", "site:bonappetit.com",
        "site:seriouseats.com", "site:epicurious.com",
    ]
    query = f"{title} recipe " + RECIPE_SITES[0]
    search_url = f"https://www.google.com/search?q={query.replace(' ', '+')}"
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True,
                                     headers={"User-Agent": "Mozilla/5.0"}) as client:
            resp = await client.get(search_url)
            html = resp.text
        import re
        # Extract first non-Google URL from search results
        links = re.findall(r'href="(https?://(?!google)[^"&]+)"', html)
        for link in links[:5]:
            if any(site.replace("site:", "") in link for site in RECIPE_SITES):
                img = await _scrape_og_image(link)
                if img:
                    return img
    except Exception:
        pass

    return ""


async def _resolve_image(source_url: str, title: str) -> str:
    """
    Full image resolution pipeline:
    1. Scrape og:image from source_url (if provided)
    2. Search by title (TheMealDB → recipe site)
    Always returns a string; empty string only if all fail.
    """
    if source_url:
        img = await _scrape_og_image(source_url)
        if img:
            return img
    return await _fetch_image_by_title(title)


async def _backfill_missing_images():
    """On startup: find any recipes with no image and resolve one."""
    conn = get_db()
    rows = conn.execute(
        "SELECT id, title, source_url FROM recipes WHERE image_url IS NULL OR image_url = ''"
    ).fetchall()
    conn.close()
    for row in rows:
        img = await _resolve_image(row["source_url"] or "", row["title"])
        if img:
            conn2 = get_db()
            conn2.execute("UPDATE recipes SET image_url = ? WHERE id = ?", (img, row["id"]))
            conn2.commit()
            conn2.close()


async def _search_themealdb_by_ingredient(ingredient: str) -> list[dict]:
    """Filter TheMealDB meals containing a specific ingredient."""
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            resp = await client.get(THEMEALDB_FILTER, params={"i": ingredient})
            data = resp.json()
            return data.get("meals") or []
        except Exception:
            return []


def _extract_dish_guess_from_text(ingredients: list[str], instructions: list[str]) -> str:
    """Build a best-guess dish name from ingredient + instruction analysis."""
    all_text = " ".join(ingredients + instructions).lower()

    # Protein detection
    proteins = []
    protein_map = {
        "chicken": "Chicken", "beef": "Beef", "pork": "Pork",
        "shrimp": "Shrimp", "salmon": "Salmon", "fish": "Fish",
        "tofu": "Tofu", "lamb": "Lamb", "turkey": "Turkey",
        "sausage": "Sausage", "tuna": "Tuna", "cod": "Cod",
        "tilapia": "Tilapia", "duck": "Duck", "crab": "Crab",
        "lobster": "Lobster", "scallop": "Scallops",
    }
    for keyword, label in protein_map.items():
        if keyword in all_text:
            proteins.append(label)

    # Dish-type detection — ordered by specificity (specific dishes first, bases last)
    dish_types = []
    # High-specificity patterns (these ARE the dish)
    specific_patterns = {
        "stir fry": "Stir Fry", "stir-fry": "Stir Fry",
        "taco": "Tacos", "tortilla": "Tacos",
        "burrito": "Burrito", "curry": "Curry", "pizza": "Pizza",
        "risotto": "Risotto", "ramen": "Ramen", "pho": "Pho",
        "sushi": "Sushi", "quiche": "Quiche", "frittata": "Frittata",
        "casserole": "Casserole", "gratin": "Gratin", "chili": "Chili",
        "pancake": "Pancakes", "waffle": "Waffles",
        "omelet": "Omelette", "omelette": "Omelette",
        "smoothie": "Smoothie",
        "cookie": "Cookies", "brownie": "Brownies",
        "muffin": "Muffins", "cake": "Cake",
        "soup": "Soup", "stew": "Stew",
        "salad": "Salad", "sandwich": "Sandwich", "wrap": "Wrap",
        "pasta": "Pasta", "noodle": "Noodles",
        "pie": "Pie", "bread": "Bread",
    }
    # Low-specificity patterns (bases/sides, used only as fallback)
    base_patterns = {"rice": "Rice", "bowl": "Bowl"}

    for keyword, label in specific_patterns.items():
        if keyword in all_text and label not in dish_types:
            dish_types.append(label)
    if not dish_types:
        for keyword, label in base_patterns.items():
            if keyword in all_text and label not in dish_types:
                dish_types.append(label)

    # Build guess: protein + dish type
    if dish_types:
        main_dish = dish_types[0]
        if proteins and main_dish not in ("Cookies", "Brownies", "Cake", "Muffins", "Smoothie"):
            return f"{proteins[0]} {main_dish}"
        return main_dish
    elif proteins:
        # Check cooking method for context
        if "roast" in all_text or "oven" in all_text:
            return f"Roasted {proteins[0]}"
        if "grill" in all_text:
            return f"Grilled {proteins[0]}"
        if "bake" in all_text:
            return f"Baked {proteins[0]}"
        return f"{proteins[0]} Dish"
    return ""


def _guess_cuisine(ingredients: list[str], instructions: list[str]) -> str:
    """Guess cuisine from ingredient/instruction signals."""
    all_text = " ".join(ingredients + instructions).lower()
    signals = {
        "Italian": ["parmesan", "mozzarella", "basil", "oregano", "pasta", "risotto", "prosciutto", "marinara"],
        "Mexican": ["cumin", "jalapeño", "jalapeno", "cilantro", "tortilla", "taco", "salsa", "chipotle", "burrito"],
        "Asian": ["soy sauce", "ginger", "sesame", "rice vinegar", "fish sauce", "wok", "miso", "sriracha"],
        "Indian": ["turmeric", "garam masala", "curry", "cumin", "coriander", "naan", "ghee", "cardamom"],
        "Mediterranean": ["olive oil", "feta", "hummus", "tahini", "pita", "za'atar", "zaatar"],
        "Thai": ["coconut milk", "lemongrass", "thai basil", "fish sauce", "galangal", "pad thai"],
        "Japanese": ["mirin", "dashi", "nori", "wasabi", "miso", "sushi", "ramen", "teriyaki"],
        "French": ["gruyère", "gruyere", "beurre", "crème", "creme", "dijon", "tarragon", "gratin"],
        "Korean": ["gochujang", "kimchi", "sesame oil", "korean", "bulgogi", "bibimbap"],
        "American": ["bbq", "barbecue", "ranch", "cheddar", "burger", "cornbread"],
    }
    scores = {}
    for cuisine, keywords in signals.items():
        score = sum(1 for kw in keywords if kw in all_text)
        if score > 0:
            scores[cuisine] = score
    if scores:
        return max(scores, key=scores.get)
    return ""


def _normalize_meal_type(raw: str) -> str:
    """Normalize meal type strings to canonical form."""
    mapping = {
        "breakfast": "Breakfast", "brunch": "Breakfast",
        "lunch": "Lunch", "luncheon": "Lunch",
        "dinner": "Dinner", "supper": "Dinner", "entree": "Dinner", "main": "Dinner", "main course": "Dinner",
        "snack": "Snack", "appetizer": "Snack", "starter": "Snack",
        "dessert": "Dessert", "sweet": "Dessert",
        "side": "Side", "side dish": "Side", "sides": "Side",
    }
    return mapping.get(raw.lower().strip(), raw.strip().capitalize() if raw.strip() else "Dinner")


def _guess_meal_type(ingredients: list[str], instructions: list[str]) -> str:
    """Guess meal type using scored signals across all text."""
    all_text = " ".join(ingredients + instructions).lower()
    ing_text = " ".join(ingredients).lower()

    scores = {"Breakfast": 0, "Lunch": 0, "Dinner": 0, "Snack": 0, "Dessert": 0, "Side": 0}

    # ── Breakfast ──────────────────────────────────────────────────────────
    for kw in ["pancake", "waffle", "oatmeal", "cereal", "granola", "french toast",
                "breakfast", "brunch", "morning", "hash brown", "frittata",
                "omelette", "omelet", "scrambled", "poached egg", "fried egg",
                "egg bake", "quiche", "bagel", "english muffin"]:
        if kw in all_text:
            scores["Breakfast"] += 3

    # Egg-heavy = breakfast only when eggs are the star, not just a binder
    dominant_proteins = ["chicken", "beef", "turkey", "pork", "salmon", "shrimp",
                         "lamb", "tuna", "steak", "sausage", "meatball", "ground"]
    has_dominant_protein = any(p in ing_text for p in dominant_proteins)
    egg_count = ing_text.count("egg")
    if egg_count >= 1 and not has_dominant_protein:
        scores["Breakfast"] += 4   # eggs are the main protein
    elif egg_count >= 3:
        scores["Breakfast"] += 2   # lots of eggs even alongside other protein

    if "bacon" in ing_text and not has_dominant_protein:
        scores["Breakfast"] += 2

    # ── Dessert ────────────────────────────────────────────────────────────
    for kw in ["cake", "cookie", "brownie", "pie", "tart", "pudding", "mousse",
                "ice cream", "sorbet", "frosting", "icing", "dessert", "muffin",
                "cupcake", "cheesecake", "tiramisu", "custard", "gelato", "biscuit"]:
        if kw in all_text:
            scores["Dessert"] += 3

    # Sugar-heavy baking without a savoury protein = dessert
    baking_hits = sum(1 for w in ["flour", "baking powder", "baking soda", "butter", "sugar", "vanilla extract"] if w in ing_text)
    if baking_hits >= 3 and not has_dominant_protein:
        scores["Dessert"] += 3
    if "chocolate" in ing_text and not has_dominant_protein:
        scores["Dessert"] += 2

    # ── Snack ──────────────────────────────────────────────────────────────
    for kw in ["snack", "dip", "hummus", "chips", "crackers", "nuts", "trail mix",
                "appetizer", "bruschetta", "bite-size", "bite size", "poppers"]:
        if kw in all_text:
            scores["Snack"] += 3

    # ── Lunch ──────────────────────────────────────────────────────────────
    for kw in ["sandwich", "wrap", "panini", "lunch", "sub", "blt", "club sandwich", "quesadilla"]:
        if kw in all_text:
            scores["Lunch"] += 3
    if any(w in all_text for w in ["soup", "salad"]) and not has_dominant_protein:
        scores["Lunch"] += 1

    # ── Side ───────────────────────────────────────────────────────────────
    for kw in ["side dish", "for serving", "to serve", "accompaniment", "garnish",
                "dressing", "sauce", "condiment"]:
        if kw in all_text:
            scores["Side"] += 2
    # Single-veg or single-starch dishes with no protein lean side
    if not has_dominant_protein and egg_count == 0:
        scores["Side"] += 1

    # ── Dinner (default for protein-heavy dishes) ──────────────────────────
    if has_dominant_protein:
        scores["Dinner"] += 3
    for kw in ["roast", "braise", "stew", "pasta", "risotto", "stir fry", "grill",
                "dinner", "supper", "meatball", "burger", "curry", "casserole", "lasagne",
                "lasagna", "bolognese", "chili", "fajita", "taco"]:
        if kw in all_text:
            scores["Dinner"] += 1

    # Pick highest score; Dinner wins ties (it's the safe default)
    best = max(scores, key=lambda k: (scores[k], k == "Dinner"))
    return best if scores[best] > 0 else "Dinner"


async def identify_dish(ingredients: list[str], instructions: list[str]) -> IdentifyResponse:
    """
    Main identification pipeline:
    1. Guess dish name from ingredient/instruction analysis
    2. Search TheMealDB for a match → get title, image, description
    3. Fall back to heuristic name + cuisine/meal_type guess if no match
    """
    guess = _extract_dish_guess_from_text(ingredients, instructions)
    cuisine_guess = _guess_cuisine(ingredients, instructions)
    meal_type_guess = _guess_meal_type(ingredients, instructions)

    # Strategy: our heuristic provides the dish name + cuisine + meal type.
    # TheMealDB provides the image (its main value with only ~300 meals).
    # We prefer our guess for the title and use TheMealDB for visual match.

    image_url = ""
    db_confidence = "low"

    # Try exact name search first
    if guess:
        meals = await _search_themealdb_by_name(guess)
        if meals:
            best = _pick_best_meal(meals, ingredients)
            image_url = best.get("strMealThumb", "")
            # If the DB name closely matches our guess, it's high confidence
            if guess.lower() in best["strMeal"].lower() or best["strMeal"].lower() in guess.lower():
                db_confidence = "high"
                guess = best["strMeal"]  # Use the DB's cleaner name
            else:
                db_confidence = "medium"

    # If no image yet, try searching by individual terms from guess
    if not image_url and guess:
        for word in guess.split():
            if len(word) > 3:
                meals = await _search_themealdb_by_name(word)
                if meals:
                    best = _pick_best_meal(meals, ingredients)
                    image_url = best.get("strMealThumb", "")
                    if not db_confidence or db_confidence == "low":
                        db_confidence = "medium"
                    break

    # Last resort: search by distinctive ingredient for an image
    if not image_url:
        staples = {"salt", "pepper", "oil", "water", "butter", "sugar", "flour", "garlic", "onion", "olive oil"}
        distinctive = [i for i in ingredients if i.lower().strip() not in staples]
        for ing in distinctive[:2]:
            meals = await _search_themealdb_by_ingredient(ing.strip())
            if meals:
                full_meals = await _search_themealdb_by_name(meals[0].get("strMeal", ""))
                if full_meals:
                    image_url = full_meals[0].get("strMealThumb", "")
                    break

    if guess:
        return IdentifyResponse(
            title=guess,
            description=_build_brief(ingredients, instructions),
            image_url=image_url,
            cuisine=cuisine_guess,
            meal_type=meal_type_guess,
            confidence=db_confidence if image_url else "medium",
        )

    # Pure heuristic fallback
    title = guess or "Homemade Recipe"
    return IdentifyResponse(
        title=title,
        description=_build_brief(ingredients, instructions),
        image_url="",
        cuisine=cuisine_guess,
        meal_type=meal_type_guess,
        confidence="low" if not guess else "medium",
    )


def _pick_best_meal(meals: list[dict], ingredients: list[str]) -> dict:
    """Score TheMealDB results by ingredient overlap with our recipe."""
    if len(meals) == 1:
        return meals[0]
    ing_words = set()
    for i in ingredients:
        for word in i.lower().split():
            if len(word) > 2:
                ing_words.add(word)

    best, best_score = meals[0], 0
    for meal in meals:
        # TheMealDB stores ingredients as strIngredient1..strIngredient20
        meal_ings = ""
        for n in range(1, 21):
            val = meal.get(f"strIngredient{n}", "") or ""
            meal_ings += " " + val.lower()
        score = sum(1 for w in ing_words if w in meal_ings)
        if score > best_score:
            best_score = score
            best = meal
    return best


def _build_brief(ingredients: list[str], instructions: list[str]) -> str:
    """Build a concise 1-2 sentence description from ingredients and method."""
    protein_words = {"chicken", "beef", "pork", "shrimp", "salmon", "fish", "tofu", "lamb", "turkey"}
    key_ings = []
    for ing in ingredients[:8]:
        name = ing.lower().strip()
        if name and name not in {"salt", "pepper", "oil", "water", "butter"}:
            key_ings.append(name)

    method = ""
    all_inst = " ".join(instructions).lower()
    methods = ["roasted", "grilled", "baked", "sautéed", "sauteed", "braised",
               "simmered", "fried", "steamed", "poached"]
    for m in methods:
        if m in all_inst:
            method = m.replace("sauteed", "sautéed")
            break

    if key_ings:
        short_list = ", ".join(key_ings[:4])
        if method:
            return f"A {method} dish featuring {short_list}."
        return f"Made with {short_list}."
    return "A homemade recipe."

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="Personal Cookbook")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.on_event("startup")
async def startup():
    import asyncio
    init_db()
    # Backfill missing images in the background — won't block server startup
    asyncio.create_task(_backfill_missing_images())

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def row_to_dict(row):
    return dict(row) if row else None

def fetch_full_recipe(conn, recipe_id: int) -> dict:
    row = conn.execute("SELECT * FROM recipes WHERE id = ?", (recipe_id,)).fetchone()
    if not row:
        return None
    recipe = row_to_dict(row)
    recipe["dietary_tags"] = json.loads(recipe["dietary_tags"])
    recipe["ingredients"] = [
        row_to_dict(r) for r in
        conn.execute("SELECT * FROM ingredients WHERE recipe_id = ? ORDER BY sort_order, id", (recipe_id,)).fetchall()
    ]
    recipe["steps"] = [
        row_to_dict(r) for r in
        conn.execute("SELECT * FROM steps WHERE recipe_id = ? ORDER BY step_number", (recipe_id,)).fetchall()
    ]
    return recipe

# ---------------------------------------------------------------------------
# Routes — CRUD
# ---------------------------------------------------------------------------

@app.get("/api/recipes")
def list_recipes(
    search: str = "",
    cuisine: str = "",
    meal_type: str = "",
    min_rating_taste: int = 0,
    min_rating_ease: int = 0,
    min_rating_health: int = 0,
    max_time: int = 0,
    tag: str = "",
    sort: str = "date_added",
    order: str = "desc",
):
    conn = get_db()
    query = "SELECT * FROM recipes WHERE 1=1"
    params = []

    if search:
        query += " AND (title LIKE ? OR description LIKE ?)"
        params += [f"%{search}%", f"%{search}%"]
    if cuisine:
        query += " AND cuisine = ?"
        params.append(cuisine)
    if meal_type:
        query += " AND meal_type = ?"
        params.append(meal_type)
    if min_rating_taste:
        query += " AND rating_taste >= ?"
        params.append(min_rating_taste)
    if min_rating_ease:
        query += " AND rating_ease >= ?"
        params.append(min_rating_ease)
    if min_rating_health:
        query += " AND rating_health >= ?"
        params.append(min_rating_health)
    if max_time:
        query += " AND (prep_time_min + cook_time_min) <= ?"
        params.append(max_time)
    if tag:
        query += " AND dietary_tags LIKE ?"
        params.append(f'%"{tag}"%')

    allowed_sort = {"date_added", "title", "rating_taste", "rating_ease", "rating_health", "cook_count", "prep_time_min"}
    sort_col = sort if sort in allowed_sort else "date_added"
    order_dir = "ASC" if order.lower() == "asc" else "DESC"
    query += f" ORDER BY {sort_col} {order_dir}"

    rows = conn.execute(query, params).fetchall()
    recipes = []
    for r in rows:
        d = row_to_dict(r)
        d["dietary_tags"] = json.loads(d["dietary_tags"])
        d["ingredient_count"] = conn.execute(
            "SELECT COUNT(*) FROM ingredients WHERE recipe_id = ?", (d["id"],)
        ).fetchone()[0]
        recipes.append(d)
    conn.close()
    return {"recipes": recipes, "count": len(recipes)}


@app.get("/api/recipes/{recipe_id}")
def get_recipe(recipe_id: int):
    conn = get_db()
    recipe = fetch_full_recipe(conn, recipe_id)
    conn.close()
    if not recipe:
        raise HTTPException(404, "Recipe not found")
    return recipe


@app.post("/api/recipes")
async def create_recipe(data: RecipeIn):
    # Auto-fetch image if none supplied: try source URL first, then title search
    image_url = data.image_url or await _resolve_image(data.source_url, data.title)
    conn = get_db()
    now = datetime.now().isoformat()
    cur = conn.execute("""
        INSERT INTO recipes (title, description, source_url, image_url,
            prep_time_min, cook_time_min, servings, cuisine, meal_type,
            dietary_tags, rating_taste, rating_ease, rating_health, notes, date_added)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data.title, data.description, data.source_url, image_url,
        data.prep_time_min, data.cook_time_min, data.servings,
        data.cuisine, _normalize_meal_type(data.meal_type), json.dumps(data.dietary_tags),
        data.rating_taste, data.rating_ease, data.rating_health, data.notes, now
    ))
    recipe_id = cur.lastrowid

    for ing in data.ingredients:
        conn.execute("""
            INSERT INTO ingredients (recipe_id, group_name, name, quantity, unit, sort_order)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (recipe_id, ing.group_name, ing.name, ing.quantity, ing.unit, ing.sort_order))

    for step in data.steps:
        conn.execute("""
            INSERT INTO steps (recipe_id, step_number, instruction, timer_min)
            VALUES (?, ?, ?, ?)
        """, (recipe_id, step.step_number, step.instruction, step.timer_min))

    conn.commit()
    recipe = fetch_full_recipe(conn, recipe_id)
    conn.close()
    return recipe


@app.put("/api/recipes/{recipe_id}")
def update_recipe(recipe_id: int, data: RecipeUpdate):
    conn = get_db()
    existing = conn.execute("SELECT id FROM recipes WHERE id = ?", (recipe_id,)).fetchone()
    if not existing:
        conn.close()
        raise HTTPException(404, "Recipe not found")

    updates = {}
    for field, value in data.model_dump(exclude_none=True).items():
        if field in ("ingredients", "steps"):
            continue
        if field == "dietary_tags":
            updates[field] = json.dumps(value)
        elif field == "meal_type":
            updates[field] = _normalize_meal_type(value)
        else:
            updates[field] = value

    if updates:
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        conn.execute(f"UPDATE recipes SET {set_clause} WHERE id = ?",
                     list(updates.values()) + [recipe_id])

    if data.ingredients is not None:
        conn.execute("DELETE FROM ingredients WHERE recipe_id = ?", (recipe_id,))
        for ing in data.ingredients:
            conn.execute("""
                INSERT INTO ingredients (recipe_id, group_name, name, quantity, unit, sort_order)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (recipe_id, ing.group_name, ing.name, ing.quantity, ing.unit, ing.sort_order))

    if data.steps is not None:
        conn.execute("DELETE FROM steps WHERE recipe_id = ?", (recipe_id,))
        for step in data.steps:
            conn.execute("""
                INSERT INTO steps (recipe_id, step_number, instruction, timer_min)
                VALUES (?, ?, ?, ?)
            """, (recipe_id, step.step_number, step.instruction, step.timer_min))

    conn.commit()
    recipe = fetch_full_recipe(conn, recipe_id)
    conn.close()
    return recipe


@app.post("/api/recipes/{recipe_id}/cooked")
def mark_cooked(recipe_id: int):
    conn = get_db()
    now = datetime.now().isoformat()
    conn.execute("""
        UPDATE recipes SET cook_count = cook_count + 1, date_last_cooked = ? WHERE id = ?
    """, (now, recipe_id))
    conn.commit()
    recipe = fetch_full_recipe(conn, recipe_id)
    conn.close()
    if not recipe:
        raise HTTPException(404, "Recipe not found")
    return recipe


@app.post("/api/admin/backfill-images")
async def backfill_images():
    """Manually trigger image backfill for all recipes missing one."""
    import asyncio
    asyncio.create_task(_backfill_missing_images())
    return {"status": "backfill started"}


@app.delete("/api/recipes/{recipe_id}")
def delete_recipe(recipe_id: int):
    conn = get_db()
    conn.execute("DELETE FROM recipes WHERE id = ?", (recipe_id,))
    conn.commit()
    conn.close()
    return {"deleted": recipe_id}


@app.get("/api/recipes/{recipe_id}/ingredients")
def get_ingredients_for_cart(recipe_id: int, servings: int = 0):
    """Returns ingredient list formatted for grocery cart import."""
    conn = get_db()
    recipe = fetch_full_recipe(conn, recipe_id)
    conn.close()
    if not recipe:
        raise HTTPException(404, "Recipe not found")

    ingredients = recipe["ingredients"]
    original_servings = recipe["servings"] or 1

    if servings and servings != original_servings:
        ratio = servings / original_servings
        for ing in ingredients:
            try:
                qty = float(ing["quantity"])
                ing["quantity"] = str(round(qty * ratio, 2))
            except (ValueError, TypeError):
                pass

    return {
        "recipe_title": recipe["title"],
        "servings": servings or original_servings,
        "ingredients": [
            {"name": i["name"], "quantity": i["quantity"], "unit": i["unit"]}
            for i in ingredients
        ]
    }


@app.get("/api/stats")
def get_stats():
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM recipes").fetchone()[0]
    cooked = conn.execute("SELECT COUNT(*) FROM recipes WHERE cook_count > 0").fetchone()[0]
    avg_taste = conn.execute("SELECT AVG(rating_taste) FROM recipes WHERE rating_taste > 0").fetchone()[0] or 0
    avg_ease = conn.execute("SELECT AVG(rating_ease) FROM recipes WHERE rating_ease > 0").fetchone()[0] or 0
    avg_health = conn.execute("SELECT AVG(rating_health) FROM recipes WHERE rating_health > 0").fetchone()[0] or 0
    cuisines = [r[0] for r in conn.execute(
        "SELECT DISTINCT cuisine FROM recipes WHERE cuisine != '' ORDER BY cuisine"
    ).fetchall()]
    meal_types = [r[0] for r in conn.execute(
        "SELECT DISTINCT meal_type FROM recipes WHERE meal_type != '' ORDER BY meal_type"
    ).fetchall()]
    # Collect all unique dietary tags across recipes
    tag_rows = conn.execute(
        "SELECT dietary_tags FROM recipes WHERE dietary_tags != '[]' AND dietary_tags != ''"
    ).fetchall()
    all_tags = set()
    for row in tag_rows:
        try:
            tags = json.loads(row[0])
            all_tags.update(t.strip() for t in tags if t.strip())
        except (json.JSONDecodeError, TypeError):
            pass
    conn.close()
    return {
        "total_recipes": total,
        "cooked_recipes": cooked,
        "avg_taste_rating": round(avg_taste, 1),
        "avg_ease_rating": round(avg_ease, 1),
        "avg_health_rating": round(avg_health, 1),
        "cuisines": cuisines,
        "meal_types": meal_types,
        "dietary_tags": sorted(all_tags),
    }


# ---------------------------------------------------------------------------
# Route — Identify dish from ingredients/instructions
# ---------------------------------------------------------------------------

@app.post("/api/recipes/identify")
async def identify_recipe(data: IdentifyRequest):
    """Analyze ingredients + instructions to identify the dish, generate a name,
    find an image, and write a brief description."""
    if not data.ingredients and not data.instructions:
        raise HTTPException(400, "Provide at least ingredients or instructions")
    result = await identify_dish(data.ingredients, data.instructions)
    return result


# ---------------------------------------------------------------------------
# Routes — Grocery List
# ---------------------------------------------------------------------------

@app.get("/api/grocery-list")
def get_grocery_list():
    conn = get_db()
    rows = conn.execute("SELECT * FROM grocery_list ORDER BY date_added DESC, id DESC").fetchall()
    items = [row_to_dict(r) for r in rows]
    conn.close()
    return {"items": items, "count": len(items)}


@app.post("/api/grocery-list")
def add_grocery_item(data: GroceryItemIn):
    conn = get_db()
    now = datetime.now().isoformat()
    cur = conn.execute("""
        INSERT INTO grocery_list (recipe_id, recipe_title, name, quantity, unit, date_added)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (data.recipe_id, data.recipe_title, data.name, data.quantity, data.unit, now))
    conn.commit()
    item = row_to_dict(conn.execute("SELECT * FROM grocery_list WHERE id = ?", (cur.lastrowid,)).fetchone())
    conn.close()
    return item


@app.post("/api/grocery-list/add-recipe/{recipe_id}")
def add_recipe_to_grocery_list(recipe_id: int, servings: int = 0):
    """Add all ingredients from a recipe to the grocery list, scaled to servings."""
    conn = get_db()
    recipe = fetch_full_recipe(conn, recipe_id)
    if not recipe:
        conn.close()
        raise HTTPException(404, "Recipe not found")

    original_servings = recipe["servings"] or 1
    ratio = servings / original_servings if servings and servings != original_servings else 1
    now = datetime.now().isoformat()
    added = []

    for ing in recipe["ingredients"]:
        qty = ing["quantity"]
        if ratio != 1:
            try:
                qty = str(round(float(qty) * ratio, 2))
            except (ValueError, TypeError):
                pass
        cur = conn.execute("""
            INSERT INTO grocery_list (recipe_id, recipe_title, name, quantity, unit, date_added)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (recipe_id, recipe["title"], ing["name"], qty, ing["unit"], now))
        added.append(cur.lastrowid)

    conn.commit()
    items = [row_to_dict(conn.execute("SELECT * FROM grocery_list WHERE id = ?", (aid,)).fetchone()) for aid in added]
    conn.close()
    return {"added": len(items), "items": items, "recipe_title": recipe["title"]}


@app.put("/api/grocery-list/{item_id}")
def update_grocery_item(item_id: int, data: GroceryItemUpdate):
    conn = get_db()
    existing = conn.execute("SELECT id FROM grocery_list WHERE id = ?", (item_id,)).fetchone()
    if not existing:
        conn.close()
        raise HTTPException(404, "Item not found")
    updates = {}
    if data.checked is not None:
        updates["checked"] = 1 if data.checked else 0
    if data.name is not None:
        updates["name"] = data.name
    if data.quantity is not None:
        updates["quantity"] = data.quantity
    if data.unit is not None:
        updates["unit"] = data.unit
    if updates:
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        conn.execute(f"UPDATE grocery_list SET {set_clause} WHERE id = ?",
                     list(updates.values()) + [item_id])
        conn.commit()
    item = row_to_dict(conn.execute("SELECT * FROM grocery_list WHERE id = ?", (item_id,)).fetchone())
    conn.close()
    return item


@app.delete("/api/grocery-list/{item_id}")
def delete_grocery_item(item_id: int):
    conn = get_db()
    conn.execute("DELETE FROM grocery_list WHERE id = ?", (item_id,))
    conn.commit()
    conn.close()
    return {"deleted": item_id}


@app.delete("/api/grocery-list")
def clear_grocery_list(checked_only: bool = False):
    conn = get_db()
    if checked_only:
        conn.execute("DELETE FROM grocery_list WHERE checked = 1")
    else:
        conn.execute("DELETE FROM grocery_list")
    conn.commit()
    conn.close()
    return {"cleared": True, "checked_only": checked_only}


# Serve frontend
FRONTEND_PATH = Path(__file__).parent / "index.html"

@app.get("/")
def serve_frontend():
    return FileResponse(FRONTEND_PATH, media_type="text/html")


if __name__ == "__main__":
    print("\n🌿 Personal Cookbook running at http://localhost:8742\n")
    uvicorn.run(app, host="0.0.0.0", port=8742)
