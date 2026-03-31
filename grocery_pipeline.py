"""
Grocery Pipeline — Recipe text → structured grocery list

Pipeline stages:
  1. parse_ingredients(text) → list of raw ingredient strings
  2. normalize(raw_list) → list of IngredientItem dicts
  3. deduplicate(items) → merged list with summed quantities
  4. categorize(items) → items grouped by store section
  5. format_grocery_list(grouped, recipes, date) → printable markdown list

Usage:
  from grocery_pipeline import run_pipeline
  result = run_pipeline(recipe_texts={"Lemon Chicken": "2 lbs chicken thighs\\n1 lemon..."})
"""

import json
import re
import unicodedata
from collections import defaultdict
from datetime import date
from fractions import Fraction
from typing import Optional


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

UNIT_ALIASES = {
    "tablespoon": "tbsp", "tablespoons": "tbsp", "tbsps": "tbsp", "tbs": "tbsp", "tbsp": "tbsp", "T": "tbsp",
    "teaspoon": "tsp", "teaspoons": "tsp", "tsps": "tsp", "tsp": "tsp", "t": "tsp",
    "cup": "cup", "cups": "cup", "c": "cup",
    "ounce": "oz", "ounces": "oz", "oz": "oz",
    "pound": "lb", "pounds": "lb", "lbs": "lb", "lb": "lb",
    "gram": "g", "grams": "g", "g": "g",
    "kilogram": "kg", "kilograms": "kg", "kg": "kg",
    "milliliter": "ml", "milliliters": "ml", "ml": "ml",
    "liter": "L", "liters": "L",
    "quart": "qt", "quarts": "qt", "qt": "qt",
    "pint": "pt", "pints": "pt", "pt": "pt",
    "gallon": "gal", "gallons": "gal", "gal": "gal",
    "clove": "clove", "cloves": "clove",
    "bunch": "bunch", "bunches": "bunch",
    "head": "head", "heads": "head",
    "can": "can", "cans": "can",
    "package": "pkg", "packages": "pkg", "pkg": "pkg", "pkgs": "pkg",
    "bag": "bag", "bags": "bag",
    "jar": "jar", "jars": "jar",
    "bottle": "bottle", "bottles": "bottle",
    "piece": "piece", "pieces": "piece", "pc": "piece", "pcs": "piece",
    "slice": "slice", "slices": "slice",
    "stick": "stick", "sticks": "stick",
    "sprig": "sprig", "sprigs": "sprig",
    "pinch": "pinch", "pinches": "pinch",
    "dash": "dash", "dashes": "dash",
    "handful": "handful", "handfuls": "handful",
    "large": "large", "medium": "medium", "small": "small",
    "whole": "whole",
}

# Units that can be summed directly
SUMMABLE_UNITS = {
    "tbsp", "tsp", "cup", "oz", "lb", "g", "kg", "ml", "L", "qt", "pt", "gal",
    "clove", "bunch", "head", "can", "pkg", "bag", "jar", "bottle",
    "piece", "slice", "stick", "sprig",
}

# Unit conversions to common base (for smart dedup)
UNIT_CONVERSIONS = {
    "tsp": ("tsp", 1),
    "tbsp": ("tsp", 3),
    "cup": ("tsp", 48),
    "oz": ("oz", 1),
    "lb": ("oz", 16),
    "g": ("g", 1),
    "kg": ("g", 1000),
    "ml": ("ml", 1),
    "L": ("ml", 1000),
    "pt": ("cup", 2),
    "qt": ("cup", 4),
    "gal": ("cup", 16),
}

# Preferred display unit per base unit
DISPLAY_UNITS = {
    "tsp": [("cup", 48), ("tbsp", 3), ("tsp", 1)],
    "oz": [("lb", 16), ("oz", 1)],
    "g": [("kg", 1000), ("g", 1)],
    "ml": [("L", 1000), ("ml", 1)],
    "cup": [("gal", 16), ("qt", 4), ("cup", 1)],
}

# Default pantry staples — skip these unless explicitly listed in large quantity
DEFAULT_PANTRY_STAPLES = {
    "salt", "pepper", "black pepper", "kosher salt", "sea salt", "table salt",
    "salt & pepper", "salt and pepper",
    "olive oil", "vegetable oil", "canola oil", "cooking spray",
    "sugar", "white sugar", "granulated sugar",
    "all-purpose flour", "flour",
    "water",
}

# Unicode fraction map
UNICODE_FRACTIONS = {
    "\u00bc": "1/4", "\u00bd": "1/2", "\u00be": "3/4",
    "\u2153": "1/3", "\u2154": "2/3",
    "\u2155": "1/5", "\u2156": "2/5", "\u2157": "3/5", "\u2158": "4/5",
    "\u2159": "1/6", "\u215a": "5/6",
    "\u215b": "1/8", "\u215c": "3/8", "\u215d": "5/8", "\u215e": "7/8",
}


# ---------------------------------------------------------------------------
# Category mapping (condensed from CATEGORIES.md)
# ---------------------------------------------------------------------------

CATEGORY_MAP = {
    "Produce": [
        "artichoke", "arugula", "asparagus", "avocado", "bean sprouts", "beets", "bell pepper",
        "bok choy", "broccoli", "brussels sprouts", "butternut squash", "cabbage", "carrots",
        "cauliflower", "celery", "corn", "cucumber", "eggplant", "fennel", "garlic", "green beans",
        "green onion", "scallion", "jalapeno", "jalapeño", "kale", "leek", "lettuce", "mushrooms",
        "mushroom", "okra", "onion", "parsnip", "peas", "potato", "potatoes", "radicchio", "radish",
        "rhubarb", "shallot", "snap peas", "spinach", "sweet potato", "tomato", "tomatoes", "turnip",
        "watercress", "zucchini", "squash",
        # fruits
        "apple", "apricot", "banana", "blackberry", "blueberry", "cantaloupe", "cherry", "clementine",
        "coconut", "cranberry", "date", "fig", "grape", "grapefruit", "guava", "honeydew", "kiwi",
        "lemon", "lime", "lychee", "mango", "nectarine", "orange", "papaya", "passion fruit", "peach",
        "pear", "persimmon", "pineapple", "plantain", "plum", "pomegranate", "raspberry", "starfruit",
        "strawberry", "tangerine", "watermelon",
        # fresh herbs
        "basil", "chives", "cilantro", "dill", "lemongrass", "mint", "fresh oregano", "parsley",
        "rosemary", "sage", "tarragon", "thyme", "fresh herbs",
    ],
    "Meat & Seafood": [
        "chicken breast", "chicken thigh", "chicken wing", "chicken drumstick", "whole chicken",
        "ground chicken", "ground turkey", "turkey breast", "turkey", "duck",
        "ground beef", "sirloin", "ribeye", "flank steak", "chuck roast", "brisket", "short ribs",
        "stew meat", "filet mignon", "new york strip", "steak", "beef",
        "pork chop", "pork loin", "pork tenderloin", "ground pork", "pork shoulder", "pork belly",
        "ham", "baby back ribs", "spare ribs", "pork",
        "lamb chop", "ground lamb", "lamb shank", "rack of lamb", "leg of lamb", "lamb",
        "salmon", "tuna", "cod", "tilapia", "halibut", "mahi mahi", "sea bass", "trout",
        "shrimp", "scallops", "crab", "lobster", "mussels", "clams", "oysters", "calamari", "squid",
        "octopus", "anchovies", "bacon", "sausage", "chorizo", "bison", "venison", "fish",
    ],
    "Dairy & Eggs": [
        "milk", "whole milk", "2% milk", "skim milk", "heavy cream", "half and half",
        "sour cream", "cream cheese", "butter", "unsalted butter", "salted butter",
        "yogurt", "greek yogurt", "eggs", "egg",
        "cheddar", "mozzarella", "parmesan", "parmigiano", "ricotta", "gouda", "brie", "feta",
        "goat cheese", "blue cheese", "swiss", "provolone", "cottage cheese", "mascarpone",
        "whipped cream", "cheese", "cream",
    ],
    "Bakery": [
        "bread", "baguette", "ciabatta", "pita", "naan", "tortilla", "tortillas",
        "hamburger buns", "hot dog buns", "english muffins", "bagels", "croissants",
        "dinner rolls", "flatbread", "focaccia",
    ],
    "Deli": [
        "deli turkey", "deli ham", "salami", "pepperoni", "prosciutto", "roast beef",
        "pastrami", "hummus", "rotisserie chicken",
    ],
    "Frozen": [
        "frozen vegetables", "frozen fruit", "frozen pizza", "ice cream", "frozen meals",
        "frozen fries", "frozen seafood", "frozen waffles", "frozen pie crust",
        "puff pastry", "phyllo dough", "edamame",
    ],
    "Pantry & Dry Goods": [
        "rice", "white rice", "brown rice", "jasmine rice", "basmati rice", "arborio rice",
        "quinoa", "couscous", "oats", "oatmeal", "pasta", "spaghetti", "penne", "fusilli",
        "linguine", "farfalle", "orzo", "lasagna", "egg noodles", "ramen noodles", "rice noodles",
        "udon", "breadcrumbs", "panko",
        "canned tomatoes", "diced tomatoes", "crushed tomatoes", "whole tomatoes",
        "tomato paste", "tomato sauce", "canned beans", "black beans", "kidney beans",
        "chickpeas", "white beans", "pinto beans", "refried beans", "canned corn",
        "canned tuna", "coconut milk", "canned coconut milk",
        "chicken broth", "beef broth", "vegetable broth", "stock", "broth",
        "olives", "capers", "artichoke hearts", "roasted red peppers", "pickles",
        "jam", "jelly", "peanut butter", "almond butter", "tahini",
        "lentils", "split peas", "navy beans", "dried beans",
        "cereal", "granola", "pancake mix",
        "flour", "all-purpose flour", "bread flour", "whole wheat flour",
        "sugar", "brown sugar", "powdered sugar", "cornstarch",
        "baking soda", "baking powder",
    ],
    "Spices & Seasonings": [
        "bay leaf", "bay leaves", "black pepper", "cayenne", "chili flakes", "chili powder",
        "cinnamon", "cloves", "coriander", "cumin", "curry powder", "garlic powder",
        "ground ginger", "italian seasoning", "mustard powder", "nutmeg", "onion powder",
        "dried oregano", "oregano", "paprika", "red pepper flakes", "dried rosemary",
        "dried sage", "salt", "kosher salt", "sea salt", "smoked paprika", "dried thyme",
        "turmeric", "white pepper", "everything bagel seasoning", "taco seasoning",
        "ranch seasoning", "seasoning",
    ],
    "Oils & Condiments": [
        "olive oil", "vegetable oil", "canola oil", "sesame oil", "coconut oil", "avocado oil",
        "cooking spray", "vinegar", "white vinegar", "apple cider vinegar", "balsamic vinegar",
        "red wine vinegar", "rice vinegar", "soy sauce", "tamari", "fish sauce",
        "oyster sauce", "hoisin sauce", "sriracha", "hot sauce", "worcestershire sauce",
        "ketchup", "mustard", "dijon mustard", "mayonnaise", "ranch dressing", "salad dressing",
        "bbq sauce", "teriyaki sauce", "buffalo sauce", "salsa", "marinara sauce", "pesto",
        "honey", "maple syrup", "molasses",
    ],
    "Beverages": [
        "water", "sparkling water", "juice", "orange juice", "apple juice", "soda",
        "coffee", "tea", "almond milk", "oat milk", "coconut water", "beer", "wine",
    ],
    "Snacks": [
        "chips", "tortilla chips", "crackers", "pretzels", "popcorn",
        "almonds", "cashews", "peanuts", "walnuts", "pecans", "mixed nuts", "nuts",
        "trail mix", "dried fruit", "raisins", "dried cranberries", "granola bars",
        "rice cakes", "beef jerky",
    ],
    "International / Specialty": [
        "miso paste", "miso", "gochujang", "sambal oelek", "curry paste", "red curry paste",
        "green curry paste", "coconut cream", "tamarind paste", "rice paper", "wonton wrappers",
        "dumpling wrappers", "nori", "seaweed", "wasabi", "mirin", "sake",
        "chinese five spice", "harissa", "za'atar", "sumac", "pomegranate molasses",
        "chipotle in adobo", "dried chiles", "achiote paste",
    ],
    "Baking": [
        "chocolate chips", "cocoa powder", "vanilla extract", "vanilla", "almond extract",
        "baking chocolate", "confectioners sugar", "cream of tartar", "food coloring",
        "gelatin", "yeast", "cornmeal", "pie crust", "parchment paper", "sprinkles",
        "sweetened condensed milk", "evaporated milk",
    ],
}

# Build a reverse lookup: normalized term → category
_CATEGORY_LOOKUP = {}
for cat, items in CATEGORY_MAP.items():
    for item in items:
        _CATEGORY_LOOKUP[item.lower()] = cat


# ---------------------------------------------------------------------------
# Stage 1: Parse raw ingredient lines from text
# ---------------------------------------------------------------------------

def parse_instagram_caption(caption: str) -> tuple[str, str, str]:
    """Extract recipe content from an Instagram caption.

    Instagram captions mix ingredients, steps, hashtags, emojis, @mentions,
    and promotional text. This function separates the useful recipe content.

    Returns:
        (recipe_title, ingredient_text, steps_text)
        - recipe_title: best-guess title from the first meaningful line
        - ingredient_text: extracted ingredient lines (newline-separated)
        - steps_text: extracted step/instruction lines (newline-separated)
    """
    # Strip common IG noise
    cleaned = caption

    # Remove hashtag blocks (often at the end)
    cleaned = re.sub(r"#\w+", "", cleaned)

    # Remove @mentions
    cleaned = re.sub(r"@\w+", "", cleaned)

    # Remove "link in bio" variants
    cleaned = re.sub(r"link\s+in\s+(my\s+)?bio", "", cleaned, flags=re.IGNORECASE)

    # Remove full CTA lines (entire line if it's a CTA)
    cta_pattern = re.compile(
        r"^.*(save\s+this\s+(post|recipe)|follow\s+for\s+more|tag\s+someone|double\s+tap"
        r"|share\s+with|link\s+in\s+bio|who\s+needs\s+this|for\s+later"
        r"|comment\s+below|let\s+me\s+know|drop\s+a|dm\s+me|check\s+out\s+my"
        r"|swipe\s+for|tap\s+the\s+link|made\s+this\?|try\s+this\?|would\s+you\s+try).*$",
        re.IGNORECASE | re.MULTILINE,
    )
    cleaned = cta_pattern.sub("", cleaned)

    # Remove emoji clusters (3+ consecutive emoji) but keep isolated ones as line markers
    cleaned = re.sub(
        r"[\U0001F300-\U0001F9FF\U00002702-\U000027B0\U0000FE00-\U0000FE0F\U0000200D]{3,}",
        "", cleaned
    )

    # Normalize line endings and collapse excessive blank lines
    cleaned = re.sub(r"\r\n", "\n", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)

    lines = [l.strip() for l in cleaned.strip().splitlines() if l.strip()]

    if not lines:
        return ("Untitled Recipe", "", "")

    # --- Identify sections ---
    # Many IG recipes use section headers like "INGREDIENTS:", "DIRECTIONS:", "METHOD:", etc.
    INGREDIENT_HEADERS = re.compile(
        r"^(ingredients|what\s+you.?ll?\s+need|you.?ll?\s+need|shopping\s+list)\s*:?\s*$",
        re.IGNORECASE,
    )
    STEP_HEADERS = re.compile(
        r"^(directions|instructions|steps|method|how\s+to\s+make|preparation)\s*:?\s*$",
        re.IGNORECASE,
    )

    # Action verbs that signal a cooking step (not an ingredient)
    ACTION_VERBS = re.compile(
        r"^(add|bake|blend|boil|bring|broil|brown|chop|combine|cook|cover|cut"
        r"|dice|drain|drizzle|fold|fry|garnish|grate|grill|heat|layer|let"
        r"|marinate|melt|mix|peel|place|plate|pour|preheat|press|reduce|remove"
        r"|rinse|roast|saute|sauté|season|serve|set|simmer|slice|spread|sprinkle"
        r"|squeeze|steam|stir|strain|toss|transfer|trim|turn|whisk|wrap)\b",
        re.IGNORECASE,
    )

    def _looks_like_ingredient(line: str) -> bool:
        """Heuristic: line looks like an ingredient (starts with qty, bullet, or is short food item)."""
        if re.match(r"^[\d½¼¾⅓⅔⅛]", line) or re.match(r"^[-•▪]\s", line):
            return True
        # Short lines (< 50 chars) without action verbs are likely ingredients
        if len(line) < 50 and not ACTION_VERBS.match(line):
            return True
        return False

    def _looks_like_step(line: str) -> bool:
        """Heuristic: line looks like a cooking step."""
        if re.match(r"^(step\s+\d+)", line, re.IGNORECASE):
            return True
        if re.match(r"^\d+[\.\)]\s+[A-Z]", line):
            return True
        if ACTION_VERBS.match(line):
            return True
        if len(line) > 60:
            return True
        return False

    ingredient_lines = []
    step_lines = []
    title_candidate = ""
    current_section = None  # None, "ingredients", "steps", "preamble"
    section_was_explicit = False  # True if set by a header like "Ingredients:"

    for i, line in enumerate(lines):
        # Check for section headers
        if INGREDIENT_HEADERS.match(line):
            current_section = "ingredients"
            section_was_explicit = True
            continue
        if STEP_HEADERS.match(line):
            current_section = "steps"
            section_was_explicit = True
            continue

        # First non-header line is likely the title / intro
        if i == 0 and not title_candidate:
            # Use the first line as title if it's short enough
            if len(line) <= 80:
                title_candidate = line
                # Strip leading emoji from title
                title_candidate = re.sub(
                    r"^[\U0001F300-\U0001F9FF\U00002702-\U000027B0\s]+", "",
                    title_candidate
                ).strip()
                continue
            else:
                # Long first line — probably a description, not a title
                current_section = "preamble"

        # Route lines to the right section
        if current_section == "ingredients":
            # If section was auto-detected (not explicit header), check if we've hit steps
            if not section_was_explicit and _looks_like_step(line) and not _looks_like_ingredient(line):
                current_section = "steps"
                step_lines.append(line)
            else:
                ingredient_lines.append(line)
        elif current_section == "steps":
            step_lines.append(line)
        elif current_section is None or current_section == "preamble":
            # No section detected yet — auto-detect from line content
            if _looks_like_ingredient(line):
                current_section = "ingredients"
                ingredient_lines.append(line)
            elif _looks_like_step(line):
                current_section = "steps"
                step_lines.append(line)

    # If no sections were detected, try a simpler pass over remaining lines
    if not ingredient_lines and not step_lines:
        for line in lines[1:]:  # skip title
            if _looks_like_ingredient(line):
                ingredient_lines.append(line)
            elif _looks_like_step(line):
                step_lines.append(line)

    title = title_candidate or "Untitled Recipe"
    # Clean up any remaining emoji from individual lines
    clean_emoji = lambda s: re.sub(
        r"[\U0001F300-\U0001F9FF\U00002702-\U000027B0\U0000FE00-\U0000FE0F\U0000200D]",
        "", s
    ).strip()

    ingredient_text = "\n".join(clean_emoji(l) for l in ingredient_lines if clean_emoji(l))
    steps_text = "\n".join(clean_emoji(l) for l in step_lines if clean_emoji(l))

    return (title, ingredient_text, steps_text)


def parse_ingredients(text: str) -> list[str]:
    """Split raw recipe text into individual ingredient lines.

    Handles common formats:
    - One ingredient per line
    - Bullet/dash prefixed lines
    - Numbered lines
    - Lines with checkboxes
    """
    lines = []
    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        # Strip common prefixes: bullets, dashes, numbers, checkboxes
        line = re.sub(r"^[\-\*\u2022\u25e6\u25aa]\s*", "", line)
        # Strip numbered list prefix "1. " or "1) " — but NOT "1.25" (decimal numbers)
        line = re.sub(r"^\d+[\.\)]\s+", "", line)
        line = re.sub(r"^\d+\)\s*", "", line)
        line = re.sub(r"^[\[\(][\s\u2713xX]?[\]\)]\s*", "", line)
        line = line.strip()
        if line and len(line) > 1:
            lines.append(line)
    return lines


# ---------------------------------------------------------------------------
# Stage 2: Normalize each ingredient line into structured data
# ---------------------------------------------------------------------------

def _parse_quantity(text: str) -> tuple[Optional[float], str]:
    """Extract numeric quantity from beginning of text. Returns (qty, remaining_text)."""
    text = text.strip()

    # Replace unicode fractions
    for uf, frac in UNICODE_FRACTIONS.items():
        text = text.replace(uf, frac)

    # Handle no-space between number and unit: "1.25lb" → "1.25 lb"
    text = re.sub(r"^(\d+(?:\.\d+)?)(lb|oz|kg|g|ml|tsp|tbsp|cup)\b", r"\1 \2", text)

    # Pattern: "1 1/2", "1/2", "1.5", "1-2" (take average of range), "1"
    # Mixed number: "1 1/2"
    m = re.match(r"^(\d+)\s+(\d+/\d+)\s*(.*)", text)
    if m:
        whole = int(m.group(1))
        frac = float(Fraction(m.group(2)))
        return whole + frac, m.group(3).strip()

    # Fraction: "1/2"
    m = re.match(r"^(\d+/\d+)\s*(.*)", text)
    if m:
        return float(Fraction(m.group(1))), m.group(2).strip()

    # Range: "1.5 to 2" or "2-3" → take the higher value (buy enough)
    m = re.match(r"^(\d+(?:\.\d+)?)\s*(?:[\-–—]|to)\s*(\d+(?:\.\d+)?)\s*(.*)", text)
    if m:
        return float(m.group(2)), m.group(3).strip()

    # Decimal: "1.5"
    m = re.match(r"^(\d+(?:\.\d+)?)\s*(.*)", text)
    if m:
        return float(m.group(1)), m.group(2).strip()

    return None, text


def _parse_unit(text: str) -> tuple[Optional[str], str]:
    """Extract unit from beginning of text. Returns (normalized_unit, remaining_text)."""
    text = text.strip()
    if not text:
        return None, text

    # Try matching known units (longest first to avoid partial matches)
    all_units = sorted(UNIT_ALIASES.keys(), key=len, reverse=True)
    for unit_name in all_units:
        # Word boundary match
        pattern = rf"^{re.escape(unit_name)}(?:\.|\b)\s*(.*)"
        m = re.match(pattern, text, re.IGNORECASE)
        if m:
            return UNIT_ALIASES[unit_name], m.group(1).strip()

    return None, text


def _clean_item_name(text: str) -> str:
    """Clean up the item name: remove prep notes, normalize."""
    text = text.strip()
    # Remove leading "of " (as in "1 cup of flour")
    text = re.sub(r"^of\s+", "", text, flags=re.IGNORECASE)
    # Remove trailing prep instructions in parens
    text = re.sub(r"\s*\(.*?\)\s*$", "", text)
    # Extract prep notes after comma (keep the item part)
    parts = text.split(",", 1)
    item = parts[0].strip()
    notes = parts[1].strip() if len(parts) > 1 else ""
    return item, notes


def _normalize_name(name: str) -> str:
    """Normalize item name for dedup matching."""
    name = name.lower().strip()

    # Remove common descriptors/prep words (both as prefix and suffix)
    prep_words = [
        "fresh", "organic", "large", "medium", "small",
        "chopped", "finely chopped", "roughly chopped", "coarsely chopped",
        "diced", "finely diced",
        "minced", "sliced", "thinly sliced",
        "grated", "freshly grated", "shredded", "crushed", "ground",
        "boneless", "skinless", "boneless skinless", "skin-on", "bone-in",
        "toasted", "roasted", "dried", "frozen", "canned",
        "plain", "unsalted", "salted",
    ]
    # Sort longest first to match "finely chopped" before "chopped"
    for desc in sorted(prep_words, key=len, reverse=True):
        # Remove as prefix ("minced garlic") or suffix ("garlic minced") or standalone
        name = re.sub(rf"\b{re.escape(desc)}\b", "", name)
    name = re.sub(r"\s+", " ", name).strip()

    # Depluralize the LAST word (handles "garlic cloves" → "garlic clove")
    DEPLURAL_EXCEPTIONS = {"cloves": "clove", "chives": "chive", "olives": "olive",
                           "leaves": "leaf", "halves": "half", "potatoes": "potato",
                           "tomatoes": "tomato"}
    words = name.split()
    if words:
        last = words[-1]
        if last in DEPLURAL_EXCEPTIONS:
            words[-1] = DEPLURAL_EXCEPTIONS[last]
        elif last.endswith("ies"):
            words[-1] = last[:-3] + "y"
        elif last.endswith("es") and not last.endswith("ches") and not last.endswith("shes"):
            words[-1] = last[:-2]
        elif last.endswith("s") and not last.endswith("ss") and len(last) > 2:
            words[-1] = last[:-1]
        name = " ".join(words)
    return name.strip()


def _preprocess_line(line: str) -> str:
    """Pre-process an ingredient line to handle special patterns before parsing."""
    # "Salt & pepper" / "Salt and pepper" → keep as-is, will match pantry staples
    # Handle "Juice of X lemon(s)" → convert to "X lemon(s)"
    m = re.match(r"^[Jj]uice\s+of\s+(.*)", line)
    if m:
        rest = m.group(1).strip()
        # "half a large lemon" → "1 lemon" (juice of = need the whole fruit)
        if re.match(r"half\s+a?\s*", rest, re.IGNORECASE):
            rest = re.sub(r"^half\s+a?\s*(large\s+|small\s+|medium\s+)?", "1 ", rest, flags=re.IGNORECASE)
        # "1 lemon" → "1 lemon" (already fine)
        # "1/2 lemon" → "1 lemon" (round up — you buy whole lemons)
        return rest

    # "A couple (generous) pinches (of) X" → treat as pantry by returning "X"
    m = re.match(r"^[Aa]\s+couple\s+(?:of\s+|generous\s+)?(?:pinch(?:es)?\s+(?:of\s+)?)?(.+)", line)
    if m:
        return m.group(1).strip()

    # "A handful of X" → "1 handful X"
    m = re.match(r"^[Aa]\s+handful\s+of\s+(.+)", line)
    if m:
        return f"1 handful {m.group(1).strip()}"

    # "Pinch of X" → treat as pantry
    m = re.match(r"^[Pp]inch\s+of\s+(.+)", line)
    if m:
        return m.group(1).strip()

    return line


def _handle_alternatives(item_name: str) -> str:
    """Handle 'X or Y' patterns — take the first option."""
    # "oregano or thyme" → "oregano"
    # "onion or garlic powder" → "garlic powder" (tricky — keep the more specific)
    m = re.match(r"^(.+?)\s+or\s+(.+)$", item_name)
    if m:
        opt1, opt2 = m.group(1).strip(), m.group(2).strip()
        # If opt2 is longer (more specific), prefer it; otherwise prefer opt1
        return opt2 if len(opt2) > len(opt1) else opt1
    return item_name


def normalize(raw_lines: list[str], source_recipe: str = "") -> list[dict]:
    """Parse each raw ingredient line into a structured dict."""
    items = []
    for line in raw_lines:
        # Pre-process special patterns
        processed = _preprocess_line(line)

        qty, remaining = _parse_quantity(processed)
        unit, remaining = _parse_unit(remaining)
        item_name, notes = _clean_item_name(remaining)

        if not item_name:
            continue

        # Handle "X or Y" alternatives
        item_name = _handle_alternatives(item_name)

        items.append({
            "raw_text": line,
            "quantity": qty,
            "unit": unit,
            "item": item_name,
            "item_normalized": _normalize_name(item_name),
            "notes": notes,
            "source_recipe": source_recipe,
        })
    return items


# ---------------------------------------------------------------------------
# Stage 3: Deduplicate across recipes
# ---------------------------------------------------------------------------

def _convert_to_base(qty: float, unit: str) -> tuple[float, str]:
    """Convert a quantity to its base unit for comparison."""
    if unit in UNIT_CONVERSIONS:
        base_unit, factor = UNIT_CONVERSIONS[unit]
        return qty * factor, base_unit
    return qty, unit


def _convert_from_base(qty: float, base_unit: str) -> tuple[float, str]:
    """Convert base quantity to best display unit."""
    if base_unit in DISPLAY_UNITS:
        for display_unit, factor in DISPLAY_UNITS[base_unit]:
            if qty >= factor:
                return round(qty / factor, 2), display_unit
    return round(qty, 2), base_unit


def _round_to_purchase(qty: float, unit: str) -> float:
    """Round up to practical purchase amounts."""
    if unit == "lb":
        # Round to nearest 0.25 lb
        return round(qty * 4 + 0.49) / 4
    elif unit in ("oz", "g"):
        return round(qty + 0.49)
    elif unit == "cup":
        return round(qty * 4 + 0.49) / 4
    elif unit in ("tbsp", "tsp"):
        return round(qty + 0.49)
    elif unit in ("piece", "clove", "bunch", "head", "can", "pkg", "bag", "jar",
                  "bottle", "slice", "stick", "sprig"):
        return round(qty + 0.49)  # always round up for countable items
    return round(qty, 1)


def deduplicate(items: list[dict], pantry_staples: set = None) -> tuple[list[dict], list[dict]]:
    """Merge duplicate ingredients, sum quantities.

    Returns (shopping_items, pantry_items) — pantry items are flagged separately.
    """
    if pantry_staples is None:
        pantry_staples = DEFAULT_PANTRY_STAPLES

    # Group by normalized name
    groups = defaultdict(list)
    for item in items:
        groups[item["item_normalized"]].append(item)

    shopping = []
    pantry = []

    for norm_name, group in groups.items():
        # Pick the most descriptive display name (longest)
        display_name = max((it["item"] for it in group), key=len)
        sources = list(set(it["source_recipe"] for it in group if it["source_recipe"]))
        all_notes = list(set(it["notes"] for it in group if it["notes"]))

        # Check if this is a pantry staple
        is_pantry = norm_name in {_normalize_name(s) for s in pantry_staples}

        # Try to sum quantities
        has_qty = [it for it in group if it["quantity"] is not None]
        no_qty = [it for it in group if it["quantity"] is None]

        if has_qty:
            # Check if units are compatible
            units = set(it["unit"] for it in has_qty if it["unit"])

            if len(units) <= 1:
                # Same unit (or no unit) — simple sum
                total_qty = sum(it["quantity"] for it in has_qty)
                unit = has_qty[0]["unit"]
                if unit:
                    total_qty = _round_to_purchase(total_qty, unit)
                merged = {
                    "item": display_name,
                    "item_normalized": norm_name,
                    "quantity": total_qty,
                    "unit": unit,
                    "notes": ", ".join(all_notes) if all_notes else "",
                    "sources": sources,
                    "is_pantry": is_pantry,
                }
            else:
                # Try unit conversion
                base_totals = defaultdict(float)
                unconvertible = []
                for it in has_qty:
                    if it["unit"] in UNIT_CONVERSIONS:
                        base_qty, base_unit = _convert_to_base(it["quantity"], it["unit"])
                        base_totals[base_unit] += base_qty
                    elif it["unit"]:
                        unconvertible.append(it)
                    else:
                        unconvertible.append(it)

                if base_totals:
                    # Convert back to display unit
                    for base_unit, total in base_totals.items():
                        disp_qty, disp_unit = _convert_from_base(total, base_unit)
                        disp_qty = _round_to_purchase(disp_qty, disp_unit)
                        merged = {
                            "item": display_name,
                            "item_normalized": norm_name,
                            "quantity": disp_qty,
                            "unit": disp_unit,
                            "notes": ", ".join(all_notes) if all_notes else "",
                            "sources": sources,
                            "is_pantry": is_pantry,
                        }
                else:
                    # Can't merge — take the first
                    it = has_qty[0]
                    merged = {
                        "item": display_name,
                        "item_normalized": norm_name,
                        "quantity": it["quantity"],
                        "unit": it["unit"],
                        "notes": ", ".join(all_notes) if all_notes else "",
                        "sources": sources,
                        "is_pantry": is_pantry,
                    }
        else:
            # No quantities — just note the item
            merged = {
                "item": display_name,
                "item_normalized": norm_name,
                "quantity": None,
                "unit": None,
                "notes": ", ".join(all_notes) if all_notes else "",
                "sources": sources,
                "is_pantry": is_pantry,
            }

        if is_pantry and (merged["quantity"] is None or merged["quantity"] <= 2):
            pantry.append(merged)
        else:
            shopping.append(merged)

    # Sort shopping list by item name
    shopping.sort(key=lambda x: x["item"].lower())
    pantry.sort(key=lambda x: x["item"].lower())

    return shopping, pantry


# ---------------------------------------------------------------------------
# Stage 4: Categorize by store section
# ---------------------------------------------------------------------------

def categorize(items: list[dict]) -> dict[str, list[dict]]:
    """Assign each item to a store section. Returns {section: [items]}."""
    grouped = defaultdict(list)

    for item in items:
        name = item["item_normalized"]
        display = item["item"].lower()

        # Try exact match on normalized name
        cat = _CATEGORY_LOOKUP.get(name)

        # Try exact match on display name
        if not cat:
            cat = _CATEGORY_LOOKUP.get(display)

        # Try substring match (e.g., "chicken thigh" in item "boneless chicken thigh")
        if not cat:
            for term, c in _CATEGORY_LOOKUP.items():
                if term in display or term in name:
                    cat = c
                    break

        # Try the other direction (item name appears in a category term)
        if not cat:
            for term, c in _CATEGORY_LOOKUP.items():
                if name in term or display in term:
                    cat = c
                    break

        item["category"] = cat or "Other"
        grouped[item["category"]].append(item)

    # Sort sections in a logical store-walk order
    section_order = [
        "Produce", "Meat & Seafood", "Dairy & Eggs", "Bakery", "Deli",
        "Frozen", "Pantry & Dry Goods", "Spices & Seasonings", "Oils & Condiments",
        "International / Specialty", "Baking", "Beverages", "Snacks", "Other",
    ]

    ordered = {}
    for section in section_order:
        if section in grouped:
            ordered[section] = grouped[section]
    # Add any sections not in our order
    for section in grouped:
        if section not in ordered:
            ordered[section] = grouped[section]

    return ordered


# ---------------------------------------------------------------------------
# Stage 5: Format as markdown grocery list
# ---------------------------------------------------------------------------

def format_grocery_list(grouped: dict, recipe_names: list[str],
                        list_date: str = None, pantry_items: list[dict] = None) -> str:
    """Generate a formatted markdown grocery list."""
    if not list_date:
        list_date = date.today().strftime("%B %d, %Y")

    lines = [f"## Grocery List — {list_date}"]
    lines.append(f"Recipes: {', '.join(recipe_names)}")
    lines.append("")

    total_items = sum(len(items) for items in grouped.values())
    lines.append(f"**{total_items} items** across {len(grouped)} sections")
    lines.append("")

    for section, items in grouped.items():
        lines.append(f"### {section}")
        for item in items:
            qty_str = ""
            if item["quantity"] is not None:
                # Format quantity nicely
                qty = item["quantity"]
                if qty == int(qty):
                    qty_str = str(int(qty))
                else:
                    qty_str = str(qty)
                if item["unit"]:
                    qty_str += f" {item['unit']}"
                qty_str = f" ({qty_str})"

            source_str = ""
            if item.get("sources"):
                source_str = f" — {', '.join(item['sources'])}"

            lines.append(f"- [ ] {item['item']}{qty_str}{source_str}")
        lines.append("")

    if pantry_items:
        lines.append("### Assumed On Hand (not added to cart)")
        for item in pantry_items:
            lines.append(f"- {item['item']}")
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Full pipeline runner
# ---------------------------------------------------------------------------

def run_pipeline(recipe_texts: dict[str, str], pantry_staples: set = None,
                 list_date: str = None) -> dict:
    """Run the full pipeline.

    Args:
        recipe_texts: {recipe_name: ingredient_text} mapping
        pantry_staples: set of items to skip (uses defaults if None)
        list_date: date string for the list header

    Returns dict with:
        - grocery_list_md: formatted markdown grocery list
        - grouped: categorized items dict
        - shopping_items: flat list of items to buy
        - pantry_items: items assumed on hand
        - recipe_count: number of recipes processed
        - item_count: total shopping items
    """
    # Collect all normalized items across recipes
    all_items = []
    for recipe_name, text in recipe_texts.items():
        raw_lines = parse_ingredients(text)
        normalized = normalize(raw_lines, source_recipe=recipe_name)
        all_items.extend(normalized)

    # Dedup and separate pantry staples
    shopping, pantry = deduplicate(all_items, pantry_staples)

    # Categorize
    grouped = categorize(shopping)

    # Format
    grocery_md = format_grocery_list(
        grouped,
        list(recipe_texts.keys()),
        list_date=list_date,
        pantry_items=pantry
    )

    return {
        "grocery_list_md": grocery_md,
        "grouped": grouped,
        "shopping_items": shopping,
        "pantry_items": pantry,
        "recipe_count": len(recipe_texts),
        "item_count": len(shopping),
    }


# ---------------------------------------------------------------------------
# Cookbook integration helper
# ---------------------------------------------------------------------------

def build_cookbook_payload(recipe_name: str, ingredient_text: str,
                         steps_text: str = "", source_url: str = "",
                         servings: int = 1, cuisine: str = "",
                         meal_type: str = "") -> dict:
    """Build a payload matching the cookbook API's RecipeIn schema.

    Returns a dict ready to POST to /api/recipes.
    """
    raw_lines = parse_ingredients(ingredient_text)
    normalized = normalize(raw_lines)

    ingredients = []
    for i, item in enumerate(normalized):
        ingredients.append({
            "group_name": "",
            "name": item["item"],
            "quantity": str(item["quantity"]) if item["quantity"] is not None else "",
            "unit": item["unit"] or "",
            "sort_order": i,
        })

    steps = []
    if steps_text:
        step_lines = parse_ingredients(steps_text)  # reuse line parser
        for i, line in enumerate(step_lines, 1):
            steps.append({
                "step_number": i,
                "instruction": line,
                "timer_min": 0,
            })

    return {
        "title": recipe_name,
        "description": "",
        "source_url": source_url,
        "image_url": "",
        "prep_time_min": 0,
        "cook_time_min": 0,
        "servings": servings,
        "cuisine": cuisine,
        "meal_type": meal_type,
        "dietary_tags": [],
        "rating_taste": 0,
        "rating_ease": 0,
        "notes": "",
        "ingredients": ingredients,
        "steps": steps,
    }


# ---------------------------------------------------------------------------
# CLI entry point (for quick testing)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Quick test with sample recipes
    test_recipes = {
        "Lemon Herb Chicken": """
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
        "Spicy Miso Ramen": """
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
    }

    result = run_pipeline(test_recipes)
    print(result["grocery_list_md"])
    print(f"\n--- {result['item_count']} items from {result['recipe_count']} recipes ---")
