#!/usr/bin/env python3
"""
import_recipe.py — Add a recipe to the cookbook from structured data or a JSON file.

Usage:
  # From a JSON file:
  python import_recipe.py recipe.json

  # Pipe JSON from stdin:
  echo '{"title": "My Soup", ...}' | python import_recipe.py

  # Used by the grocery workflow to push parsed recipes to the cookbook API.

Expects the cookbook server running at http://localhost:8742.
"""

import json
import sys
import urllib.request
import urllib.error

COOKBOOK_API = "http://localhost:8742/api/recipes"


def post_recipe(recipe: dict) -> dict:
    """POST a recipe dict to the cookbook API. Returns the created recipe."""
    # Validate required field
    if not recipe.get("title"):
        raise ValueError("Recipe must have a 'title' field")

    # Set sensible defaults
    recipe.setdefault("description", "")
    recipe.setdefault("source_url", "")
    recipe.setdefault("image_url", "")
    recipe.setdefault("prep_time_min", 0)
    recipe.setdefault("cook_time_min", 0)
    recipe.setdefault("servings", 4)
    recipe.setdefault("cuisine", "")
    recipe.setdefault("meal_type", "")
    recipe.setdefault("dietary_tags", [])
    recipe.setdefault("rating_taste", 0)
    recipe.setdefault("rating_ease", 0)
    recipe.setdefault("rating_health", 0)
    recipe.setdefault("notes", "")
    recipe.setdefault("ingredients", [])
    recipe.setdefault("steps", [])

    # Ensure ingredient sort_order
    for i, ing in enumerate(recipe["ingredients"]):
        ing.setdefault("sort_order", i + 1)
        ing.setdefault("group_name", "")
        ing.setdefault("quantity", "")
        ing.setdefault("unit", "")

    # Ensure step numbers
    for i, step in enumerate(recipe["steps"]):
        step.setdefault("step_number", i + 1)
        step.setdefault("timer_min", 0)

    data = json.dumps(recipe).encode("utf-8")
    req = urllib.request.Request(
        COOKBOOK_API,
        data=data,
        headers={"Content-Type": "application/json"},
    )

    try:
        resp = urllib.request.urlopen(req)
        result = json.loads(resp.read())
        return result
    except urllib.error.URLError as e:
        print(f"ERROR: Could not reach cookbook server at {COOKBOOK_API}", file=sys.stderr)
        print(f"  Is the server running? Start with: python cookbook/server.py", file=sys.stderr)
        print(f"  Details: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    # Read from file arg or stdin
    if len(sys.argv) > 1:
        with open(sys.argv[1]) as f:
            recipe = json.load(f)
    elif not sys.stdin.isatty():
        recipe = json.load(sys.stdin)
    else:
        print("Usage: python import_recipe.py <recipe.json>")
        print("       echo '{...}' | python import_recipe.py")
        sys.exit(1)

    result = post_recipe(recipe)
    print(f"Added to cookbook: \"{result['title']}\" (ID: {result['id']})")
    print(f"  Ingredients: {len(result.get('ingredients', []))}")
    print(f"  Steps: {len(result.get('steps', []))}")
    print(f"  Servings: {result.get('servings', '?')}")
    return result


if __name__ == "__main__":
    main()
