from rapidfuzz import fuzz

query = "Tata Small Cap Fund"
candidates = [
    "TATA Small Cap Fund Regular Plan - Reinvestment of Income Distribution cum capital withdrawal option ",
    "TATA Small Cap Fund Direct Plan - Reinvestment of Income Distribution cum capital withdrawal option ",
    "Tata Small Cap Fund-Regular Plan-Growth",
    "Tata Small Cap Fund-Direct Plan-Growth"
]

print(f"Query: '{query}'")
print("\nFuzzy scores (token_set_ratio):")
for i, cand in enumerate(candidates, 1):
    score = fuzz.token_set_ratio(query.lower(), cand.lower())
    print(f"[{i}] {int(score):3d}% - {cand[:60]}")
