"""
RRG Live Data Fix — Auto-patcher
Run: python3 patch-rrg.py
Changes rrg.html to read from rrm_data.json instead of rrg_data.json
"""
import sys, os

FILE = 'rrg.html'
if not os.path.exists(FILE):
    print(f"ERROR: {FILE} not found. Run this in the repo root folder.")
    sys.exit(1)

with open(FILE, 'r', encoding='utf-8') as f:
    code = f.read()

original = code
changes = 0

# ═══ FIX 1: Change data source from rrg_data.json to rrm_data.json ═══
FIND1 = "./data/rrg_data.json"
REPLACE1 = "./data/rrm_data.json"

if FIND1 in code:
    code = code.replace(FIND1, REPLACE1)
    changes += 1
    print("✅ Fix 1: Data source changed from rrg_data.json → rrm_data.json")
else:
    if REPLACE1 in code:
        print("ℹ️  Fix 1: Already using rrm_data.json — skipped")
    else:
        print("⚠️  Fix 1: Pattern not found")

# Save
if changes > 0:
    with open(FILE, 'w', encoding='utf-8') as f:
        f.write(code)
    print(f"\n✅ Saved {changes} change(s) to {FILE}")
    print("Push to GitHub — RRG will show live sector rotation data!")
else:
    print("\nNo changes needed.")
