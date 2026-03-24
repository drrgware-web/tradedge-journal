"""
Remove RRG Canvas from Dashboard — Auto-patcher
Run: python3 patch-dashboard-remove-rrg.py
"""
import sys, os

FILE = 'dashboard.html'
if not os.path.exists(FILE):
    print(f"ERROR: {FILE} not found. Run this in the repo root folder.")
    sys.exit(1)

with open(FILE, 'r', encoding='utf-8') as f:
    code = f.read()

changes = 0

# Remove the RRG Canvas card block
# The card starts with <a href="./rrg.html" and ends with </a>
import re
pattern = r'    <a href="./rrg\.html"[^>]*>.*?</a>\s*'
match = re.search(pattern, code, re.DOTALL)
if match:
    code = code[:match.start()] + code[match.end():]
    changes += 1
    print("✅ Removed RRG Canvas card from dashboard")
else:
    print("⚠️  RRG Canvas card not found (maybe already removed?)")

if changes > 0:
    with open(FILE, 'w', encoding='utf-8') as f:
        f.write(code)
    print(f"\n✅ Saved changes to {FILE}")
    print("Now also delete rrg.html from GitHub:")
    print("  git rm rrg.html")
    print("  git add dashboard.html")
    print('  git commit -m "remove: RRG Canvas (MATRIX covers this)"')
    print("  git push")
else:
    print("\nNo changes needed.")
