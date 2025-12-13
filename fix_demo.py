#!/usr/bin/env python
"""Fix the demo file - replace unicode characters and fix path"""

from pathlib import Path

# Read file
with open('demos/end_to_end_demo.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix 1: Fix the path from 'src' to root directory
old_path = "sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))"
new_path = "sys.path.insert(0, str(Path(__file__).parent.parent))"
content = content.replace(old_path, new_path)
print(f"Fixed path: {old_path} -> {new_path}")

# Fix 2: Replace unicode box-drawing characters with ASCII
replacements = {
    '├─': '[+]',
    '└─': '[-]',
    '│': '[|]',
}
for old, new in replacements.items():
    count = content.count(old)
    content = content.replace(old, new)
    if count > 0:
        print(f"Replaced {count} occurrences of '{old}' with '{new}'")

# Write back
with open('demos/end_to_end_demo.py', 'w', encoding='utf-8') as f:
    f.write(content)

print('✓ All fixes applied successfully')
