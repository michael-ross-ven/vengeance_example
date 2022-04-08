
"""
start with:
    flux_example.py
    excel_example.py
"""
import sys
from pathlib import Path

module_path = str(Path(__file__).parent.parent.parent) + '\\vengeance_example'
# module_path = str(Path(__file__).parent.parent.parent)
if module_path not in set(sys.path):
    sys.path.insert(0, module_path)

print('vengeance_example loaded')
