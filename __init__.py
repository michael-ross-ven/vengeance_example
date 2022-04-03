
"""
start with:
    main.py, or
    flux_example.py / excel_example.py
"""

'''
code below is a hack to get module import syntax to work
    'from . import share'
'''

'''
import sys
from pathlib import Path

module_path = str(Path(__file__).parent.parent)
if module_path not in set(sys.path):
    sys.path.insert(0, module_path)

from . import flux_example
from . import excel_example
from . import share

print('vengeance_example __init__')
'''