
import sys
from pathlib import Path

module_path = str(Path(__file__).parent.parent)
if module_path not in set(sys.path):
    sys.path.insert(0, module_path)

try:
    from vengeance_example import flux_example
    from vengeance_example import excel_example
    from vengeance_example import share
except (ImportError, ModuleNotFoundError):
    import flux_example
    import excel_example
    import share


def main():
    flux_example.main()
    # excel_example.main()
    pass


if __name__ == '__main__':
    main()
