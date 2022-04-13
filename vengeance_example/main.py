
"""
    > cd "{parent folder of /vengeance_example}"
    > python -m vengeance_example.main

    > cd "{/vengeance_example}"
    > python main.py
"""
try:
    import flux_example
    import excel_example
    import share
except (ModuleNotFoundError, ImportError):
    from . import flux_example
    from . import excel_example
    from . import share


def main():
    import sys
    from pathlib import Path

    # module_path = str(Path(__file__).parent.parent.parent)
    # module_path2 = str(Path(__file__).parent.parent)
    sys.path.append('..')
    a = sys.path

    # flux_example.main()
    # excel_example.main()
    pass


if __name__ == '__main__':
    main()
