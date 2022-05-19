
"""
    > cd "/{parent folder of vengeance_example}/"
    > python -m vengeance_example.main

    > cd "/{vengeance_example}/"
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
    flux_example.main()
    # excel_example.main()

    pass


if __name__ == '__main__':
    main()
