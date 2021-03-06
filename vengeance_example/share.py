
import os

from typing import Any
import vengeance as vgc
# from vengeance import open_workbook
# from vengeance import close_workbook
# from vengeance import lev_cls
from vengeance import flux_cls

''' :types: '''
wb:      Any
wb_levs: (None, dict)

wb        = None
wb_levs   = {}
files_dir = os.path.split(os.path.realpath(__file__))[0] + '\\files\\'

if not os.path.exists(files_dir):
    raise FileExistsError('whoops, need to modify files_dir')


def is_running_debug():
    import inspect

    for s_frame in reversed(inspect.stack()):
        if s_frame.filename.endswith('pydevd.py'):
            return True

    return False


def resolve_profiler_function():
    from vengeance import print_runtime

    if is_running_debug():
        return print_runtime

    try:
        from line_profiler import LineProfiler
        return LineProfiler()
    except ImportError:
        return print_runtime


def print_profiler(profiler):
    try:
        if profiler.functions:
            profiler.print_stats()
    except AttributeError:
        pass


# noinspection PyTypeChecker
def random_matrix(num_rows=100,
                  num_cols=3,
                  len_values=3,
                  with_header=True,
                  value_type=str):

    from string import ascii_lowercase
    from random import choices
    from random import uniform

    # region {closure functions}
    def header_names():
        h = []
        for ci in range(1, num_cols + 1):

            cs = ''
            while ci > 0:
                ci_2 = (ci - 1) % 26
                cs   = chr(ci_2 + 97) + cs
                ci   = (ci - ci_2) // 26

            h.append('col_{}'.format(cs))

        return h

    def random_chars():
        return ''.join(choices(ascii_lowercase, k=len_values))

    def random_numbers():
        return round(uniform(0, 9), len_values)
    # endregion

    if value_type == str:
        m = [[random_chars() for _ in range(num_cols)]
                             for _ in range(num_rows)]
        # m = [[random_chars()] * num_cols
        #                         for _ in range(num_rows)]
    elif value_type == float:
        m = [[random_numbers() for _ in range(num_cols)]
                               for _ in range(num_rows)]
        # m = [[random_numbers()] * num_cols
        #                           for _ in range(num_rows)]
    else:
        raise AssertionError

    if with_header:
        m.insert(0, header_names())

    # m = tuple(tuple(row) for row in m)

    return m


def set_project_workbook(excel_app='any',
                         **kwargs):
    global wb

    print()
    wb = vgc.open_workbook(files_dir + 'example.xlsm',
                           excel_app,
                           **kwargs)
    return wb


def close_project_workbook(save=True):
    global wb
    global wb_levs

    import gc

    if isinstance(wb_levs, dict):
        wb_levs = {}

    if wb is not None:
        vgc.close_workbook(wb, save)
        wb = None

    gc.collect()


def worksheet_to_lev(ws, *,
                     m_r=1,
                     h_r=2,
                     c_1=None,
                     c_2=None):

    from vengeance import lev_cls

    if isinstance(ws, lev_cls):
        return ws

    # region {closure functions}
    def worksheet_name():
        """ convert ws variable type to hashable value """
        if isinstance(ws, str):
            return ws.lower()
        if hasattr(ws, 'Name'):
            return ws.Name.lower()      # _Worksheet win32com type

        return ws

    def worksheet_headers():
        headers = {}
        if h_r:
            headers.update(lev_cls.index_headers(ws, h_r))
        if m_r:
            headers.update(lev_cls.index_headers(ws, m_r))

        return headers
    # endregion

    global wb
    global wb_levs

    ws_name = worksheet_name()
    if ws_name in ('sheet1', 'empty sheet'):
        h_r = 1
        m_r = 0
    elif c_1 is None:
        c_1 = 'B'

    lev_key = (ws_name,
               m_r, h_r,
               c_1, c_2)
    is_cached = isinstance(wb_levs, dict)

    if is_cached and lev_key in wb_levs:
        return wb_levs[lev_key]

    if wb is None:
        wb = set_project_workbook(ReadOnly=True)

    ws   = wb.Sheets[ws_name]
    ws_h = worksheet_headers()
    c_1  = ws_h.get(c_1, c_1)
    c_2  = ws_h.get(c_2, c_2)

    lev = lev_cls(ws,
                  meta_r=m_r,
                  header_r=h_r,
                  first_c=c_1,
                  last_c=c_2)

    if is_cached:
        wb_levs[lev_key] = lev

    return lev


def worksheet_to_flux(ws, *,
                      m_r=1,
                      h_r=2,
                      c_1=None,
                      c_2=None) -> flux_cls:

    lev = worksheet_to_lev(ws, m_r=m_r, h_r=h_r,
                               c_1=c_1, c_2=c_2)
    return flux_cls(lev)


def write_to_worksheet(ws, m, *,
                       r_1='*h',
                       c_1=None,
                       c_2=None):

    from vengeance.util.iter import is_header_row

    lev = worksheet_to_lev(ws, c_1=c_1, c_2=c_2)
    lev.activate()

    if r_1 == '*a' and not lev.is_empty:
        m = tuple(m)
        if is_header_row(m[0], lev.header_names()):
            m = m[1:]
    else:
        lev.clear('*f %s:*l *l' % r_1)

    lev['*f %s' % r_1] = m









