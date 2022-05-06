
"""
flux_cls
    * similar idea behind a pandas DataFrame, but is more closely aligned with Python's design philosophy
    * when you're willing to trade for a little bit of speed for a lot simplicity
    * a lightweight, pure-python wrapper class around list of lists
    * applies named attributes to rows; attribute values are mutable during iteration
    * provides convenience aggregate operations (sort, filter, groupby, etc)
    * excellent for prototyping and data-wrangling
"""
import sys

from collections import namedtuple
from datetime import datetime
from pprint import pprint as prettyprint
from typing import Generator
from typing import Union
from typing import Any

import vengeance
from vengeance import flux_cls
from vengeance import print_runtime
from vengeance import print_performance
from vengeance import is_date
from vengeance import to_datetime
from vengeance.util.text import vengeance_message
from vengeance.classes.flux_row_cls import flux_row_cls

try:
    import share
except (ModuleNotFoundError, ImportError):
    from . import share

python_version = sys.version_info
profiler = share.resolve_profiler_function()


@print_runtime('blue')
def main():
    # print(vengeance_message('vengeance: {}'.format(vengeance.__version__)))
    # help(flux_cls)

    instantiate_flux()
    # invalid_instantiations()

    flux = flux_cls(share.random_matrix(num_rows=100,
                                        num_cols=5,
                                        len_values=3))
    iterate_flux_rows(flux)
    iterate_primitive_rows(flux)

    flux_sort_and_filter_methods(flux)
    flux_grouping_methods(flux)

    flux_jagged_rows(flux)
    flux_rows_methods(flux)
    flux_columns_methods(flux)
    flux_column_values(flux)

    flux_join()

    write_to_file(flux)
    read_from_file()

    # read_from_excel()
    # write_to_excel(flux)

    flux_subclass()

    # attribute_access_performance()
    # attribute_access_performance_exper()


def instantiate_flux():
    # region {matrix objects}
    some_namedtuple = namedtuple('some_namedtuple', ('col_a', 'col_b', 'col_c'))

    class some_cls:
        def __init__(self, v_a, v_b, v_c):
            self.col_a = v_a
            self.col_b = v_b
            self.col_c = v_c

        @property
        def property(self):
            return self.col_a

        def method(self):
            return self.col_a

    class some_slots_cls:
        __slots__ = ('col_a',
                     'col_b',
                     'col_c')

        def __init__(self, v_a, v_b, v_c):
            self.col_a = v_a
            self.col_b = v_b
            self.col_c = v_c
    # endregion

    # matrix organized like csv data, column names are provided in first row
    m = share.random_matrix(num_rows=20,
                            num_cols=10,
                            len_values=5)
    flux = flux_cls(m)

    a = dir(flux_cls)
    rp = repr(flux)

    a = flux.headers
    a = flux.header_names()

    a = flux.num_cols
    a = flux.num_rows
    b = len(flux)

    a = flux.is_empty()
    a = flux.is_jagged()

    # help(flux._preview_as_array)
    a = flux.preview_indices
    b = flux._preview_as_tuples
    c = flux._preview_as_array

    flux.preview_indices = slice(-5, None)       # preview last 5 rows
    b = flux._preview_as_array
    flux.preview_indices = [3, 5, 6]             # preview rows 3, 5, 6
    c = flux._preview_as_array

    flux_b = flux.copy()
    # flux_b = flux.copy(deep=True)

    # if duplicate header values
    m = share.random_matrix(num_rows=10,
                            num_cols=4,
                            len_values=5)
    m[0] = ['col_a', 'col_a', 'col_a', 'col_a']
    flux = flux_cls(m)

    a = flux.header_names()
    b = flux.matrix[0].values

    # if spaces
    m = share.random_matrix(num_rows=10,
                            num_cols=4,
                            len_values=5)
    m[0] = ['Col a', 'Col b', 'Col c', 'Col d']
    flux = flux_cls(m)

    a = flux.header_names()
    b = flux.matrix[0].values

    # __init__ from objects
    m = [some_cls('a', 'b', 'c') for _ in range(3)]
    flux_b = flux_cls(m)

    # __init__ from slots objects
    m = [some_slots_cls('a', 'b', 'c') for _ in range(3)]
    flux_b = flux_cls(m)

    # __init__ from namedtuples
    m = [some_namedtuple('a', 'b', 'c') for _ in range(3)]
    flux_b = flux_cls(m)

    return flux


def invalid_instantiations():
    """
    1) matrix must have at least 2 dimensions
    2) certain reserved column names cannot appear as
       dynamic column names in matrix, eg
        headers
        values
        row_label
        __bool__
        __class__
        ...
        __str__
        __subclasshook__
        __weakref__
        _flux_row_cls__raise_attribute_error
        _preview_as_array
        _preview_as_tuple
        dict
        header_names
        is_empty
        is_header_row
        is_jagged
        join_values
        namedrow
        namedtuple
        reserved_names
    """
    from vengeance.classes.flux_row_cls import flux_row_cls

    # no arguments is fine
    flux = flux_cls()

    try:
        '''
        a one-dimensional list is not fine, unknown if was meant to be a:
            row:    [['col_a', 'a']] 
        or 
            column: [['col_a'], ['a']]
        '''
        flux = flux_cls(['col_a', 'a'])
    except IndexError as e:
        print(e)

    reserved = flux_row_cls.reserved_names()
    print('reserved header names: \n{}'.format('\n'.join(reserved)))

    try:
        flux = flux_cls([['headers',
                          'values',
                          'header_names',
                          'is_jagged',
                          '__dict__',
                          '__len__']])
    except NameError as e:
        print(e)

    print()


def iterate_flux_rows(flux: flux_cls):
    """ rows as flux_row_cls objects

    *** preferred iteration syntax ***
    "for row in flux:"
        * iterates flux_rows
        * skips header row, begins at flux.matrix[1]
    """
    flux = flux.copy()

    assert flux.num_rows >= 10

    # individual rows
    row_a = flux.matrix[0]
    row_b = flux.matrix[5]
    row_c = flux.matrix[10]

    # flux.label_row_indices() assigns .row_label property
    a = row_a.row_label
    b = row_b.row_label
    c = row_c.row_label

    a = row_a.values
    b = list(flux.dicts())
    b = list(flux.namedrows())
    b = list(flux.namedtuples())

    for row in flux:
        rp = repr(row)

        a = row.headers
        a = row.values
        a = row.header_names()
        a = row.is_jagged()

        # to help with debugging
        # a = row._preview_as_array
        # a = row._preview_as_tuple
        # i = row.row_label

        # a = row.dict()
        # a = row.namedrow()
        # a = row.namedtuple()

        # read row values
        a = row.col_a
        a = row['col_a']
        a = row[0]
        a = row.values[0]               # row.values[0] is faster than row[0]

        # assign row values
        row.col_a     = a
        row['col_a']  = a
        row[0]        = a
        row.values[0] = a

        # assign multiple row values
        # row.values = ['bleh'] * len(row)
        # row.values[2:] = ['bleh'] * (len(row) - 2)

    # slice matrix
    for row in flux.matrix[5:-5]:
        pass

    # stride matrix
    for row in flux.matrix[::3]:
        pass

    # row offset comparisions
    for row_1, row_2 in zip(flux.matrix[1:], flux.matrix[2:]):
        if row_1.col_a == row_2.col_b:
            pass


def iterate_primitive_rows(flux: flux_cls):
    """ rows as primitive values """
    flux = flux.copy()

    assert flux.num_rows >= 10

    # individual rows
    row = flux.matrix[0].values
    row = flux.matrix[5].values
    row = flux.matrix[10].values

    for row in flux:
        a = row.values[0]
        b = row.values[1]

    # values
    m = list(flux.values())
    # or
    m = [*flux.values()]
    # or
    m = [[*row.values] for row in flux]

    # build new matrix of primitive values
    m = [[h.replace('col', 'new_col') for h in flux.header_names()]]
    m.extend(flux.values(3, 5))

    flux_b = flux_cls(m)

    pass


def flux_sort_and_filter_methods(flux: flux_cls):
    """
    methodnames ending in -ed are not in-place, like python's sorted() and sort()
      flux.sort()
      flux = flux.sorted()
      flux.filter()
      flux = flux.filtered()
    """

    # region {flux filter functions}

    # variables for filter functions
    criteria_a = {'c', 'd', 'e', 'f', 'z'}
    criteria_b = {'a', 'b', 'm'}

    def starts_with_a(_row_):
        """ first-class function

        filter functions should return a boolean value
            False for rows that will be excluded
            True  for rows that will be included
        """
        return (str(_row_.col_a).startswith('a') or
                str(_row_.col_b).startswith('a') or
                str(_row_.col_c).startswith('a'))

    def starts_with_criteria(_row_):
        """ first-class function referencing variables from closure

        filter functions should return a boolean value
            False for rows that will be excluded
            True  for rows that will be included

        closure scope bypasses the need for additional parameters
        to be passed to function, eg
            starts_with_criteria(_row_, criteria_a, criteria_b)
        """
        return (str(_row_.col_a)[0] in criteria_a or
                str(_row_.col_b)[0] in criteria_b)
    # endregion

    flux_a = flux.copy()
    flux_b = flux.copy()

    # in-place modifications
    flux_b.reverse()

    flux_b.sort('col_b')
    flux_b.sort('col_a', 'col_b', 'col_c',
                reverse=[False, True, False])

    flux_b.filter(starts_with_a)
    flux_b.filter(starts_with_criteria)
    # flux_b.filter(lambda _row_: str(_row_.col_b) != 'b')
    flux_b.filter_by_unique('col_a', 'col_b')

    # returns new flux_cls
    flux_b = flux_a.reversed()

    flux_b = flux_a.sorted('col_b')
    flux_b = flux_a.sorted('col_a', 'col_b', 'col_c',
                           reverse=[True, False, True])

    flux_b = flux_a.filtered(starts_with_a)
    flux_b = flux_a.filtered(starts_with_criteria)
    # flux_b = flux_a.filtered(lambda _row_: str(_row_.col_b) != 'b')
    flux_b = flux_a.filtered_by_unique('col_a', 'col_b')

    pass


def flux_grouping_methods(flux: flux_cls):
    """
    two extremely important methods introduced here:
        .map_rows_append()
        .map_rows_nested()
    """
    from vengeance import to_datetime

    # region {closures}
    def extract_month(row):
        dt = to_datetime(row.date, '%Y-%m-%d')
        return dt.month

    def extract_year_and_month(row):
        dt = to_datetime(row.date, '%Y-%m-%d')
        return dt.year, dt.month

    # endregion
    flux = flux.copy()

    flux['col_a'] = ['a'] * flux.num_rows
    flux['col_b'] = ['b'] * flux.num_rows

    a = flux.unique('col_a')
    b = flux.unique('col_a', 'col_b')

    # original ordering of values is maintained: returns ordereddict keys, not an unordered set
    s = flux.unique.__doc__
    t = type(a)                                 # <ordereddict keys>

    # .map_rows() and .map_rows_append() have slightly different behavior
    d_1 = flux.map_rows('col_a', 'col_b')
    d_2 = flux.map_rows_append('col_a', 'col_b')

    # k = ('a', 'b')
    a = d_1[('a', 'b')]          # .map_rows():        only ever stores a single row
    b = d_2[('a', 'b')]          # .map_rows_append(): a list of rows, effectively, a groupby operation

    # specify column values to map
    d = flux.map_rows('col_a')
    d = flux.map_rows('col_a', 'col_b', 'col_c')
    d = flux.map_rows(1, 2)
    d = flux.map_rows(slice(-3, -1))

    flux['value_a'] = [100.0] * flux.num_rows

    d = flux.map_rows_append('col_a', 'col_b')
    countifs = {k: len(rows) for k, rows in d.items()}
    sumifs   = {k: sum([row.value_a for row in rows])
                                    for k, rows in d.items()}

    # map dictionary values to types other than flux_row_cls
    d = flux.map_rows_append('col_a', 'col_b', rowtype='dict')
    d = flux.map_rows_append('col_a', 'col_b', rowtype='list')
    d = flux.map_rows_append('col_a', 'col_b', rowtype='tuple')
    d = flux.map_rows_append('col_a', 'col_b', rowtype='namedrow')
    d = flux.map_rows_append('col_a', 'col_b', rowtype='namedtuple')

    # or use actual class instead of string
    # d = flux.map_rows_append('col_a', 'col_b', rowtype=dict)
    # d = flux.map_rows_append('col_a', 'col_b', rowtype=list)
    # d = flux.map_rows_append('col_a', 'col_b', rowtype=tuple)

    # group rows: hierarchically nested column values
    m = [['col_a', 'col_b', 'col_c']] + \
        [['a', 'b', 'c'] for _ in range(3)] + \
        [['c', 'd', 'e'] for _ in range(3)] + \
        [['e', 'f', 'g'] for _ in range(3)] + \
        [['a', 'b', 'g'] for _ in range(2)] + \
        [['c', 'b', 'e'] for _ in range(5)]
    flux_b = flux_cls(m)

    # .groupby() aliased to .map_rows_nested()
    # d_1 = flux_b.map_rows_nested('col_a', 'col_b')
    # d_2 = flux_b.groupby('col_a', 'col_b')

    # compare differences to .map_rows_append()
    d_1 = flux_b.map_rows_nested('col_a', 'col_b')
    d_2 = flux_b.map_rows_append('col_a', 'col_b')
    # a = list(d_1.keys())
    # b = list(d_2.keys())

    m = [['date', 'col_a', 'col_b', 'col_c']] + \
        [['2000-01-01', 'a', 'b', 'c'] for _ in range(3)] + \
        [['2001-01-01', 'c', 'd', 'e'] for _ in range(3)] + \
        [['2002-01-01', 'e', 'f', 'g'] for _ in range(3)] + \
        [['2003-01-01', 'a', 'b', 'g'] for _ in range(2)] + \
        [['2004-01-01', 'c', 'b', 'e'] for _ in range(5)]
    flux_b = flux_cls(m)

    # mapping methods also accept a function
    d_1 = flux_b.map_rows_nested(extract_year_and_month)
    d_2 = flux_b.map_rows_append(extract_year_and_month)
    # a = list(d_1.keys())
    # b = list(d_2.keys())

    # .contiguous()
    #   group rows where *adjacent* values are identical
    items = list(flux.contiguous('col_c'))

    pass


def flux_jagged_rows(flux: flux_cls):
    flux = flux.copy()

    assert flux.num_rows >= 5
    assert flux.num_cols >= 3

    row_too_short = flux.matrix[1]
    row_too_long  = flux.matrix[2]

    # check reprs
    flux_repr_before = repr(flux)
    row_repr_before  = repr(row_too_long)

    # make some jagged rows
    del row_too_short.values[1]
    row_too_long.values += ['too long 1', 'too long 2']

    assert flux.is_jagged()
    b = list(flux.jagged_rows())

    # check repr again with jagged rows
    flux_repr_jagged = repr(flux)
    row_repr_jagged  = repr(row_too_long)

    assert '🗲jagged🗲' not in flux_repr_before
    assert '🗲jagged' not in row_repr_before

    assert '🗲jagged🗲' in flux_repr_jagged
    assert '🗲jagged' in row_repr_jagged

    pass


def flux_rows_methods(flux: flux_cls):
    flux_a = flux.copy()
    flux_b = flux.copy()

    rows = share.random_matrix(10,
                               flux.num_cols,
                               with_header=False)

    # insert / append rows from another raw lists
    flux_a.append_rows(rows)
    flux_a.insert_rows(5, rows[:3])

    # insert / append rows from another flux_cls
    flux_b.insert_rows(1, flux_a)
    flux_b.append_rows(flux_a.matrix[10:15])

    # delete all but first 10 rows
    # del flux_a.matrix[11:]
    flux_a.shorten_to(10)
    flux_b.shorten_to(10)

    flux_a = flux.copy()
    flux_b = flux.copy()

    # append rows from flux_a and flux_b
    flux_c = flux_a + flux_b

    # inplace add
    flux_a += flux_b.matrix[-5:]
    flux_a += flux_b.matrix[10:15]
    flux_a += [['a', 'b', 'c']] * 10

    pass


def flux_columns_methods(flux: flux_cls):
    flux = flux.copy()

    flux.rename_columns({'col_a': 'renamed_a',
                         'col_b': 'renamed_b'})

    flux.insert_columns((0,       'inserted_a'),
                        (0,       'inserted_b'),
                        (0,       'inserted_c'),
                        ('col_c', 'inserted_d'))

    flux.insert_columns(('inserted_d', 'inserted_x'),
                        ('inserted_d', 'inserted_y'))

    flux.append_columns('append_a',
                        'append_b',
                        'append_c')

    flux.delete_columns('inserted_a',
                        'inserted_b',
                        'inserted_c',
                        'inserted_d')

    flux.rename_columns({'renamed_a': 'col_a',
                         'renamed_b': 'col_b'})

    # encapsulate insertion, deletion and renaming of columns within single function
    flux_b = flux_cls(share.random_matrix(num_rows=5,
                                          num_cols=5))
    flux_b.reassign_columns('col_c',
                            'col_b',
                            {'col_a': 'renamed_a'},
                            {'col_a': 'renamed_a__duplicate'},
                            '(inserted_a)',
                            '(inserted_b)',
                            '(inserted_c)')
    pass


def flux_column_values(flux: flux_cls):
    flux = flux.copy()

    assert 'col_a' in flux.headers
    assert 'col_b' in flux.headers
    assert 'col_c' in flux.headers

    # single column
    col = [row.col_b for row in flux]
    col = flux['col_b']
    col = flux.columns('col_b')
    col = flux[-1]
    col = list(col)

    # multiple columns
    cols = flux.columns('col_a', 'col_b', 'col_c')
    cols = flux.columns(0, -2, -1)
    cols = flux[1:3]

    a, b, c = flux.columns('col_a', 'col_b', 'col_c')
    a, b, c = flux['col_a', 'col_b', 'col_c']

    # primitive values
    for a, b, c in zip(*flux['col_a', 'col_b', 'col_c']):
        pass

    # append a new column
    flux['append_d'] = ['append'] * flux.num_rows

    # insert a new column
    flux[(0, 'insert_a')] = [['insert'] for _ in range(flux.num_rows)]
    flux[(0, 'enum')] = flux.indices()

    # shorthand to apply a single value to all rows in column
    flux['col_z'] = ['blah'] * flux.num_rows
    flux['col_z'] = [{'bleh': [4, 5, 6]}] * flux.num_rows
    flux['col_z'] = [[1, 2, 3] for _ in range(flux.num_rows)]

    flux['new_col'] = [100.0] * flux.num_rows

    try:
        flux['new_col'] = [100.0] * (flux.num_rows + 1)
        raise IndexError('column is too long, should raise error')
    except IndexError:
        pass

    # set existing values from another column
    flux['col_a'] = flux['col_b']
    # append to a new column
    flux['col_new'] = flux['col_b']
    # combine column values
    flux['col_new'] = [(row.col_a, row.col_b, row.col_c) for row in flux]
    # apply function to column
    flux['col_c'] = [v.lower() for v in flux['col_c']]

    # convert datatypes in column
    # flux['col_c'] = [int(v) for v in flux['col_c']]
    # flux['col_c'] = [float(v) for v in flux['col_c']]
    # flux['col_c'] = [str(v) for v in flux['col_c']]
    # flux['col_c'] = [set(v) for v in flux['col_c']]
    # flux['col_c'] = [to_datetime(v, '%Y-%m-%d') for v in flux['col_c']]
    #   etc...

    pass


def flux_join():

    flux_a = flux_cls([['name', 'id_a', 'sell_price', 'model_num', 'cost'],
                        ['washer', '#6151-165', 50.10, '-x2', None],
                        ['washer', '#6151-166', 50.10, '-x3', None],
                        ['washer', '#6151-167', 50.10, '-x4', None],
                        ['dryer', '#8979-154', 100.50, 'a', None],
                        ['dryer', '#8979-155', 100.50, 'a', None],
                        ['dishwasher', '#6654-810', 130.00, 'v5', None]])

    flux_b = flux_cls([['name', 'id_b', 'cost', 'weight', 'amount'],
                       ['washer', '#6151-165', 50.10, 33.33, 4],
                       ['dryer', '#8979-154', 100.50, 50.50, 6],
                       ['dishwasher', '#6654-810', 130.00, 100.33, 10]])

    flux_a.append_columns('amount')

    # join rows with manual lookups
    mapped_rows_b = flux_b.map_rows('id_b')

    for row_a in flux_a:
        row_b = mapped_rows_b.get(row_a.id_a)
        if row_b is None:
            continue

        row_a.cost   = row_b.cost
        row_a.amount = row_b.amount

    # .join method
    # flux.join(flux_other, {'column_self': 'column_other'})
    for row_a, row_b in flux_a.join(flux_b, {'id_a': 'id_b'}):
        row_a.cost   = row_b.cost
        row_a.amount = row_b.amount

        # or copy all column values in common with row_b
        row_a.join_values(row_b)

    mapped_rows_b = flux_b.map_rows_append('id_b')

    # flux.join(any_dict, 'column_self')
    for row_a, rows_b in flux_a.join(mapped_rows_b, 'id_a'):
        row_a.cost   = sum([row_b.cost for row_b in rows_b])
        row_a.amount = sum([row_b.amount for row_b in rows_b])

    pass


def write_to_file(flux: flux_cls):
    flux.to_csv(share.files_dir + 'flux_file.csv')
    flux.to_json(share.files_dir + 'flux_file.json')
    flux.serialize(share.files_dir + 'flux_file.flux')

    # .to_json() with no path argument returns a json string
    # json_str = flux.to_json()

    # .to_file()
    # flux.to_file(share.files_dir + 'flux_file.csv')
    # flux.to_file(share.files_dir + 'flux_file.json')
    # flux.to_file(share.files_dir + 'flux_file.flux')

    # specify encoding
    # flux.to_csv(share.files_dir + 'flux_file.csv', 'utf-8-sig')
    # flux.to_json(share.files_dir + 'flux_file.json', 'utf-8-sig')

    pass


def read_from_file():
    """ class methods (flux_cls, not flux) """

    flux = flux_cls.from_csv(share.files_dir + 'flux_file.csv')
    flux = flux_cls.from_json(share.files_dir + 'flux_file.json')
    flux = flux_cls.deserialize(share.files_dir + 'flux_file.flux')

    # .from_file()
    # flux = flux_cls.from_file(share.files_dir + 'flux_file.csv')
    # flux = flux_cls.from_file(share.files_dir + 'flux_file.json')
    # flux = flux_cls.from_file(share.files_dir + 'flux_file.flux')

    # specify encoding
    # flux = flux_cls.from_csv(share.files_dir + 'flux_file.csv', 'utf-8-sig')
    # flux = flux_cls.from_json(share.files_dir + 'flux_file.json', 'utf-8-sig')

    # additional kw arguments control how file is read, such as: strict, lineterminator, ensure_ascii, etc
    # flux = flux_cls.from_csv(share.files_dir + 'flux_file.csv', strict=False, lineterminator='\r')
    # nrows: reads a restricted number of rows from csv file
    # flux = flux_cls.from_csv(share.files_dir + 'flux_file.csv', nrows=50})

    pass


def read_from_excel():
    if vengeance.conditional.loads_excel_module is False:
        print('excel module excluded for platform compatibility')
        return

    flux = share.worksheet_to_flux('sheet1')
    flux = share.worksheet_to_flux('sheet1',
                                   c_1='col_a',
                                   c_2='col_a')
    flux = share.worksheet_to_flux('subsections',
                                   c_1='<sect_2>',
                                   c_2='</sect_2>')

    pass


def write_to_excel(flux: flux_cls):
    if vengeance.conditional.loads_excel_module is False:
        print('excel module excluded for platform compatibility')
        return

    share.write_to_worksheet('sheet2', flux)
    share.write_to_worksheet('sheet2', flux.matrix[:4])
    share.write_to_worksheet('sheet1', flux, c_1='F')

    pass


def flux_subclass():
    """
    the transformation idioms in pandas DataFrames can be difficult to interpret, such as
        df['diff'] = np.sign(df.column1.diff().fillna(0)).shift(-1).fillna(0)

    it helps to encapsulate a series of complex state transformations
    in a separate class, where each transformation is given a meaningful
    method name and is responsible for one, and only one action

    the transformation definitions can be controlled by the .commands
    class variable, which provides a high-level description of its intended
    behaviors, without the need to look into any function bodies.
    controlling its behavior through discrete transformations also
    makes each state more explicit, modular and easier to maintain
    """
    m = [['transaction_id', 'name', 'apples_sold', 'apples_bought', 'date'],
         ['id-001', 'alice', 2, 0, '2019-01-13'],
         ['id-002', 'alice', 0, 1, '2018-03-01'],
         ['id-003', 'bob',   2, 5, '2019-07-22'],
         ['id-004', 'chris', 2, 1, '2019-06-28'],
         ['id-005',  None,   7, 1,  None]]
    flux = flux_custom_cls(m, product='apples')

    # prettyprint(flux_custom_cls.commands)
    # print(repr(flux))

    flux.execute_commands(flux.commands)
    # flux.execute_commands(flux.commands, print_commands=True)

    # profiler: useful for helping to debug any performance issues
    # flux.execute_commands(flux.commands, profiler=True)
    # flux.execute_commands(flux.commands, profiler='line_profiler')
    # flux.execute_commands(flux.commands, profiler='print_runtime')

    flux.validate()

    pass


class flux_custom_cls(flux_cls):

    # high-level summary of state transformations
    commands = (('sort',  ('apples_sold', 'apples_bought'),
                          {'reverse': [False, True]}),
                '_replace_null_names',
                '_convert_dates',
                '_count_unique_names',
                '_filter_apples_sold',
                ('append_columns',    ('commission',
                                       'apple_brand',
                                       'revenue',
                                       'apple_bonus'))
                )

    def __init__(self, matrix=None, product=None):
        super().__init__(matrix)

        self.product          = product
        self.num_unique_names = None

    def _replace_null_names(self):
        for row in self:
            if row.name is None:
                row.name = 'unknown'

    def _convert_dates(self):
        # if no errors are expected
        # self['date'] = [to_datetime(v) for v in self['date']]

        # trap rowtype errors
        for i, row in enumerate(self, 1):
            is_valid, row.date = is_date(row.date)
            # if not is_valid:
            #     print("invalid date: '{}', row {:,}".format(row.date, i))

    def _count_unique_names(self):
        self.num_unique_names = len(self.unique('name'))

    def _filter_apples_sold(self):
        def by_apples_sold(_row_):
            return _row_.apples_sold >= 2

        self.filter(by_apples_sold)

    def validate(self):
        error_indices = []
        for i, row in enumerate(self, 1):
            if row.name is None:
                error_indices.append(i)

        if error_indices:
            self.preview_indices = error_indices
            a = self._preview_as_array
            b = self._preview_as_tuples
            raise AssertionError('validation failed')

    def __repr__(self):
        return 'product: {} {}'.format(self.product,
                                       super().__repr__())


def attribute_access_performance():
    """
    if speed of attribute accesses is paramount, use row.values
    eg:
        c_a = flux.headers['col_a']
        for row in flux:
            a = row.values[c_a]
            row.values[c_a] = 'a'

    Ryzen 7 5800X precision boost OC to 5.0 Ghz
    num_rows = 1_000_000

    ν: @flux_example._attribute_access_normal:        755.7 ms
    ν: @flux_example._attribute_access_namedtuples:   232.7 ms
    ν: @flux_example._attribute_access_rva:           143.9 ms
    ν: @flux_example._attribute_access_values:        106.5 ms
    ν: @flux_example._attribute_access_values_unpack: 65.2 ms
    """
    num_rows = 1_000_000

    flux_a = flux_cls(share.random_matrix(num_rows))
    flux_b = flux_a.copy()
    flux_c = flux_a.copy()
    flux_d = flux_a.copy()
    flux_e = flux_a.copy()

    _attribute_access_normal(flux_a)
    _attribute_access_namedtuples(flux_b)
    _attribute_access_rva(flux_c)
    _attribute_access_values(flux_d)
    _attribute_access_values_unpack(flux_e)

    print()


@print_runtime
def _attribute_access_normal(flux: flux_cls):
    for row in flux:
        a = row.col_a
        b = row.col_b
        c = row.col_c


@print_runtime
def _attribute_access_namedtuples(flux: flux_cls):
    for row in flux.namedtuples():
        a = row.col_a
        b = row.col_b
        c = row.col_c


@print_runtime
def _attribute_access_rva(flux: flux_cls):
    rva = flux.row_values_accessor('col_a', 'col_b', 'col_c')
    for row in flux:
        a, b, c = rva(row)


@print_runtime
def _attribute_access_values(flux: flux_cls):
    c_a = flux.headers['col_a']
    c_b = flux.headers['col_b']
    c_c = flux.headers['col_c']

    for row in flux:
        a = row.values[c_a]
        b = row.values[c_b]
        c = row.values[c_c]

        # row.values[c_a] = 'a'
        # row.values[c_b] = 'b'
        # row.values[c_c] = 'c'


@print_runtime
def _attribute_access_values_unpack(flux: flux_cls):
    for row in flux:
        a, b, c = row.values


def attribute_access_performance_exper():
    from line_profiler import LineProfiler
    from vengeance.classes.flux_row_cls import flux_row_cls

    lprofiler = LineProfiler()

    flux_row_cls.__getattr__ = lprofiler(flux_row_cls.__getattr__)
    flux_row_cls.__setattr__ = lprofiler(flux_row_cls.__setattr__)

    num_rows = 1_000
    flux = flux_cls(share.random_matrix(num_rows))

    _attribute_access_exper(flux)

    if lprofiler.functions:
        lprofiler.print_stats()


def _attribute_access_exper(flux: flux_cls):
    for row in flux:
        a = row.col_a
        b = row.col_b
        c = row.col_c

        row.col_a = 'a'
        row.col_b = 'b'
        row.col_c = 'c'


if __name__ == '__main__':
    main()
    share.print_profiler(profiler)
