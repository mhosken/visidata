import collections
import itertools
import functools
from copy import copy

from visidata import asyncthread, Progress, status, fail
from visidata import ColumnItem, ColumnExpr, SubrowColumn, Sheet, Column
from visidata import SheetsSheet

from copy import copy

SheetsSheet.addCommand('&', 'join-sheets', 'vd.replace(createJoinedSheet(selectedRows or fail("no sheets selected to join"), jointype=chooseOne(jointypes)))')

def createJoinedSheet(sheets, jointype=''):
    if jointype == 'append':
        return SheetConcat('&'.join(vs.name for vs in sheets), sources=sheets)
    else:
        vs = copy(sheets[0])
        vs.name = '+'.join(vs.name for vs in sheets)
        vs.reload = functools.partial(SheetJoin, vs, sources=sheets, jointype=jointype)
        return vs

jointypes = {k:k for k in ["inner", "outer", "full", "diff", "append"]}


class RowsForwarder:
    def __init__(self, target, rowfunc=lambda outer: outer):
        self.target = target
        self.rowfunc = rowfunc

    def __len__(self):
        return len(self.target)

    def __getitem__(self, k):
        if isinstance(k, slice):
            return list(self.rowfunc(r) for r in self.target[k])
        return self.rowfunc(self.target[k])

    def __getattr__(self, k):
        return getattr(self.target, k)

    def applyRow(self, func, row, *args, **kwargs):
        return func(self.rowfunc(row), *args, **kwargs)


#### slicing and dicing
# rowdef: [(key, ...), sheet1_row, sheet2_row, ...]
#   if a sheet does not have this key, sheet#_row is None
@asyncthread
def SheetJoin(self, sources=None, jointype='inner'):
        'Column-wise join/merge. `jointype` constructor arg should be one of jointypes.'
        sheets = sources

        # first item in joined row is the key tuple from the first sheet.
        # first columns are the key columns from the first sheet, using its row (0)
        self.columns = []
        for i, c in enumerate(sheets[0].keyCols):
            self.addColumn(SubrowColumn(c.name, c, 1))
        self.setKeys(self.columns)

        rowsBySheetKey = {}
        rowsByKey = {}

        with Progress(total=sum(len(vs.rows) for vs in sheets)*2) as prog:
            for vs in sheets:
                # tally rows by keys for each sheet
                rowsBySheetKey[vs] = collections.defaultdict(list)
                for r in vs.rows:
                    prog.addProgress(1)
                    key = tuple(c.getDisplayValue(r) for c in vs.keyCols)
                    rowsBySheetKey[vs][key].append(r)

            for sheetnum, vs in enumerate(sheets):
                # subsequent elements are the rows from each source, in order of the source sheets
                ctr = collections.Counter(c.name for c in vs.nonKeyVisibleCols)
                for c in vs.nonKeyVisibleCols:
                    newname = c.name if ctr[c.name] == 1 else '%s_%s' % (vs.name, c.name)
                    self.addColumn(SubrowColumn(newname, c, sheetnum+1))

                for r in vs.rows:
                    prog.addProgress(1)
                    key = tuple(c.getDisplayValue(r) for c in vs.keyCols)
                    if key not in rowsByKey: # gather for this key has not been done yet
                        # multiplicative for non-unique keys
                        rowsByKey[key] = []
                        for crow in itertools.product(*[rowsBySheetKey[vs2].get(key, [None]) for vs2 in sheets]):
                            rowsByKey[key].append([key] + list(crow))

        # this is so the original sheet/columns still work
        self.realrows = []
        self.addRow = lambda row,self=self: self.realrows.append(row)
        self.rows = RowsForwarder(self.realrows, lambda rr: rr[1])

        with Progress(total=len(rowsByKey)) as prog:
            for k, combinedRows in rowsByKey.items():
                prog.addProgress(1)

                if jointype == 'full':  # keep all rows from all sheets
                    for combinedRow in combinedRows:
                        self.addRow(combinedRow)

                elif jointype == 'inner':  # only rows with matching key on all sheets
                    for combinedRow in combinedRows:
                        if all(combinedRow):
                            self.addRow(combinedRow)

                elif jointype == 'outer':  # all rows from first sheet
                    for combinedRow in combinedRows:
                        if combinedRow[1]:
                            self.addRow(combinedRow)

                elif jointype == 'diff':  # only rows without matching key on all sheets
                    for combinedRow in combinedRows:
                        if not all(combinedRow):
                            self.addRow(combinedRow)


class ColumnConcat(Column):
    def __init__(self, name, colsBySheet, **kwargs):
        super().__init__(name, **kwargs)
        self.colsBySheet = colsBySheet

    def calcValue(self, row):
        srcSheet, srcRow = row
        srcCol = self.colsBySheet.get(srcSheet, None)
        if srcCol:
            return srcCol.calcValue(srcRow)

    def setValue(self, row, v):
        srcSheet, srcRow = row
        srcCol = self.colsBySheet.get(srcSheet, None)
        if srcCol:
            srcCol.setValue(srcRow, v)
        else:
            fail('column not on source sheet')


# rowdef: (Sheet, row)
class SheetConcat(Sheet):
    'combination of multiple sheets by row concatenation'
    def reload(self):
        self.rows = []
        for sheet in self.sources:
            for r in sheet.rows:
                self.addRow((sheet, r))

        self.columns = []
        self.addColumn(ColumnItem('origin_sheet', 0))
        allColumns = {}
        for srcsheet in self.sources:
            for srccol in srcsheet.visibleCols:
                colsBySheet = allColumns.get(srccol.name, None)
                if colsBySheet is None:
                    colsBySheet = {}  # dict of [Sheet] -> Column
                    allColumns[srccol.name] = colsBySheet
                    if isinstance(srccol, ColumnExpr):
                        combinedCol = copy(srccol)
                    else:
                        combinedCol = ColumnConcat(srccol.name, colsBySheet, type=srccol.type)
                    self.addColumn(combinedCol)

                if srcsheet in colsBySheet:
                    status('%s has multiple columns named "%s"' % (srcsheet.name, srccol.name))

                colsBySheet[srcsheet] = srccol

        self.recalc()  # to set .sheet on columns, needed if this reload becomes async
