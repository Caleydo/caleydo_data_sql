__author__ = 'Samuel Gratzl'
import numpy as np
import caleydo_server.range as ranges

import itertools


def assign_ids(ids, idtype):
  import caleydo_server.plugin

  manager = caleydo_server.plugin.lookup('idmanager')
  return np.array(manager(ids, idtype))

class SQLDatabase(object):
  def __init__(self, dbconfig):
    self.dbconfig = dbconfig


class SQLEntry(object):
  def __init__(self, db, desc):
    self._db = db
    self.desc = desc
    self.name = desc['name']
    self.type = desc['type']

  def idtypes(self):
    return []

  def to_description(self):
    return dict(type=self.type,
                name=self.name,
                fqname=self._db.name+'/'+self.name)

  def to_idtype_descriptions(self):
    def to_desc(t):
      return dict(id=t, name=t, names=t + 's')

    return map(to_desc, self.idtypes())

class SQLTable(SQLEntry):
  def __init__(self, db, desc):
    super(SQLTable, self).__init__(db, desc)

class SQLColumn(object):
  def __init__(self, desc, i, table):
    self._table = table
    self.key = i
    self.column = desc['column']
    self.name = desc['name'] if 'name' in desc else self.column
    self.type = desc['type']
    self._converter = lambda x: x

    def create_lookup(cats):
      rlookup = { str(v) : k for k,v in cats.iteritems() }
      return lambda x : rlookup[str(x)]

    if self.type == 'categorical':
      self.categories = desc['categories']
      if isinstance(self.categories, dict):
        self._converter = create_lookup(self.categories)
        self.categories = self.categories.keys()
    elif self.type == 'int' or self.type == 'real':
      self._range = desc['range'] if 'range' in desc else None

  @property
  def range(self):
    if self.type == 'int' or self.type == 'real' and self._range is None:
      self._range = self.compute_range()
    return self._range

  def compute_range(self):
    return self._table.range_of(self.column)

  def __call__(self, v):
    return self._converter(v)

  def asnumpy(self, range=None):
    return [self(v) for v in self._table.rows_of(self.column, range)]

  def dump(self):
    value = dict(type=self.type)
    if self.type == 'categorical':
      value['categories'] = self.categories
    if self.type == 'int' or self.type == 'real':
      value['range'] = self.range
    return dict(name=self.name, value=value)


class SQLTable(SQLEntry):
  def __init__(self, db, desc):
    super(SQLTable, self).__init__(db, desc)

    self.columns = [SQLColumn(a, i, self) for i,a in enumerate(desc['columns'])]
    self._idColumn = desc['idColumn']
    self._table = desc['table']
    self._rowids = None

  @property
  def idtype(self):
    return self.desc["idType"]

  def idtypes(self):
    return [self.idtype]

  def to_description(self):
    r = super(SQLTable, self).to_description()
    r['idtype'] = self.idtype
    r['columns'] = [d.dump() for d in self.columns]
    r['size'] = [self.nrows, len(self.columns)]
    return r

  @property
  def nrows(self):
    r = next(self._db.execute('select count(*) as c from '+self._table))
    return r[0]

  def range_of(self, column):
    r = next(self._db.execute('select min({0}), max({0}) from {1}'.format(column, self._table)))
    return [r[0], r[1]]

  def rows_of(self, column, range = None):
    if range is None:
      return np.array(list(self._db.execute('select {0} from {1} order by {2}'.format(column, self._table, self._idColumn))))
    #TODO
    return []


  def rows(self, range=None):
    n = [r[0] for r in self._db.execute('select {0} from {1} order by {0}'.format(self._idColumn,self._table))]
    if range is None:
      return n
    return n[range.asslice()]

  def rowids(self, range=None):
    if self._rowids is None:
      self._rowids = assign_ids(self.rows(), self.idtype)
    n = self._rowids
    if range is None:
      return n
    return n[range.asslice()]

  def asiterlist(self, range=None):
    columns = ','.join(d.column for d in self.columns)
    if range is None:
      return self._db.execute('select {0} from {1} order by {2}'.format(columns,self._table, self._idColumn))
    #TODO
    return []

  def asnumpy(self, range=None):
    if range is None:
      return list(self.asiterlist(range))
    #TODO
    #return n[range[0].asslice()]
    return []

  def filter(self, query):
    # perform the query on rows and cols and return a range with just the mathing one
    # np.argwhere
    np.arange(10)
    return ranges.all()

  def asjson(self, range=None):
    arr = self.asnumpy(range)
    rows = self.rows(None if range is None else range[0])
    rowids = self.rowids(None if range is None else range[0])

    dd = [[c(row[c.key]) for c in self.columns] for row in self.asiterlist(range)]
    r = dict(data=dd, rows=rows, rowIds = rowids)

    return r



class SQLiteDatabase(SQLDatabase):
  def __init__(self, dbconfig):
    super(SQLiteDatabase, self).__init__(dbconfig)
    import sqlite3
    import os
    self.db = sqlite3.connect(dbconfig['url'])
    self.name=dbconfig.get('name', os.path.basename(dbconfig['url']))

    def create_entry(entry):
      if entry['type'] == 'table':
        return SQLTable(self, entry)
    self.entries = [e for e in map(create_entry, dbconfig['tables']) if e is not None]

  def execute(self, sql, *args):
    return self.db.execute(sql, *args)

  def __len__(self):
    return len(self.entries)

  def __iter__(self):
    return iter(self.entries)


class SQLDatabasesProvider(object):
  def __init__(self):
    import caleydo_server.config
    c = caleydo_server.config.view('caleydo_data_sql')
    def create_db(db):
      if db['type'] == 'sqlite3':
        return SQLiteDatabase(db)
      print 'cant handle database type: '+db['type']+''
      return None
    self.databases = [d for d in (create_db(db) for db in c.databases) if d is not None]

  def __len__(self):
    return sum((len(f) for f in self.databases))

  def __iter__(self):
    return itertools.chain(*self.databases)

def create():
  return SQLDatabasesProvider()
