__author__ = 'Samuel Gratzl'
import numpy as np
import caleydo_server.range as ranges
import itertools
import sqlalchemy
from caleydo_server.dataset_def import ADataSetEntry, ADataSetProvider

def assign_ids(ids, idtype):
  import caleydo_server.plugin

  manager = caleydo_server.plugin.lookup('idmanager')
  return np.array(manager(ids, idtype))

class SQLDatabase(object):
  def __init__(self, config):
    self.config = config
    #access to DB in DBI format
    self.engine = sqlalchemy.create_engine(config['url'])
    import os
    self.name = config.get('name', os.path.basename(config['url']))
    self._db = None

    self.entries = [e for e in map(self.create_entry, config['tables']) if e is not None]

  @property
  def db(self):
    if self._db is None:
      self._db = self.engine.connect()
    return self._db

  def create_entry(self, entry):
    if entry['type'] == 'table':
        return SQLTable(self, entry)
    return None

  def execute(self, sql, *args, **kwargs):
    return self.db.execute(sql, *args, **kwargs)

  def __len__(self):
    return len(self.entries)

  def __iter__(self):
    return iter(self.entries)

  def __getitem__(self, dataset_id):
    for f in self.entries:
      if f.id == dataset_id:
        return f
    return None

class SQLEntry(ADataSetEntry):
  def __init__(self, db, desc):
    super(SQLEntry, self).__init__(desc['name'], db.name, desc['type'])
    self._db = db
    self.desc = desc

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
      self._categories = desc.get('categories', None)
      if isinstance(self._categories, dict):
        self._converter = create_lookup(self._categories)
        self._categories = self._categories.keys()
    elif self.type == 'int' or self.type == 'real':
      self._range = desc['range'] if 'range' in desc else None

  @property
  def categories(self):
    if self._categories is None and self.type == 'categorical':
      self._categories = self._table.categories_of(self.column)
    return self._categories

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

    self._idColumn = desc['idColumn']
    self._table = desc['table']
    self._rowids = None
    self.columns = [SQLColumn(a, i, self) for i,a in enumerate(desc['columns'])]


  @property
  def idtype(self):
    return self.desc['idType']

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
    r = next(iter(self._db.execute('select count(*) as c from '+self._table)))
    return r[0]

  def range_of(self, column):
    r = next(iter(self._db.execute('select min({0}), max({0}) from {1}'.format(column, self._table))))
    return [r[0], r[1]]

  def categories_of(self, column):
    return [ r[0] for r in self._db.execute('select distinct({0}) from {1}'.format(column, self._table)) ]

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

  def aslist(self, range=None):
    return list(self.asiter(range))

  def asiter(self, range=None):
    r = self._db.execute(self._to_query())
    if range is None:
      return r
    return list(r)[range.asslice()]

  def _to_query(self):
    columns = ','.join(d.column for d in self.columns)
    return 'select {0} from {1} order by {2}'.format(columns,self._table, self._idColumn)

  def aspandas(self, range=None):
    import pandas as pd
    df = pd.read_sql_query(self._to_query(), engine=self._db.engine)
    if range is None:
      return df
    return df.iloc[range.asslice()]

  def filter(self, query):
    # perform the query on rows and cols and return a range with just the mathing one
    # np.argwhere
    np.arange(10)
    return ranges.all()

  def asjson(self, range=None):
    rows = self.rows(None if range is None else range[0])
    rowids = self.rowids(None if range is None else range[0])

    dd = [[c(row[c.key]) for c in self.columns] for row in self.asiter(range)]
    r = dict(data=dd, rows=rows, rowIds = rowids)

    return r


class SQLDatabasesProvider(ADataSetProvider):
  def __init__(self):
    import caleydo_server.config
    c = caleydo_server.config.view('caleydo_data_sql')
    import caleydo_server.plugin
    definitions = list(c.databases)
    for definition in caleydo_server.plugin.list('sql-database-definition'):
      definitions.extend(caleydo_server.config.view(definition.configKey).databases)

    self.databases = [SQLDatabase(db) for db in definitions]

  def __len__(self):
    return sum((len(f) for f in self.databases))

  def __iter__(self):
    return itertools.chain(*self.databases)

  def __getitem__(self, dataset_id):
    for db in self.databases:
      r = db[dataset_id]
      if r is not None:
        return r
    return None


def create():
  return SQLDatabasesProvider()
