import numpy as np
import itertools
import sqlalchemy
from phovea_server.dataset_def import ADataSetEntry, ADataSetProvider, AColumn, ATable

# patch sqlalchemy for better parallelism using gevent
# import sqlalchemy_gevent
# sqlalchemy_gevent.patch_all()


__author__ = 'Samuel Gratzl'


def assign_ids(ids, idtype):
  from phovea_server.plugin import lookup

  manager = lookup('idmanager')
  return np.array(manager(ids, idtype))


class SQLDatabase(object):
  def __init__(self, config):
    self.config = config
    # access to DB in DBI format
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

  def __iter__(self):
    return iter((f for f in self.entries if f.can_read()))

  def __getitem__(self, dataset_id):
    for f in self.entries:
      if f.id == dataset_id and f.can_read():
        return f
    return None


class SQLEntry(ADataSetEntry):
  def __init__(self, db, desc):
    super(SQLEntry, self).__init__(desc['name'], db.name, desc['type'])
    self._db = db
    self.desc = desc


class SQLColumn(AColumn):
  def __init__(self, desc, i, table):
    super(SQLColumn, self).__init__(desc['name'] if 'name' in desc else self.column, desc['type'])
    self._table = table
    self.key = i
    self.column = desc['column']
    self._converter = lambda x: x

    def create_lookup(cats):
      rlookup = {str(v): k for k, v in cats.iteritems()}

      def lookup(x):
        if x is None and 'unknown' in rlookup:
          return rlookup['unknown']
        if x is None and 'unknown' in cats:
          return 'unknown'
        return None if x is None else rlookup[str(x)]

      return lookup

    if self.type == 'categorical':
      self._categories = desc.get('categories', None)
      if isinstance(self._categories, dict):
        self._converter = create_lookup(self._categories)
        self._categories = self._categories.keys()
      self._category_colors = desc.get('category_colors', None)
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
    return np.array([self(v) for v in self._table.rows_of(self.column, range)])

  def dump(self):
    value = dict(type=self.type)
    if self.type == 'categorical':
      value['categories'] = self._category_colors if self._category_colors is not None else self.categories
    if self.type == 'int' or self.type == 'real':
      value['range'] = self.range
    return dict(name=self.name, value=value)


class SQLTable(ATable):
  def __init__(self, db, desc):
    super(SQLTable, self).__init__(desc['name'], db.name, desc['type'])
    self._db = db
    self.desc = desc
    self.idtype = self.desc['idType']

    self._idColumn = desc['idColumn']
    self._table = desc['table']
    self._rowids = None
    self.columns = [SQLColumn(a, i, self) for i, a in enumerate(desc['columns'])]
    self.shape = [self.nrows, len(self.columns)]

  def to_description(self):
    r = super(SQLTable, self).to_description()
    r['idtype'] = self.idtype
    r['columns'] = [d.dump() for d in self.columns]
    r['size'] = [self.nrows, len(self.columns)]
    return r

  @property
  def nrows(self):
    r = next(iter(self._db.execute('select count(*) as c from ' + self._table)))
    return r[0]

  def range_of(self, column):
    r = next(iter(self._db.execute('select min({0}), max({0}) from {1}'.format(column, self._table))))
    return [r[0], r[1]]

  def categories_of(self, column):
    return [r[0] for r in
            self._db.execute('select distinct({0}) from {1} where {0} is not null'.format(column, self._table))]

  def rows_of(self, column, range=None):
    n = np.array([r[0] for r in self._db.execute('select {0} from {1} order by {2}'.format(column, self._table, self._idColumn))])
    if range is None:
      return n
    return n[range.asslice()]

  def rows(self, range=None):
    n = [r[0] for r in self._db.execute('select {0} from {1} order by {0}'.format(self._idColumn, self._table))]
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

  def _to_query(self):
    columns = ','.join(d.column + ' as "' + d.name + '"' for d in self.columns)
    return 'select {0} from {1} order by {2}'.format(columns, self._table, self._idColumn)

  def aspandas(self, range=None):
    import pandas as pd
    result = self._db.db.execute(self._to_query())
    columns = result.keys()
    df = pd.DataFrame.from_records(list(result), columns=columns)

    if range is None:
      return df
    return df.iloc[range.asslice()]


class SQLDatabasesProvider(ADataSetProvider):
  def __init__(self):
    import phovea_server.config
    c = phovea_server.config.view('phovea_data_sql')
    import phovea_server.plugin
    definitions = list(c.databases)
    for definition in phovea_server.plugin.list('sql-database-definition'):
      definitions.extend(phovea_server.config.view(definition.configKey).databases)

    self.databases = [SQLDatabase(db) for db in definitions]

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
