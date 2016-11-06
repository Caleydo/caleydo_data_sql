import numpy as np
import itertools
import sqlalchemy
from phovea_server.dataset_def import ADataSetEntry, ADataSetProvider
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

  def aslist(self, range=None):
    return [self(v) for v in self._table.rows_of(self.column, range)]

  def asnumpy(self, range=None):
    return np.array(self.aslist(range))

  def dump(self):
    value = dict(type=self.type)
    if self.type == 'categorical':
      value['categories'] = self._category_colors if self._category_colors is not None else self.categories
    if self.type == 'int' or self.type == 'real':
      value['range'] = self.range
    return dict(name=self.name, value=value)


class SQLView(object):
  def __init__(self, desc, table):
    self._table = table
    self._db = self._table._db
    self._idColumn = desc['idColumn']
    self._columnPrefix = desc['columnPrefix']
    self._from = desc['from']
    self._where = desc['where']
    self._arguments = desc['arguments']

  def dump(self):
    return self._arguments

  @property
  def columns(self):
    return self._table.columns

  def _to_query(self, args):
    columns = ','.join(self._columnPrefix + d.column + ' as "' + d.name + '"' for d in self.columns)
    return self._build_query(columns, args)

  def _build_query(self, columns, args):
    q = 'select {0} from {1} where {2} order by {3}'.format(columns, self._from, self._where, self._idColumn)
    kwargs = {a: args[a] for a in self._arguments}
    return sqlalchemy.sql.text(q), kwargs

  def rows(self, args=None):
    q, kwargs = self._build_query(self._idColumn, args)
    n = [r[0] for r in self._db.execute(q, **kwargs)]
    return n

  def rowids(self, args=None):
    return assign_ids(self.rows(args), self._table.idtype)

  def aslist(self, args=None):
    return self.aspandas(args).to_dict('records')

  def aspandas(self, args=None):
    import pandas as pd
    q, kwargs = self._to_query(args)
    result = self._db.db.execute(q, **kwargs)
    columns = result.keys()
    df = pd.DataFrame.from_records(list(result), columns=columns)
    return df

  def asjson(self, args=None):
    rows = self.rows(args)
    rowids = assign_ids(rows, self._table.idtype)

    dd = self.aslist(args)
    r = dict(data=dd, rows=rows, rowIds=rowids)

    return r


class SQLTable(SQLEntry):
  def __init__(self, db, desc):
    super(SQLTable, self).__init__(db, desc)

    self._idColumn = desc['idColumn']
    self._table = desc['table']
    self._rowids = None
    self.columns = [SQLColumn(a, i, self) for i, a in enumerate(desc['columns'])]
    self.views = {k: SQLView(v, self) for k, v in desc.get('views', {}).iteritems()}

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
    r['views'] = {k: v.dump() for k, v in self.views.iteritems()}
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
    n = np.array(
        [r[0] for r in self._db.execute('select {0} from {1} order by {2}'.format(column, self._table, self._idColumn))])
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

  def aslist(self, range=None):
    return self.aspandas(range).to_dict('records')

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

  def asjson(self, range=None):
    rows = self.rows(None if range is None else range[0])
    rowids = self.rowids(None if range is None else range[0])

    dd = self.aslist(range)
    r = dict(data=dd, rows=rows, rowIds=rowids)

    return r


class SQLDatabasesProvider(ADataSetProvider):
  def __init__(self):
    import phovea_server.config
    c = phovea_server.config.view('phovea_data_sql')
    import phovea_server.plugin
    definitions = list(c.databases)
    for definition in phovea_server.plugin.list('sql-database-definition'):
      definitions.extend(phovea_server.config.view(definition.configKey).databases)

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
