__author__ = 'Samuel Gratzl'

import sql as s
import os

class SQLiteDatabase(s.SQLDatabase):
  def __init__(self, dbconfig):
    super(SQLiteDatabase, self).__init__(dbconfig, dbconfig.get('name', os.path.basename(dbconfig['url'])))
    import sqlite3
    self.db = sqlite3.connect(dbconfig['url'])

  def execute(self, sql, *args):
    return self.db.execute(sql, *args)

def create(dbconfig):
  return SQLiteDatabase(dbconfig)