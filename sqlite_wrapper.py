#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# jmcgrady@twitter.com
#
# query_datastore_builder.py
# REFS: JIRA HWENG-
# REPO: https://cgit.twitter.biz/caique/

import sqlite3
import os
from collections import namedtuple
sql_strs={
    'list_tables':"SELECT name FROM sqlite_master WHERE type='table'ORDER BY name;",
    'create_table':"CREATE table {name}({f_spec});",
    'drop_table':"DROP table {name};",
    'table_info':"SELECT sql FROM sqlite_master WHERE type='table' AND name='{name}';",
    'create_record':'INSERT INTO {table.name} {val_str};',
    'read_record':'SELECT rowid, * FROM {name} WHERE {row_cond};',
    'update_record':'UPDATE {table.name} SET {col_eqs} WHERE {table.row_cond};',
    'delete_record':'DELETE FROM {name} WHERE {row_cond};',
    'create_ts':"CREATE TRIGGER {name}_create_ts AFTER INSERT ON {name} BEGIN UPDATE {name} SET create_ts = DATETIME('NOW') WHERE rowid = new.rowid; END;",
    'update_ts':"CREATE TRIGGER {name}_update_ts AFTER UPDATE ON {name} BEGIN UPDATE {name} SET update_ts = DATETIME('NOW') WHERE rowid = new.rowid; END;"}

class Sqlite_Error(Exception):
    def __init__(self, m_str, item=None):
        if item:
            Exception.__init__(self, m_str.format(**item.__dict__))
        else:
            Exception.__init__(self, m_str)

class Sqlite_Mixin(object):
    def run_sql(self, q_name):
        return self.db.ex_sql(sql_strs[q_name].format(**self.__dict__))

class Sqlite_DB(object):
    def __init__(self, fname):
        while True:
            try:
                self.conn = init_db(fname)
                break
            except:
                pass
        self.conn.row_factory = sqlite3.Row
        self.l_tables = []

    def ex_sql(self, str_sql):
        try:
            db_ret = self.conn.execute(str_sql)
            self.conn.commit()
            return [r for r in db_ret]
        except Exception as e:
            raise Sqlite_Error('{0} returned {1}'.format(str_sql, e.message))
            
    @property
    def tables(self):
        return self.ex_sql(sql_strs['list_tables'])

    def list_tables(self):
        return [r['name'] for r in self.tables]

    def drop_tables(self, tables=None):
        for t_name in tables or self.list_tables():
            self.ex_sql(sql_strs['drop_table'].format(name=t_name))
            
    def def_table(self, t_name, cols=None, t_spec=None):
        mt = Sqlite_Table(self, t_name, cols, t_spec)
        if mt not in self.l_tables: self.l_tables.append(mt)
        return mt

class Sqlite_Table(Sqlite_Mixin):
    def __init__(self, db, t_name, cols=None, t_spec=None):
        self.db = db
        self.name = t_name
        self.cols = [Sqlite_Col(c) for c in cols or self.get_cols()]
        self.col_names = [c.name for c in self.cols]  ##doesn't work with tables from sqlite_master's sql right now
        #~ self.tfact = namedtuple('{0}_row'.format(t_name), self.col_names)
        
        self.l_rows = []
        self.t_spec = t_spec
        self.f_spec = self.fspec

    @property
    def exists(self):
        return self.name in self.db.list_tables()

    def str_spec(self):
        if self.t_spec:
            return ", ".join(["{0} ({1})".format(*i) for i in self.t_spec])

    @property
    def fspec(self):
        return ', '.join([t for t in (', '.join([str(c) for c in self.cols]), self.str_spec()) if t])

    def get_cols(self):
        if self.name not in self.db.list_tables():
            raise Sqlite_Error('table {name} does not exist, must specify cols',self)
        t_sql = self.run_sql('table_info')[0]['sql']
        mret = t_sql[t_sql.find('(')+1:t_sql.rfind(')')].split(', ')  ## parse better for colnames and tspec
        return mret

    def init(self, reset=False): 
        if self.exists and reset:
            self.run_sql('drop_table')
        if not self.exists:
            self.run_sql('create_table')
            if 'update_ts' in self.col_names:
                self.run_sql('update_ts')
            if 'create_ts' in self.col_names:
                self.run_sql('create_ts')
        return self

    def def_row(self, data):
        mr = Sqlite_Row(self, data)
        if mr not in self.l_rows: self.l_rows.append(mr)
        return mr

    def read_row(self, data):
        return self.def_row(data).read()

    def get_all_rows(self):
        self.l_rows = []
        self.row_cond = '1'
        mret = self.run_sql('read_record')
        if mret: self.keys = mret[0].keys()
        for r in mret:
            self.def_row({k:r[k] for k in self.keys})
        return self
    
    def get_row(self, rowid):
        return([r for r in self.l_rows if r.data['rowid'] == rowid][0])

    def get_query(self, strq):
        return([r for r in self.l_rows if r.data['query'] == strq][0])

class Sqlite_Col(object):
    def __init__(self, cspec):
        '''cspec: c_name (coltype) (constraint)'''
        self.name, _, rest = cspec.partition(' ')
        if rest:
            self.coltype, _, self.constraint = rest.partition(' ')
        else:
            self.coltype, self.constraint = None, None

    def __str__(self):
        return " ".join([t for t in (self.name, self.coltype, self.constraint) if t])
        
class Sqlite_Row(Sqlite_Mixin):
    def __init__(self, table, data=None):
        self.table = table
        self.db = self.table.db
        self._values = None
        if isinstance(data, tuple):
            self._values = data
        elif data:  #use dictionaries for tables with col_names from sqlite_master
            self.data = data 

    @property
    def keys(self):
        if self._values: return self.table.col_names
        return self.data.keys()

    @property
    def values(self):
        return self._values or self.data.values()

    def v_str(self):
        if self.keys:
            return "({0}) VALUES ({1})".format(", ".join(["{0}".format(d) for d in self.keys]),", ".join(["'{0}'".format(d) if stringlike(d) else str(d) for d in self.values]))
        else:
            return "VALUES ({0})".format(", ".join(["'{0}'".format(d) if stringlike(d) else d for d in self.values]))

    def create(self):
        self.val_str = self.v_str()
        self.run_sql('create_record')
        return self

    def row_cond(self):
        if not self.keys:
            raise Sqlite_Error('need keys for row_cond')
        return " AND ".join('{0} = {1}'.format(k,v) for k,v in zip(self.keys,["'{0}'".format(d) if stringlike(d) else d for d in self.values]))
        
    def read(self):
        self.table.row_cond = self.row_cond()
        ret_rec = self.table.run_sql('read_record')[0]
        self.keys = ret_rec.keys()
        self.data = tuple(d for d in ret_rec)
        return self

    def update(self, d_new):
        self.table.row_cond = self.row_cond()
        self.col_eqs = ", ".join(["{0} = {1}".format(k,"'{0}'".format(v) if stringlike(v) else v) for k,v in d_new.items()])
        self.run_sql('update_record')
        return self

def stringlike(thing):
    #~ if thing in ['True', 'False', 'None', '']: return False
    return isinstance(thing, str) or isinstance(thing, unicode)

def init_db(fname):
    d_name = fname.rpartition('/')[0]
    if d_name and not os.path.exists(d_name):
        os.makedirs(d_name)
    return sqlite3.connect(fname, check_same_thread = False)

def main():
    my_db = Sqlite_DB('test_db.db')
    print my_db.list_tables()
    #~ my_db.drop_tables()
    #~ print my_db.list_tables()
    #~ mt = my_db.def_table('test_table')
    mt = Sqlite_Table(my_db,'test_table',
        ['col1',
        'col2 CHAR(25)',
        'col3 VARCHAR(25)',
        'col4 NUMERIC NOT NULL',
        'col5 TEXT(25)',
        'update_ts'],
        [('PRIMARY KEY','col1'),
        ('UNIQUE','col2')])
    #~ mt.init(reset=True)
    mt.init()
    #~ mr = mt.read_row({'col1':'thing'})
    mr = mt.def_row({'col1':'tfsng1','col2':'2sad bottles of beer','col4':27})
    mr.create()
    #~ print mr.data
    #~ 
    mt.get_all_rows()
    print [zip(r.keys,r.data) for r in mt.l_rows]
    return 0

if __name__ == '__main__':
    main()

