from collections import defaultdict
import sys
import copy

class Mapper(object):
  
  db_map = {}
  table_map_template = {
    'columns':   {},
    'primary':   {},
    'unique':    {},
    'indexes':   defaultdict(lambda: []),
    'fk_host':   defaultdict(lambda: []),
    'fk_maybe':  {},
    'fk_create': {},
    'pk_changed_to_resolve_unique_conficts': []
  }
  _db_name = None
  _conn = None
  _cursor = None
  _logger = None
  
  def __init__(self, conn, db_name, logger):
    self._db_name = db_name
    self._logger = logger
    
    self._conn = conn
    self._cursor = self._conn.cursor()
  
  def __del__(self):
    if self._cursor:
      self._cursor.close()
  
  def map_db(self):
    self._map_describe()
    self._map_relations()
    self._map_indexes()
    
    return self.db_map
  
  def _map_describe(self):
    cur = self._cursor
    
    cur.execute("SHOW TABLES")

    while True:
      table = cur.fetchone()
      if not table:
	break
      
      table = table.items()[0][1]
      table_map = copy.deepcopy(self.table_map_template)
      
      cur2 = self._conn.cursor()
      cur2.execute("DESCRIBE `%s`" % table)
      
      while True:
	field = cur2.fetchone()
	if not field:
	  break
	
	is_int = "int"  in field['Type'] or \
		 "long" in field['Type']
	
	append_conditions = {
	  'primary':  field['Key'] == 'PRI' and is_int,
	  'unique':   field['Key'] == 'UNI',
	  'fk_maybe': "_id" in field['Field'] and is_int,
	  'columns':  True
	}
	
	if field['Key'] == 'PRI' and not is_int:
	  print "Column `%s`.`%s` is a non-numeric primary key. It is not possible to " \
		"auto-handle it, therefore there could be some problems with it. Please " \
		"make sure its value is unique throughout all merged databases." % (table, field['Field'])
		
	for key, should_append in append_conditions.items():
	  if should_append:
	    field['is_int'] = is_int
	    table_map[key][field['Field']] = field
      
      self.db_map[table] = table_map
      
  def _map_relations(self):
    cur = self._cursor
    
    self._logger.qs = """
    SELECT 
      ke.referenced_table_name parent, 
      ke.referenced_column_name parent_col, 
      ke.table_name child, 
      ke.column_name child_col, 
      ke.constraint_name 
    FROM 
      information_schema.KEY_COLUMN_USAGE ke 
    WHERE 
      ke.referenced_table_name IS NOT NULL AND
      constraint_schema = '%s'
    ORDER BY 
      ke.referenced_table_name
    """ % self._db_name
    cur.execute(self._logger.qs)

    while True:
      data = cur.fetchone()
      if not data:
	break
      child = data['child']
      child_col = data['child_col']
      
      del data['child'], data['child_col']
      
      self.db_map[child]['fk_host'][child_col] = data
      if child_col in self.db_map[child]['fk_maybe'].keys():
        del self.db_map[child]['fk_maybe'][child_col]
 
  def _map_indexes(self):
    cur = self._cursor
    
    self._logger.qs = "SHOW TABLES"
    cur.execute(self._logger.qs)

    index_cur = self._conn.cursor()
    while True:
      data = cur.fetchone()
      if not data:
	break
      
      table_name = data.values()[0]
      
      self._logger.qs = "SHOW INDEXES FROM %s" % table_name
      index_cur.execute(self._logger.qs)
      
      while True:
	index = index_cur.fetchone()
	if not index:
	  break
	  
	if index['Key_name'] == 'PRIMARY' or bool(int(index['Non_unique'])):
	  continue
	
	self.db_map[table_name]['indexes'][index['Key_name']].append(index['Column_name'])

    index_cur.close()
    
    