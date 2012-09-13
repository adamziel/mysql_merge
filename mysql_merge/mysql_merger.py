from mysql_merge.utils import create_connection, handle_exception

class Merger(object):
  
  _conn = None
  _cursor = None
  _dbMap = None
  _config = None
  _logger = None
  _counter = 0
  
  _destination_db = None
  _source_db = None
  
  _increment_step = property(lambda self: self._counter * 1000000)
  
  def __init__(self, db_map, source_db, destination_db, config, counter, logger):
    self._source_db = source_db
    self._destination_db = destination_db
    self._db_map = db_map
    self._config = config
    self._counter = counter
    self._logger = logger

    self._conn = create_connection(self._source_db)
    self._cursor = self._conn.cursor()
  
    self.prepare_db()
    
  def prepare_db(self):
    cur = self._cursor
    
    self._logger.qs = "set names utf8"
    cur.execute(self._logger.qs)
    
    self._logger.qs = "set foreign_key_checks=0"
    cur.execute(self._logger.qs)
    
  def __del__(self):
    if self._cursor:
      self._cursor.close()
    
    if self._conn:
      self._conn.close()
    
  def merge(self):
    self._conn.begin()
    
    self.convert_tables_to_innodb()
    self.convert_fks_to_update_cascade()
    self.convert_mapped_fks_to_real_fks()
    self.increment_pks()
    self.map_pks_to_target_on_unique_conflict()
    self.copy_data_to_target()
    self.rollback_pks()
    
    self._conn.commit()
  
  def convert_tables_to_innodb(self):
    cur = self._cursor
    
    # Convert all tables to InnoDB
    for table_name, table_map in self._db_map.items():
      try:
	self._logger.qs = "alter table `%s` engine InnoDB" % (table_name)
	cur.execute(self._logger.qs)
      #except _mysql_exceptions.OperationalError,e:
      except Exception,e:
	handle_exception("There was an error while converting table `%s` to InnoDB\nPlease fix your schema and try again" % table_name, e, self._conn)
  
  def convert_fks_to_update_cascade(self):
    cur = self._cursor
    
    # Convert FK to on update cascade
    for table_name, table_map in self._db_map.items():
      for col_name, fk_data in table_map['fk_host'].items():
	try:
	  self._logger.qs = "alter table `%s` drop foreign key `%s`" % (table_name, fk_data['constraint_name'])
	  cur.execute(self._logger.qs)
    
	  self._logger.qs = "alter table `%s` add foreign key `%s` (`%s`) references `%s` (`%s`) on update cascade" % ( table_name, fk_data['constraint_name'], col_name, fk_data['parent'], fk_data['parent_col'])
	  cur.execute(self._logger.qs)
	except Exception,e:
	  handle_exception("There was an error while converting FK `%s` on `%s`.`%s` to ON UPDATE CASCADE" % (fk_data['constraint_name'], table_name, col_name), e, self._conn)
      
  def convert_mapped_fks_to_real_fks(self):
    cur = self._cursor
    
    # Convert mapped FKs to real FKs
    for table_name, table_map in self._db_map.items():
      for col_name, fk_data in table_map['fk_create'].items():
	constraint_name = ""
	try:
	  constraint_name = "%s_%s_dbmerge" % (table_name, col_name)
	  self._logger.qs = "alter table `%s` add foreign key `%s` (`%s`) references `%s` (`%s`) on update cascade" % ( table_name, constraint_name, col_name, fk_data['parent'], fk_data['parent_col'])
	  cur.execute(self._logger.qs)
	except Exception,e:
	  handle_exception("There was an error while creating new FK `%s` on `%s`.`%s`" % (constraint_name, table_name, col_name), e, self._conn)

  def increment_pks(self):
    cur = self._cursor
    
    # Update all numeric PKs to ID + 1 000 000
    for table_name, table_map in self._db_map.items():
      for col_name, col_data in table_map['primary'].items():
	try:
	  self._logger.qs = "UPDATE `%(table)s` SET `%(pk)s` = `%(pk)s` + %(step)d" % { "table": table_name, "pk": col_name, 'step': self._increment_step }
	  cur.execute(self._logger.qs)
	except Exception,e:
	  handle_exception("There was an error while updating PK `%s`.`%s` to %d + pk_value" % (table_name, col_name, self._increment_step), e, self._conn)
  
  def map_pks_to_target_on_unique_conflict(self):
    cur = self._cursor
    
    # Update all the PKs in the source db to the value from destination db
    # if there's unique value collidinb with target database
    update_cur = self._conn.cursor()
    for table_name, table_map in self._db_map.items():
      pks = table_map['primary'].keys()
      if len(pks) != 1:
	continue
      
      pk_col = pks[0]
      pks_processed = []
      for index_name, columns in table_map['indexes'].items():
	new_pk = old_pk = ""
	try:
	  # Get all rows that have the same unique value as our destination table
	  self._logger.qs = "SELECT t1.`%(pk_col)s` as old_pk, t2.`%(pk_col)s` as new_pk " \
	      "FROM `%(table)s` t1 " \
	      "LEFT JOIN `%(destination_db)s`.`%(table)s` t2 ON (%(join)s)" \
	      "WHERE t2.`%(pk_col)s` is not null" % {
		  'destination_db': self._destination_db['db'],
		  "table": table_name, 
		  "pk_col": pk_col, 
		  "join": " AND ".join(["(t1.`%(column)s` = t2.`%(column)s` AND t2.`%(column)s` is not null)" % {'column': column} for column in columns])
		}
	  cur.execute(self._logger.qs)
	  
	  # Update all those rows PKs - to trigger the CASCADE on all pointers
	  while True:
	    row = cur.fetchone()
	    if not row:
	      break
	    
	    new_pk, old_pk = row['new_pk'], row['old_pk']
	    self._logger.qs = "UPDATE `%(table)s` set `%(pk_col)s`=%(new_pk)s where `%(pk_col)s`=%(old_pk)s" % {
	      'table': table_name,
	      'pk_col': pk_col,
	      'new_pk': row['new_pk'],
	      'old_pk': row['old_pk'],
	    }
	    update_cur.execute(self._logger.qs)
	    self._db_map[table_name]['pk_changed_to_resolve_unique_conficts'].append(str(row['new_pk']))
	    
	except Exception,e:
	  handle_exception("There was an error while normalizing unique index `%s`.`%s` from values '%s' to value '%s'" % (table_name, index_name, old_pk, new_pk), e, self._conn)
	
	"""
	# Delete all updated PKs - cascade was already triggered
	if len(pks_processed):
	  self._logger.qs = "DELETE FROM `%(table)s` WHERE `%(pk_col)s` IN (%(pks)s)" % {
	    'table': table_name,
	    'pk_col': pk_col,
	    'pks': ",".join(pks_processed)
	  }
	  update_cur.execute(self._logger.qs)
	"""
	    
    update_cur.close()
  
  def copy_data_to_target(self):
    cur = self._cursor
    
    # Copy all the data to destination table
    for table_name, table_map in self._db_map.items():
      try:
	where = ""
	if len(table_map['pk_changed_to_resolve_unique_conficts']):
	  where = "WHERE %(pk_col)s NOT IN (%(ids)s)" % {
	    'pk_col': table_map['primary'].keys()[0],
	    'ids': ",".join(table_map['pk_changed_to_resolve_unique_conficts'])
	  }
	self._logger.qs = "INSERT INTO `%(destination_db)s`.`%(table)s` SELECT * FROM `%(source_db)s`.`%(table)s` %(where)s" % {
	  'destination_db': self._destination_db['db'],
	  'source_db': self._source_db['db'],
	  'table': table_name,
	  'where': where
	}
	cur.execute(self._logger.qs)
      except Exception,e:
	hint = "--> HINT: Looks like you runned this script twice on the same database\n" if "Duplicate" in "%s" % e else ""
	handle_exception(("There was an error while moving data between databases. Table: `%s`.\n" + hint) % (table_name), e, self._conn)
      
  def rollback_pks(self):
    cur = self._cursor
    
    # Return all PKs to thei previous state: ID - 1 000 000
    for table_name, table_map in self._db_map.items():
      for col_name, col_data in table_map['primary'].items():
	try:
	  self._logger.qs = "UPDATE `%(table)s` SET `%(pk)s` = `%(pk)s` - %(step)d" % { "table": table_name, "pk": col_name, 'step': self._increment_step }
	  cur.execute(self._logger.qs)
	except Exception,e:
	  handle_exception("There was an error while updating PK `%s`.`%s` to -%d + pk_value" % (table_name, col_name, self._increment_step), e, self._conn)
  