import MySQLdb
import MySQLdb.cursors
import _mysql_exceptions
import sys
import traceback

qs = ""
class MiniLogger(object):
  
  _instance = None
  def __new__(cls, *args, **kwargs):
      if not cls._instance:
	  cls._instance = super(MiniLogger, cls).__new__(
			      cls, *args, **kwargs)
      return cls._instance
      
  def __set__(self,k,v):
    if k == "qs":
      qs = v
    setattr(self, k, v)
  
def create_connection(connection, common_data={}):
  try:
    data = dict(common_data)
    data.update(connection)
  
    return MySQLdb.connect(data['host'], data['user'], data['password'], data['db'], cursorclass=MySQLdb.cursors.DictCursor)
    
  except Exception,e:
    handle_exception("Exception on connecting to the database", e, )

logger = MiniLogger()
def handle_exception(custom_message, exception, connection=None):
   print ""
   print "-----------------------------------------------"
   print custom_message
   if logger.qs:
     print "Last query was: "
     print logger.qs
   print "The error message is: "
   print exception
   print "The traceback is: "
   traceback.print_tb(sys.exc_info()[2])
   print ""
   if connection:
     print "Rollback"
     connection.rollback()
   print ""
   sys.exit()
   

def map_fks(db_map):
  for table_name, table_map in db_map.items():
    for col_name, col_data in table_map['fk_maybe'].items():
      choice = ""
      while True:
	print "Looks like a column: `%s`.`%s` could be a foreign key" % (table_name, col_name)
	print "However no constraint is defined. What would you like to do?"
	print ""
	print "[1] Specify target table and column"
	print "[2] Ignore this column"
	print "[3] Ignore all remaining columns"
	choice = raw_input("-> ")
	print ""
	print ""
	if choice in ["1","2","3"]:
	  break
	else:
	  print '"%s" is not a valid choice' % choice
	  print ""
	  
      if choice == "2":
	continue
      
      if choice == "3":
	break
      
      while True:
	print "Enter a parent table and column in this format:"
	print "table.column"
	parent_string = raw_input("-> ")
	print ""
	print ""
	try:
	  parent, parent_col = parent_string.split(".")
	  try:
	    assert parent in db_map.keys()
	    try: 
	      assert parent_col in [column for table in db_map.values() for column in table['columns'].keys()]
	      break
	    except AssertionError:
	      print "There is no such column in that table"
	  except AssertionError:
	    print "There is no such table"
	except Exception, e:
	  print "Incorrect value!"
      
      db_map[table_name]['fk_create'][col_name] = {'parent': parent, 'parent_col': parent_col}
      del db_map[table_name]['fk_maybe'][col_name]
    else:
      continue
    break