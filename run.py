import sys
import copy
import traceback
from collections import defaultdict

from mysql_merge.utils import MiniLogger, create_connection, handle_exception, map_fks
from mysql_merge.mysql_mapper import Mapper
from mysql_merge.mysql_merger import Merger
import mysql_merge.config as config

# VALIDATE CONFIG:
if len(config.merged_dbs) == 0:
  print "You must specify at least one database to merge"
  sys.exit()
  
# Prepare logger

#####################################################################

# STEP 1 - map database schema, relations and indexes
mapped_db = config.merged_dbs[0]
conn = create_connection(mapped_db, config.common_data)

mapper = Mapper(conn, mapped_db['db'], MiniLogger())
db_map = mapper.map_db()

conn.close()

# STEP 2 - map all the fields that looks like FKs but aren't stored as ones
map_fks(db_map)

# STEP 3 - actually merge all the databases
counter = 0
for source_db in config.merged_dbs:
 counter = counter + 1
 try:
  source_db_tpl = copy.deepcopy(config.common_data)
  source_db_tpl.update(source_db)
  
  destination_db_tpl = copy.deepcopy(config.common_data)
  destination_db_tpl.update(config.destination_db)

  merger = Merger(db_map, source_db_tpl, destination_db_tpl, config, counter, MiniLogger())
  merger.merge()
  
  print "Your DBs was merged successfully"

 except Exception,e:
   handle_exception("There was an unexpected error while merging db %s" % source_db['db'], e, merger._conn)
   
   