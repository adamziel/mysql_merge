import MySQLdb
import MySQLdb.cursors
import _mysql_exceptions
import sys
import traceback
from mysql_merge.levenshtein import levenshtein_lowest
from mysql_merge.config import default_mapping, ignore_unlisted, verbose

qs = ""


class MiniLogger(object):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(MiniLogger, cls).__new__(
                cls, *args, **kwargs)
        return cls._instance

    def __set__(self, k, v):
        if k == "qs":
            qs = v
        setattr(self, k, v)

    def log(self, v):
        if verbose:
            print v


def lists_diff(a, b):
    b = set(b)
    return [aa for aa in a if aa not in b]


def create_connection(connection, common_data={}):
    try:
        data = dict(common_data)
        data.update(connection)

        return MySQLdb.connect(data['host'], data['user'], data['password'], data['db'],
                               cursorclass=MySQLdb.cursors.DictCursor)

    except Exception, e:
        handle_exception("Exception on connecting to the database", e, )


logger = MiniLogger()


def handle_exception(custom_message, exception, connection=None):
    print ""
    print "-----------------------------------------------"
    print custom_message
    if qs:
        print "Last query was: "
        print qs
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


def map_fks(db_map, force_input=True):
    pks = dict((table_name, table_data['primary'].keys()[0]) for table_name, table_data in db_map.items() if len(
        table_data['primary'].keys()) > 0)
    tables = pks.keys()
    ignores = {}
    local_ignore_unlisted = False
    processed_colstrings = []
    remembered_choices = {}

    for table_name, table_map in db_map.items():
        for col_name, col_data in table_map['fk_maybe'].items():
            col_string = '%s.%s' % (table_name, col_name)

            # Make sure we're not forcing a duplicate entry
            if col_string in processed_colstrings:
                continue
            processed_colstrings.append(col_string)

            # Use default mapping for this column if there is one
            if default_mapping.has_key(col_string):
                val = default_mapping[col_string]
                if val is None:
                    ignores[col_string] = None
                    continue
                parent, parent_col = val.split(".")
            # Ignore this column if that's what's in config
            elif ignore_unlisted:
                continue

            # Force input otherwise
            elif force_input:
                if not remembered_choices.has_key(col_name):
                    choice = ""
                    col_table_name = "_".join(col_name.split("_")[:-1])
                    guess = levenshtein_lowest(col_table_name, tables)

                    while True:
                        print "Looks like a column: `%s`.`%s` could be a foreign key" % (table_name, col_name)
                        print "However no constraint is defined. What would you like to do?"
                        print ""
                        print "[1] Specify target table and column"
                        print "[2] Use the guess: %s.%s" % (guess, pks[guess])
                        print "[3] Ignore this column"
                        print "[4] Ignore all remaining columns"
                        choice = raw_input("-> ")
                        print ""
                        print ""
                        if choice in ["1", "2", "3", "4"]:
                            break
                        else:
                            print '"%s" is not a valid choice' % choice
                            print ""

                    s_cont = s_bre = False
                    if choice == "3":
                        ignores[col_string] = None
                        s_cont = True

                    elif choice == "4":
                        local_ignore_unlisted = True
                        s_bre = True

                    elif choice == "2":
                        parent = guess
                        parent_col = pks[guess]

                    elif choice == "1":
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
                                        assert parent_col in [column for table in db_map.values() for column in
                                                              table['columns'].keys()]
                                        break
                                    except AssertionError:
                                        print "There is no such column in that table"
                                except AssertionError:
                                    print "There is no such table"
                            except Exception, e:
                                print "Incorrect value!"

                    print "Should I remember this choice? (Y/n)"
                    remember = raw_input("-> ")
                    if not len(remember) or remember in ["y", "Y"]:
                        if choice == "3":
                            remembered_choices[col_name] = "!"
                        elif choice == "4":
                            remembered_choices[col_name] = "@"
                        else:
                            remembered_choices[col_name] = (parent, parent_col)
                        print "Choice remembered!"
                        print ""
                        print ""
                else:
                    if remembered_choices[col_name] == "!":
                        s_cont = True
                    elif remembered_choices[col_name] == "@":
                        s_bre = True
                    else:
                        parent, parent_col = remembered_choices[col_name]

                if s_cont:
                    continue

                if s_bre:
                    break

            default_mapping[col_string] = "%s.%s" % (parent, parent_col)
            db_map[table_name]['fk_create'][col_name] = {'parent': parent, 'parent_col': parent_col}
            del db_map[table_name]['fk_maybe'][col_name]
        else:
            continue
        break

    if force_input:
        print ""
        print "Here's your mapping just in case something goes wrong later"
        print "You can paste it to config.py to let this script know about your choices"
        if local_ignore_unlisted == True:
            print "ignore_unlisted = True"

        print "default_mapping = {"
        anything_printed = False
        for table_name, table_data in db_map.items():
            for column_name, column_data in db_map[table_name]['fk_create'].items():
                print "  '%s.%s': '%s.%s', " % (
                table_name, column_name, column_data['parent'], column_data['parent_col'])
                anything_printed = True
        #for table_string in ignores.keys():
        #  print "  '%s': None, " % (table_string)

        print "}"
        print ""

        if anything_printed:
            choice = ""
            while True:
                print "HINT: If there are less items here than in the config, you are probably running this"
                print "      script for the second or nth time and all the FKs are already mapped. It's okay"
                print "      and you can calmly confirm the list is valid."
                print "Is it valid?"
                print "[1] Yes"
                print "[2] No"
                choice = raw_input("-> ")
                if choice not in ["1", "2"]:
                    print "It's not a valid choice"
                else:
                    if choice == "2":
                        print "Boooo! Please run the script again or modify the mapping as you like to and paste it to config.py!"
                        sys.exit()
                    break
