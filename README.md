mysql_merge
=============================

This small script allows you to merge two mysql databases (or more). 
As a result you'll have your data copied into specified database.

It solves some important problems with data integrity across different databases like:
* Primary Keys conflict
* Keeping Foreign Keys up to date (sometimes even those not marked as foreign keys in the database schema!)
* Dealing with conflicting unique indexes
* Minor schema differences between databases
* ... and a few more

###To use:
1. clone this repo (obviously)
2. copy file mysql_merge/config.py.example mysql_merge/config.py
3. setup your database details
4. *make sure your destination db already have schema loaded - this script does not transfer a schema!*
5. run as: python run.py

*This script will change some of your PK's. It will rollback all changes in case of any error, but you should backup all your data before using it anyway!*
*While PK values will not remain the same, all foreign keys and looks-like-it's-FK columns will be updated accordingly*

###What it does in details:
1. Maps all tables, columns, relations and unique/primary indexes
2. If some FK-like columns are found (*_id) but they are not marked as PK - user is asked to specify appropriate mappings
3. Change all tables to InnoDB
4. Alter all tables and modify FOREIGN KEYS to ON UPDATE CASCADE
5. Update all numerical PKs to PK + iteration_nb * 1000000 ( so they don't conflict in the destination database )
6. Detect which unique values conflicts with data in the destination db
7. Update PKs on those rows to corresponding PKs from the destination db
8. Copy data from all tables to the destination db
9. Rolls back 5th step

###Limitations:
* Completely not intended to work with composite primary/foreign key other than M2M intermediary tables
* It won't rollback changes from step 7
* Non-FK values won't change their value (like non-fk column user_id that wasn't specified in step 2; all simulated-fks)
* Conflicting non-numeric PKs will stop the script with an appropriate error message
* MyISAM tables will be converted to InnoDB, script won't continue on failure
* This is not meant to deal with any triggers, please turn them off
* ... probably there are more :)
