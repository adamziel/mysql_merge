mysql_merge
=============================

This small script allows you to merge two mysql databases (or more) 
As a result you'll have your data copied into another database called "destination db";

It solves some important problems with data integrity across different databases like:
* Primary Keys conflict
* Keeping Foreign Keys up to date
* Moving conflicting unique indexes
... and a few more

###It solves:
* Conflicting Primary Keys
* Unique indexes
* Foreign keys

###To use:
1. clone this repo (obviously)
2. copy file mysql_merge/config.py.example mysql_merge/config.py
3. setup your database details
4. run as: python run.py

*It will rollback all changes in case of any error, but you should backup all your data before using it anyway!*
*PK values won't remain the same, but all foreign keys and looks-like-it's-FK columns will be updated accordingly*

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
* ... probably there are more :)
