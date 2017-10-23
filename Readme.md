Nexis database
==============
Create SQlite3 database from nexis articles preprocessed using [lexisparse](https://github.com/FreshRamen/lexisparse).

Provides functions and handlers to interact with the database.

I originally wrote the module to add cleaned files to an SQLite database (which doesn't necessarily save space but is much easier to work with – while also being easier on file-indexing services). It has since turned into a halfway functional ORM for SQLite. 


## Example
	import nexis_database as n
    db = n.dbORM(file_name='Nexis Articles.db')
    db.connect()

    ## Add articles
    db.insert_text_files(table="Documents", files_path =lexisparse_output_path)
    
    ## List tables
    db.list_tables()
    
    ## List pragma table info
    db.pragma(table="Documents)
    
    ## Close connection
    db.close()
