Nexis database
==============
Create sqlite database from nexis articles preprocessed using [lexisparse](https://github.com/FreshRamen/lexisparse).

Provides functions and handlers to interact with the database.


## Example

    database = n.NexisDatabase(file_name='Nexis Articles.db')
    database.connect()

    ## Add articles
    database.insert_text_files("Documents", lexisparse_output_path)