"""
Script makes an SQLite3 database from nexis files pre-processed by lexisparse

Provides functions and handlers to interact with the database

"""
import os
import sqlite3
import progressbar
import subprocess
import zipfile, zlib
import csv
import pandas
import numpy


class dbORM(object):
    """   
    An object that allows python to interact with a database of nexis articles
    
    Primarily to store articles cleaned by lexisparse
    
    But it could be used with any other large folder of text files
    
    """
    def __init__(self, file_name='Nexis Articles.db'):
        self.__name__ = file_name
    
    """
    Basic database interactions.    
    
    """
        
    def connect(self, check_packed=True):
        """Connect to a database.
        Creates connection object (con) and cursor (c)
        
        If check_packed is True, dbORM will check first if there is a zipped db object with the same name and unpack if true.
        """
        
        if check_packed:
            zip_name = self.__name__ + ".zip"
            if not os.path.isfile(self.__name__) and os.path.isfile(zip_name):
                print("Unpacking existing database:",zip_name)
                self.unpack()
        self.con = sqlite3.connect(self.__name__)
        self.c = self.con.cursor()
        # print("Connection established")
        
    def close(self):
        """Close database connection"""
        self.c.close()
        
    def execute(self, command, commit=False):
        """Execute a command
        
        """
        self.c.execute(command)
        if commit:
            self.commit()
    
    def commit(self):
        """Commit to database
        
        """
        self.con.commit()
        
    def fetch(self, what = "all", size=None):
        """Fetch data from database.
        What can be "ALL", "MANY", or "ONE". Defaults to ALL
        
        """
        if what.upper() == "ALL":
            return self.c.fetchall()
        elif what.upper() == "MANY":
            if size is not None:
                return self.c.fetchmany(size)
            else:
                return self.c.fetchmany()
        elif what.upper() == "ONE":
            return self.c.fetchone()
        else:
            print("what must be element of 'all', 'many' or 'one'.")
    
    def drop_table(self, table_name):
        """
        Shorthand for dropping a table.
        Be careful with this.
        
        """
        cmd="DROP TABLE {}".format(table_name)
        self.execute(cmd)
        self.commit()
    
    
    """
    Miscellaneous functions    
        
    """
            
    def read_data(self, file):
        """
        Read a text file from disk
        
        """
        file_con = open(file, 'r')
        text = file_con.read()
        return(text)
    
    
    """
    Adding new tables        
            
    """
    
    def create_table(self, table_name, col_names, col_types=None, col_constraints=None, other_columns=None, overwrite=False):
        """Create a table in the database
        col_names must be provided
        col_types defaults to TXT
        col_constraints defaults to 
        other_columns can manually specifiy columns 
        
        Example usage: 
        
            db.create_table('Sentiments', col_names = ["File", "Paragraph", "Text", "Sentiment"], col_types = ["TXT", "INT", "TXT", "INT"], other_columns = "PRIMARY KEY (File, Paragraph)")
            
        """
        if overwrite and table_name in self.list_tables():
            self.drop_table(table_name)
        ncols = len(col_names)
        if col_types is None:
            col_types = list(numpy.repeat("TXT", ncols))
        if col_constraints is None:
            col_constraints = list(numpy.repeat("", ncols))
        query = [' '.join([cn, cp, cc]) for cn, cp, cc in zip(col_names, col_types, col_constraints)]
        query = "CREATE TABLE {} (".format(table_name) + ', '.join(query)
        if other_columns is not None:
            query = ', '.join([query, other_columns])
        query = query + ")"
        self.execute(query)
        self.commit()
    
    def insert_text_files(self, table_name, files_path, create_table=True):
        """Adds many text files into a table in the 
        database, using file name as ID
        
        table_name = table where to add the files
        files_path = directory with text files
    
        """
        if create_table is True:
            self.create_table(table_name, "File")
            self.add_column(table_name, "Text")
        all_files = os.listdir(files_path)
        txt_files = [f for f in all_files if ".txt" in f or ".TXT" in f]
        with progressbar.ProgressBar(max_value=len(txt_files)) as bar:
            for i, f in enumerate(txt_files):
                file_path = os.path.join(files_path, f)
                article_text = self.read_data(file_path)
                data = [str(f), str(article_text)]
                self.insert_data(table_name, str(f), str(article_text), commit=False)
                bar.update(i)
        self.con.commit()
    
    def insert_csv(self, table_name, csv_file, create_table=True):
        """Add CSV file to a table in the database

        """
        if create_table: # Ensure first columns parsed as primary key
            with open(csv_file, 'r') as f:
                reader = csv.reader(f)
                header=reader.__next__()
                self.create_table(table_name, header[0])
                for h in header[1:]:
                    self.add_column(table_name, h)
        df = pandas.read_csv(csv_file)
        df.to_sql(table_name, self.con, if_exists='append', index=False)
        # num_lines = len(open(csv_file, 'r').readlines())
        #     else:
        #         reader.__next__()
        #     with progressbar.ProgressBar(max_value=num_lines) as bar:
        #         for i, row in enumerate(reader):
        #             print(*row)
        #             self.insert_data(table_name, *row, commit=False)
        #             bar.update(i)
        # with open('data.csv','rb') as fin: # `with` statement available in 2.5+
        #     # csv.DictReader uses first line in file for column headings by default
        #     dr = csv.DictReader(fin) # comma is default delimiter
        #     to_db = [(i['col1'], i['col2']) for i in dr]
        self.con.commit()
    
    def insert_pandas(self, table_name, df, overwrite=False):
        """Inserts Pandas DataFrame object to a new or existing table
        
        Use create_table() first if column flags or so need to be set.
        
        If overwrite is True, overwrites existing table
        """
        if overwrite:
            try:
                self.drop_table(table_name)
            except:
                print("No existing table found")
        df.to_sql(table_name, self.con, if_exists='append', index = False)
        self.commit()
    
    
    """
    Altering tables    
        
    """
        
    def add_column(self, table_name, new_column, column_type="TEXT"):
        """Add column to table"""
        self.c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}"\
                .format(tn=table_name, cn=new_column, ct=column_type))
        self.con.commit()
        
    def insert_data(self, table_name, *data, commit=True):
        """Insert a row of data into database table

        """
        columns=self.list_columns(table_name)
        len_data = len(data)
        column_names=', '.join(columns)
        q_signs = ', '.join("?"*len_data)
        com="INSERT INTO {tn} ({cn}) VALUES ({dt})".\
                    format(tn=table_name, cn=column_names, dt=q_signs)
        if len_data != len(columns):
            print("Not enough values provided")
        else:
            try:
                self.c.execute(com, data)
            except sqlite3.IntegrityError:
                print('ERROR: ID already exists in PRIMARY KEY column {}'.format(id_column))
            if commit is True:
                self.con.commit()
   
    def get_paragraph_count(self, table_name, col_name="Text", print_out=True):
        """Function returns the number of paragraphs per 'Text' in 'Documents'
        
        """
        num_docs = self.count_rows(table_name)
        cmd="SELECT {} FROM {} LIMIT ".format(col_name,table_name)
        paras = []
        with progressbar.ProgressBar(max_value=num_docs) as bar:
            for i in range(1,num_docs+1):
                self.execute(cmd+str(i))
                text = self.c.fetchall()
                pars = text[0][0].count('\n\n')+1
                paras.append(pars)
                bar.update(i)
        return(paras)
    

    """
    Selecting data        
            
    """
            
    def select(self, what, where, fetch=None, arguments=None):
        """Select query to table
        
        Fetch is optional, can be either 'all', 'first' or 'many'
        
        Optional arguments can be passed via `arguments`
        
        Returns nothing if fetch is None (default)
        """
        query = 'SELECT {} FROM {}'.format(what, where)
        if arguments is not None:
            query = query + " " + arguments
        self.execute(query)
        if fetch is not None:
            res = self.fetch(fetch)
            return res
    
    def select_like(self, what, where, like):
        """Select entire table where a specific column contains text"""
        cmd="SELECT * FROM {} WHERE {} LIKE '%{}%'".format(where, what, like)
        self.execute(cmd)
        result = self.fetch()
        return result
    
    def get_pandas(self, table, arguments=None, chunksize=None):
        """Return a database table as pandas dataframe
        
        Optional arguments can be passed via `arguments`
        """
        query = "SELECT * FROM {}".format(table)
        if arguments is not None:
            query = query + " " + arguments
        df = pandas.read_sql_query(query, self.con, chunksize=chunksize)
        return df
    
    """
    Database info / statistics    
        
    """
    
    def list_tables(self):
        """List tables in database
        
        Returns list
        """
        query="SELECT name FROM sqlite_master WHERE type='table';"
        self.execute(query)
        output = self.fetch()
        tables = [t[0] for t in output]
        return tables
    
    def list_columns(self, table):
        """List columns in table
        
        """
        query='PRAGMA TABLE_INFO({})'.format(table)
        self.execute(query)
        output = self.fetch()
        columns = [tup[1] for tup in output]
        return columns
    
    def pragma(self, table):
        """Full pragma output for table
        
        Prints table with column information
            (id, name, type, notnull, default_value, primary_key)
        
        Returns nothing
        """
        query='PRAGMA TABLE_INFO({})'.format(table)
        self.execute(query)
        output = self.fetch()
        info = [list(tup) for tup in output]
        print("\nColumn Info:\n{:10s}{:25s}{:10s}{:10s}{:12s}{:10s}"\
               .format("ID", "Name", "Type", "NotNull", "DefaultVal", "PrimaryKey"))
        for col in info:
            print_text=tuple(str(t) for t in col)
            print('{:10s}{:25s}{:10s}{:10s}{:12s}{:10s}'.format(*print_text))
    
    def column_info(self, table):
        """Summary information for columns in table
        
        Prints table with some pragma information plus actual not null count
        
        Returns nothing
        """
        query = 'PRAGMA TABLE_INFO({})'.format(table)
        self.execute(query)
        info = self.fetch()
        info = [list(i)[0:3] for i in info] # Only ID, Name, Type
        columns = [i[1] for i in info] # Extract columns
        for i, col in enumerate(columns):
            count = self.count_notnull(col, table)
            info[i].append(count)    
        print("\nColumn Info:\n{:10s}{:25s}{:10s}{:10s}"\
               .format("ID", "Name", "Type", "NotNull"))
        for col in info:
            print_text=tuple(str(t) for t in col)
            print('{:10s}{:25s}{:10s}{:10s}'.format(*print_text))
       
    def count(self, what, where):
        """Count number of rows
        
        returns int

        """
        query = "SELECT COUNT({}) FROM {}".format(what, where)
        self.execute(query)
        count = self.fetch()
        return int(count[0][0])
    
    def count_distinct(self):
        """Count distinct entries
        
        Returns int
        """
        query = "SELECT COUNT(DISTINCT {}) FROM {}".format(what, where)
        self.execute(query)
        count = self.fetch()
        return int(count[0][0])
    
    def count_notnull(self, what, where):
        """Count non-null entries in column
        
        Returns int
        """
        query='SELECT COUNT({0}) FROM {1} WHERE {0} IS NOT NULL'.format(what, where)
        self.execute(query)
        count = self.fetch()
        return int(count[0][0])
        
    def count_like(self, what, where, like):
        """Count number of rows containing text (`like`)
        
        Returns int
        """
        cmd="SELECT COUNT({}) FROM {} WHERE {} LIKE '%{}%'".format(what, where, what, like)
        self.execute(cmd)
        count =self.fetch()
        return count[0][0]            
    
    def count_articles(self, like):
        """Count articles matching text (`like`)
        
        Shorthand function for count_like() with what='Text' and 
        where='documents'
        
        Returns int
        """
        result = self.count_like(like=like, what="Text", where="Documents")
        return result
    
    """
    Storing the database            
                
    """
    
    def pack(self):
        """Compress the sql database in same directory and close connection
        Deletes the original db after successful compression
        """
        filename = self.__name__
        zipfile.ZipFile(filename + '.zip', 'w', compression=zipfile.ZIP_DEFLATED).write(filename)
        if zipfile.ZipFile(filename+'.zip','r').testzip() is None:
            self.close()
            os.remove(filename)

    def unpack(self):
        """Decompress the sql database from 7z

        """
        filename=self.__name__
        zipfile.ZipFile(filename+'.zip').extract(filename)



NexisDatabase = dbORM # For backwards compatibility