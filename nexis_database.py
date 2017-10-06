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
        print("Connection established")
        
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
        if overwrite:
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
            
    def select(self, table_name):
        """Select query to table"""
        query = 'SELECT * FROM {tn}'.format(tn=table_name)
        self.execute(query)
        # all_rows = self.c.fetchall()
        # return all_rows
    
    def select_where(self, table_name, col_name, match_string, limit = 5):
        """docstring for select_where"""
        cmd="SELECT * FROM {} WHERE {} LIKE '%{}%' LIMIT {}".format(table_name, col_name, match_string, limit)
        self.execute(cmd)
        result = self.fetch()
        return result
    
    def read_pandas(self, table_name, chunksize=None):
        """Read a table as a pandas data frame"""
        query = "SELECT * FROM {}".format(table_name)
        df = pandas.read_sql_query(query, self.con, chunksize=chunksize)
        return df
    
    """
    Database info / statistics    
        
    """
       
    def count_rows(self, table_name, print_out=False):
        """ Returns the total number of rows in the database

        """
        self.c.execute('SELECT COUNT(*) FROM {}'.format(table_name))
        count = self.c.fetchall()
        if print_out:
            print('\nTotal rows: {}'.format(count[0][0]))
        return count[0][0]
    
    def count_rows_where(self, table_name, col_name, match_txt=None):
        """Returns the total number of rows in the database containing `match_txt`
        
        """
        cmd="SELECT COUNT(*) FROM {} WHERE {} LIKE '%{}%'".format(table_name, col_name, match_txt)
        self.execute(cmd)
        count =self.fetch()
        return count[0][0]            
    
    def get_articles_count(self, match_txt=None, table_name="Documents", col_name="Text"):
        """Shorthand function for count_rows_where() function for articles containing some text"""
        result = self.count_rows_where(table_name, col_name, match_txt)
        return result
    
    def list_tables(self):
        """Returns list of all tables in database
        
        """
        self.c.execute("SELECT name FROM sqlite_master WHERE type='table';")
        print(self.c.fetchall())
    
    def column_info(self, table_name):
        """ Returns a list of tuples with column informations:
            (id, name, type, notnull, default_value, primary_key)
        """
        self.c.execute('PRAGMA TABLE_INFO({})'.format(table_name))
        info = [list(tup) for tup in self.c.fetchall()]
        print("\nColumn Info:\n{:10s}{:25s}{:10s}{:10s}{:12s}{:10s}"\
               .format("ID", "Name", "Type", "NotNull", "DefaultVal", "PrimaryKey"))

        for col in info:
            print_text=tuple(str(t) for t in col)
            print('{:10s}{:25s}{:10s}{:10s}{:12s}{:10s}'.format(*print_text))
    
    def list_columns(self, table_name):
        """returns a list of columns in the table"""
        self.execute('PRAGMA TABLE_INFO({})'.format(table_name))
        names = [tup[1] for tup in self.c.fetchall()]
        return names
    
    def column_value_count(self, table_name, print_out=True):
        """ Returns a dictionary with columns as keys and the number of not-null
            entries as associated values.
        """
        self.execute('PRAGMA TABLE_INFO({})'.format(table_name))
        info = self.fetch()
        col_dict = dict()
        for col in info:
            col_dict[col[1]] = 0
        for col in col_dict:
            self.execute('SELECT ({0}) FROM {1} WHERE {0} IS NOT NULL'.format(col, table_name))
            # In my case this approach resulted in a better performance than using COUNT
            number_rows = len(self.fetch())
            col_dict[col] = number_rows
        if print_out:
            print("\nNumber of entries per column:")
            for i in col_dict.items():
                print('{}: {}'.format(i[0], i[1]))  
    
    """
    Storing the database            
                
    """
    
    def pack(self):
        """Compress the sql database in same directory and close connection
        Deletes the original db after successful compression
        """
        zipfile.ZipFile(filename + '.zip', 'w', compression=zipfile.ZIP_DEFLATED).write(filename)
        if zipfile.ZipFile(filename+'.zip','r').testzip() is None:
            os.remove(filename)

    def unpack(self):
        """Decompress the sql database from 7z

        """
        filename=self.__name__
        zipfile.ZipFile(filename+'.zip').extract(filename)



NexisDatabase = dbORM # For backwards compatibility