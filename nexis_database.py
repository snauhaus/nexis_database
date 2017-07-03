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

class NexisDatabase(object):
    """docstring for NexisDatabase
    
    An object that allows python to interact with a database of nexis articles
    as output by the script lexisparse
    
    """
    def __init__(self, file_name='Nexis Articles.db', primary_key=str()):
        self.__name__ = file_name
        self.primary_key=primary_key
        
    def read_data(self, file):
        file_con = open(file, 'r')
        text = file_con.read()
        return(text)
    
    def connect(self):
        """docstring for make_database"""
        self.con = sqlite3.connect(self.__name__)
        self.c = self.con.cursor()
        
    def close(self):
        """docstring for close"""
        self.c.close()
    
    def create_table(self, table_name, primary_key, key_type="TEXT"):
        """Create a table with one primary key column
        
        """
        db_name=self.__name__
        self.primary_key=primary_key
        self.c.execute('CREATE TABLE {tn} ({nf} {ft} PRIMARY KEY)'\
                .format(tn=table_name, nf=primary_key, ft=key_type))
        self.con.commit()
        
    def set_primary_key(self, primary_key):
        """Retrieve name of primary key if provided
        
        """
        self.primary_key=primary_key
        
    def add_column(self, table_name, new_column, column_type="TEXT"):
        """Add column to table"""
        self.c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}"\
                .format(tn=table_name, cn=new_column, ct=column_type))
        self.con.commit()
        
    def insert_data(self, table_name, *data, commit=True):
        """docstring for insert

        This could be tricky if text contains special characters, or commas perhaps even...
        """
        columns=self.get_column_names(table_name)
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
        with open(csv_file, 'r') as f:
            reader = csv.reader(f)
            
            with progressbar.ProgressBar(max_value=len(data)) as bar:
                for i, row in enumerate(reader):
                    if i == 1:
                        if create_table is True:
                            header=row
                            self.create_table(table_name, header[0])
                            for h in header[1:]:
                                self.add_column(table_name, h)
                        else:
                            bar.update(i)
                            next
                    self.insert_data(table_name, *row, commit=False)
                    bar.update(i)

    def execute(self, command):
        """Execute command in db
        
        """
        self.c.execute(command)
    
    def select(self, table_name):
        """docstring for select"""
        self.c.execute('SELECT * FROM {tn}'.\
                format(tn=table_name))
        all_rows = self.c.fetchall()
        return all_rows
       
    def count_rows(self, table_name, print_out=False):
        """ Returns the total number of rows in the database

        """
        self.c.execute('SELECT COUNT(*) FROM {}'.format(table_name))
        count = self.c.fetchall()
        if print_out:
            print('\nTotal rows: {}'.format(count[0][0]))
        return count[0][0]
    
    def column_info(self, table_name):
        """ Returns a list of tuples with column informations:
            (id, name, type, notnull, default_value, primary_key)
        """
        self.c.execute('PRAGMA TABLE_INFO({})'.format(table_name))
        info = [list(tup) for tup in self.c.fetchall()]
        print("\nColumn Info:\n{:10s}{:10s}{:10s}{:10s}{:10s}{:10s}"\
               .format("ID", "Name", "Type", "NotNull", "DefaultVal", "PrimaryKey"))

        for col in info:
            print_text=tuple(str(t) for t in col)
            print('{:10s}{:10s}{:10s}{:10s}{:10s}{:10s}'.format(*print_text))
    
    def get_column_names(self, table_name):
        """returns columns in table"""
        self.c.execute('PRAGMA TABLE_INFO({})'.format(table_name))
        names = [tup[1] for tup in self.c.fetchall()]
        return names
    
    def values_in_col(self, table_name, print_out=True):
        """ Returns a dictionary with columns as keys and the number of not-null
            entries as associated values.
        """
        self.c.execute('PRAGMA TABLE_INFO({})'.format(table_name))
        info = self.c.fetchall()
        col_dict = dict()
        for col in info:
            col_dict[col[1]] = 0
        for col in col_dict:
            self.c.execute('SELECT ({0}) FROM {1} WHERE {0} IS NOT NULL'.format(col, table_name))
            # In my case this approach resulted in a better performance than using COUNT
            number_rows = len(self.c.fetchall())
            col_dict[col] = number_rows
        if print_out:
            print("\nNumber of entries per column:")
            for i in col_dict.items():
                print('{}: {}'.format(i[0], i[1]))  

    def pack_database(self, method="zip"):
        """docstring for pack_database

        Compress the sql database in same directory

        Defaults to zip, because its much faster than 7zip, although
        it also achieves a lower compression

        The zip method will delete the database after compression
        """
        filename=self.__name__
        if method=="7z":
            subprocess.call(['7z', 'a', filename+'.7z', filename])
        elif method=="zip":
            zipfile.ZipFile(filename+'.zip','w',compression=zipfile.ZIP_DEFLATED).write(filename)
            if zipfile.ZipFile(filename+'.zip','r').testzip() is None: os.remove(filename)

    def unpack_database(self, method="zip"):
        """docstring for unpack_database

        Decompress the sql database from 7z

        The zip method will delete the archive after decompression
        """
        if method=="7z":
            filename=self.__name__
            subprocess.call(['7z', 'x', filename, filename+'.7z'])
        elif method=="zip":
            filename=self.__name__
            zipfile.ZipFile(filename+'.zip').extract(filename)




