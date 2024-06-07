# Excel data processing project with Python

## Overview

The main scope of this project is to manipulate data from multiple excel files (168 files, with around 50 KB each) by applying a set of transformations, including merging files, new folder creation and data ingestion in a SQL Server database. <br>
Libraries used:
* OS module to work with directories and files 
* Pandas for data manipulation
* SQLAlchemy to create new tables and import them in the database 

## Data set

The data set represents consumer and business loans from the National Bank of Romania, broken down into multiple subcategories, in national currency and in other currencies (converted in RON). It is composed of 168 excel files with 44 columns and 55 or 56 rows each, depending from which year was the data. The data set can be found in this repository under "Original_files".

## To get started

1. Install the following libraries:
    ```python
    pip install pandas
    pip install sqlalchemy
    pip instal pyodbc

2. Save the Original files" attached in this repo in a directory and replace the following values: <br>
    * In the variable "root_dir" add the root of the directory in which you want this folder to be created. I recommend to choose the same directory in which "Original files" is saved in order to have everything in one place. This step must be done in order to create "Modified_files" and "Merged_files" directories automatically. Without these files, the code will not produce the end result. 
    * At step 9 replace "host" with your DBMS instance name and "database" with your database where you wish to load these files. You can use your DBMS default database in order to not bother with creating a new DB. Also, you might need to change the authenthication method according to your set up. For detailed instructions of how to set up a connection according to your DBMS with SQLAlchemy go to: https://docs.sqlalchemy.org/en/20/core/engines.html 


## Transformations

```python
# Directory paths:

# Data source path
original_files = '<your file path>\Original files'

# Root directory path
root_dir = '<same root as the one from original_files>'

# "Modified files" directory path
modified_files = root_dir + "\Modified files"

# "Merged files" directory path
merged_files = root_dir + "\Merged files"
```

<br>

1. If "Modified files" directory is not created, create a new directory. <br>
If "Merged files" directory is not created, create a new directory. <br>
If subdirectories are not created in the "Modified files", create a copy from the data source.

```python
for root in os.walk(root_dir): #create "Modified_files" 
    while os.path.exists(modified_files) == False:
        os.mkdir(modified_files)
    else:
        continue

for root in os.walk(root_dir): #create "Merged_files"
    while os.path.exists(merged_files) == False:
        os.mkdir(merged_files)
    else:
        continue


for root, dirs, files in os.walk(original_files): #create subdirectories in "Modified_files" directory
    for dir in dirs:
        new_directory_path = os.path.join(modified_files, dir)  # new subdirectories paths
        while os.path.exists(new_directory_path) == False: # if directory doesn't exist, create new directories in "Modified files"
            os.mkdir(new_directory_path)
        else:
            continue
````
<br>

2. Tables before 2009 (including) have 4 rows before table header while tables from 2010 onward have 3 rows before the header. Delete first rows according to the reporting period. 

3. Delete last row and first and last column of each table   

4. Transpose each table.

5. Add a new column with the date and month as per file name after column 1

6. Replace column 1 header with "County"

7. Change sheet name with date and month of the report

````python
def modify_tables(original_file, modified_file):
    print('Process started')
    i = 0  # total nr of files modified
    j = 0  # total nr of files already modified
    for root, dirs, files in os.walk(original_file):
        for file in files:  # create an excel file path to loop through
            old_excel_path = os.path.join(root, file)
            file_name = os.path.relpath(old_excel_path, original_file) #ensure the subdirectory of each excel file is correct
            new_excel_path = os.path.join(modified_file, file_name[0:13] + "modified.xlsx")
            year = file[2:6]  # create a string based on file name to process data based on the file format.
            date_column = file[2:8]  # get date from file name
            if os.path.isfile(new_excel_path) == True:  # check if the file already exists
                j += 1
                if j == 168:
                    print("All files already exists")
                    break
            else:
                if file.endswith('xlsx') or file.endswith('xlsm'):
                    year = file[2:6]
                    if year.isdigit():
                        year = int(year)
                    else:
                        continue
                    if int(year) < 2010:  # get rid of the first 3/4 rows using header parameter and last row
                                    # get rid of first and last column range B:AQ
                        df1 = pd.read_excel(old_excel_path, header=4, index_col=0, usecols='B:AQ', engine='openpyxl')
                        df1.to_excel(new_excel_path)
                        df1_transposed = df1.transpose() #transpose each file
                        df1_transposed.insert(  # insert a date column
                            loc=0,
                            column='Date',
                            value= date_column
                        )
                        df1_transposed.to_excel(new_excel_path, sheet_name=file[2:8], index_label='County')
                        i += 1  # count one file modified
                        print(f'file saved in: {new_excel_path}')  # notify when each file is saved
                    else:
                        df2 = pd.read_excel(old_excel_path, header=3, index_col=0, usecols='B:AQ', engine='openpyxl')
                        df2_transposed = df2.transpose()
                        df2_transposed.insert(  # insert a date column
                            loc=0,
                            column='Date',
                            value= date_column
                        )
                        df2_transposed.to_excel(new_excel_path, sheet_name=file[2:8], index_label='County')
                        i += 1  # count one file modified
                        print(f'file saved in: {new_excel_path}')  # notify when each file is saved

    print(f"Process finished. Total of: {i} files have been modified")  # notify when the entire process is finished

modify_tables(original_files, modified_files) #Run "modify_tables" function

````

7. Merge all files pertaining to each subdirectory into a single one and rename each file as "merged+report year"
8. From 2010 onwards, data has been scaled by 1M . To make the data liniar across all tables, multiply all the values from col C to BB by 1,000,000
9. Replace the sheet name with the year of the report


````python

dir_list = os.listdir(modified_files)  # list all the subdirectories from directory 'Modified files'.
def append_files(mod_files, dir_list, merged_files):
    print("File merging process started")
    i = 0 #count the number of files that already exist
    j = 0  # count the number of files saved
    
    for dir in dir_list:
        excl_list = []  # will be used as a list of all excels for each subdirectory iteration
        subdir_path = os.path.join(mod_files, dir)  # path of each subdirectory
        merged_file_path = os.path.join(merged_files, f"merged_f{dir}.xlsx")  # new exce path for merged excls
        if os.path.isfile(merged_file_path) == True: #check if merged file already exist
            i += 1
            if i == 12:  # if al the files exist, break the loop and print message
                print("All files already exists")
                break
        else:
            files = glob.glob(subdir_path + "/*.xlsx")  # get all files which end in xlsx from each subdirectory
            for file in files:  # for each excel file in each subdirectory, read and append them in the excl_list
                excl_list.append(pd.read_excel(file))
            excel_merged = pd.concat(excl_list, ignore_index=True)  # merge all the excels from the list

            if int(dir) > 2009:  # if subdirectory is from 2010 onwards, after appending, multiply values by 1M
                excel_merged.iloc[:, 2:len(excel_merged.columns)] = excel_merged.iloc[:,
                    2:len(excel_merged.columns)] * 1000000
                excel_merged.to_excel(merged_file_path, sheet_name=dir, index=False, engine='openpyxl')
                j += 1
                print(f'File {merged_file_path} has been saved.'.format(file))
            else:  # else, just append
                excel_merged.to_excel(merged_file_path, sheet_name=dir, index=False, engine='openpyxl')
                j += 1
                print(f'File {merged_file_path} has been saved.'.format(file))

    print(f'File merging process finished. Total of: {j} files have been merged and saved')

append_files(modified_files, dir_list, merged_files) #Run "append_files" function

````


9. Connect to the DB, create tables and insert all tables.

````python
url_object = URL.create(
    'mssql+pyodbc',
    host='ALEXANDRUPC\SQLSERVER2022',
    database='tempdb',
    query={
        "driver": "ODBC Driver 18 for SQL Server",
        "TrustServerCertificate": "yes",
        "authentication": "ActiveDirectoryIntegrated",
    },
)

engine = create_engine(url_object)

metadata_obj = MetaData() #create metadata object for table creation

for root, dirs, files in os.walk(merged_files):
    for excel in files:
        excel_path = os.path.join(root, excel)  # create excel path
        table_name = os.path.basename(excel_path)[:12]  # use file names as table names
        df = pd.read_excel(excel_path)
        columns_1 = [Column(column_name, String) for column_name in df.columns[:3]] #from each excel, get column name from 0 to 53 and assign data type
        columns_2 = [Column(column_name, Integer) for column_name in df.columns[3:53]]
        table = Table(table_name, metadata_obj, *columns_1, *columns_2)
        metadata_obj.create_all(engine)
        with engine.connect() as conn:  # with statement ensure the connection to the DB is closed after file loading
            df.to_sql(
                    table_name,
                    conn,
                    if_exists='replace',
                    index=False)  # write records stored in df in the SQL table defined
print(f"All 12 files have been loaded in the database")

````
