import pandas as pd
from datetime import datetime
import numpy as np
import time

"""read from a file, in this case read_file_name is always set to unfiltered.csv
returns a dataframe (df)
"""
def read(read_file_name):
    #read file and create a dataframe (df)
    #if unfiltered.csv is empty, pandas will raise exception and keep the microservice trying in a forever TRUE while LOOP
    df = pd.read_csv(read_file_name, index_col=0)


    print("Reading file: ", read_file_name, "...")

    #give the df an index column
    df = df.reset_index()

    #pandas will replace any empty or null cells with NaN object, which is uncomputable in python
    #hence we have to replace NaN with "null" in all cells in the dataframe (df)
    #here we use numpy module to handel NaN object; np.nan refers to NaN
    #inplace=True means all NaN objects are replaced in their original cells
    df.replace(np.nan, "null", inplace=True)

    print("File read done.")
    return(df)

"""takes a dataframe (df) and write to a file
in this case write_file_name is always set to filter.csv
"""
def write(df, write_file_name):
    print("Writing file: ", write_file_name, "...")

    #write to file "filter.csv" with the defined columns and exclude index column (index = False)
    df.to_csv(write_file_name, columns=['File Name','Size (MB)', 'Date Modified', 'File Format', 'File Path'], index=False)

    print("File write done.")
    return

"""takes a dataframe (df) and filter out all unmatched rows
returned a filtered dataframe (df)
"""
def filter(df):
    print("Filtering...")

    #extract filter params from the first row under the column headers (row index = 0)
    #pandas syntax is df.loc[row index].at[column name]
    name_filter = df.loc[0].at['File Name']
    size_filter = df.loc[0].at['Size (MB)']
    datemodified_filter = df.loc[0].at['Date Modified']
    format_filter = df.loc[0].at['File Format']

    print("Filters: name: ", name_filter,", size: ", size_filter,", date modified: ", datemodified_filter,", format: ", format_filter)

    #drop the filter row in dataframe, then reset dataframe's index (drop = True means replace index and don't add an additional index column)
    df = df.drop(0)
    df = df.reset_index(drop=True)

    #filter by name
    if name_filter != "null":
        #iterate through all rows. len(df.index) gives the total number of rows in dataframe
        for count in range(0, len(df.index)):
            if df.loc[count].at['File Name'] != name_filter:
                df = df.drop(index=count)
        df = df.reset_index(drop=True)

    #filter by size
    if size_filter != "null":
        #extract sign and size limit from size_filter. The first character is the sign, follow by space, the float starts at the 3rd character (index 2)
        sign = size_filter[0]
        size_limit = float(size_filter[2:])
        #iterate through all rows. len(df.index) gives the total number of rows in dataframe
        for count in range(0, len(df.index)):
            file_size = float(df.loc[count].at['Size (MB)'])
            if sign == ">":
                if file_size <= size_limit:
                    df = df.drop(index=count)
            elif sign == "<":
                if file_size >= size_limit:
                    df = df.drop(index=count)
        df = df.reset_index(drop=True)

    #fileter by datemodified
    if datemodified_filter != "null":
        # extract sign and size limit from size_filter. The first character is the sign, follow by space, the float starts at the 3rd character (index 2)
        sign = datemodified_filter[0]
        #date format must always be mm/dd/yyyy hh:mm:ss
        datetime_limit = datetime.strptime(datemodified_filter[2:], '%m/%d/%Y %H:%M:%S')

        for count in range(0, len(df.index)):
            datetime_object = datetime.strptime(df.loc[count].at['Date Modified'], '%m/%d/%Y %H:%M:%S')
            if sign == ">":
                if datetime_object <= datetime_limit:
                    df = df.drop(index=count)
            elif sign == "<":
                print("compare", datetime_object, "<", datetime_limit)
                print(datetime_object>=datetime_limit)
                if datetime_object >= datetime_limit:
                    df = df.drop(index=count)
        df = df.reset_index(drop=True)

    #filter by file format
    if format_filter != "null":
        # iterate through all rows. len(df.index) gives the total number of rows in dataframe
        for count in range(0, len(df.index)):
            if df.loc[count].at['File Format'] != format_filter:
                df = df.drop(count)
        df = df.reset_index(drop=True)

    return df

"""empty unfiltered.csv
this function is called last in this microservice to make unfiltered.csv empty and ready to be read again
"""
def empty_csv(read_file_name):
    #empty unfiltered.csv
    f = open(read_file_name, "w")
    #empty the content by truncate it to 0 byte
    f.truncate()
    f.close()
    print("unfiltered.csv emptied")
    return

if __name__ == "__main__":
    read_file_name = "unfiltered.csv"
    write_file_name = "filtered.csv"
    print("filter microservice online...")

    while True:
        time.sleep(5)
        try:
            #read unfiltered.csv
            df = read(read_file_name)
        except:
            #if exception is raised, e.g. unfiltered.csv is empty or doesn't exist, do nothing, continue while True loop
            pass
        else:
            #filter dataframe
            df = filter(df)
            #write the filtered dataframe to filtered.csv
            write(df, write_file_name)
            time.sleep(5)
            #empty unfiltered.csv to prepare it for the next call
            empty_csv(read_file_name)
