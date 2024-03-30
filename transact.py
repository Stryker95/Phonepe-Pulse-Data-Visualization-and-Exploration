#!pip install psycopg2
#!git clone https://github.com/PhonePe/pulse.git

import plotly.express as px
from PIL import Image
import pandas as pd
import json
import os
import streamlit as st

#Once created the clone of GIT-HUB repository then,
#Required libraries for the program


#This is to direct the path to get the data as states
#Transaction data broken down by type of payment at state level.

#code 1
path1="C:/Users/HP/pulse/data/aggregated/transaction/country/india/state/"
agg_tran_list= os.listdir(path1)

columns1={"States":[],"Years":[],"Quarter":[], "Transaction_type":[],"Transaction_count":[], "Transaction_amount":[]}

for state in agg_tran_list:
    cur_states=path1+state+"/"
    agg_year_list=os.listdir(cur_states)

    for year in agg_year_list:
        cur_year=cur_states+year+"/"
        agg_file_list=os.listdir(cur_year)

        for file in agg_file_list:
            cur_file=cur_year+file
            data=open(cur_file,"r")
            
            A=json.load(data)
            
            for i in A["data"]["transactionData"]:
                name=i["name"]
                count=i["paymentInstruments"][0]["count"]
                amount=i["paymentInstruments"][0]["amount"]
                columns1["Transaction_type"].append(name)
                columns1["Transaction_count"].append(count)
                columns1["Transaction_amount"].append(amount)
                columns1["States"].append(state)
                columns1["Years"].append(year)
                columns1["Quarter"].append(int(file.strip(".json")))
                
agg_trans_state=pd.DataFrame(columns1)

import psycopg2
from psycopg2 import Error

#Transaction data broken down by type of payment at district level.
#code 1.1
# Establishing a connection to the PostgreSQL database
try:
    #This code establishes a connection to a PostgreSQL database 
    # using the psycopg2 library in Python
    connection = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Sanchit@1995",
        database="phonepe",
        port="5432"
    )

    
    #a cursor object is created using the cursor() method of the 
    # connection object. The cursor object is used to execute SQL 
    # queries and fetch results from the database
    cursor = connection.cursor()
    #States - VARCHAR(255) - A string representing the state.

    create_table_query = """
        CREATE TABLE IF NOT EXISTS payment_transact_districtlevel (
            States VARCHAR(255),
            Years INT,
            Quarter INT,
            Transaction_type VARCHAR(255),
            Transaction_count INT,
            Transaction_amount FLOAT
        )
    """
    cursor.execute(create_table_query)

    # Inserting data into the PostgreSQL database
    for index, row in agg_trans_state.iterrows():
        cursor.execute(
            '''INSERT INTO payment_transact_districtlevel
            (States, Years, Quarter, Transaction_type, 
            Transaction_count, Transaction_amount) 
            VALUES (%s, %s, %s, %s, %s, %s)''',
            (row['States'], row['Years'], row['Quarter'], 
             row['Transaction_type'], row['Transaction_count'], 
             row['Transaction_amount'])
        )
    #The INSERT statement inserts a new row into the 
    # payment_transact_dsitrictlevel table with the values 
    # from the current row of the DataFrame.
    #The %s placeholders in the VALUES clause are replaced 
    # with the corresponding values from the DataFrame row.

    connection.commit()
    #After all rows have been inserted, the commit() method 
    # is called on the connection to commit the transaction. 
    # This makes the changes permanent in the database.
except Error as e:
    print("Error while connecting to PostgreSQL", e)
    
#Once created the clone of GIT-HUB repository then,
#Required libraries for the program
#code 2 

import pandas as pd
import json
import os

#This is to direct the path to get the data as states
#'''Transaction data broken down by type of payment at country level year wise and quarter wise.'''

path3="C:/Users/HP/pulse/data/aggregated/transaction/country/india/"
agg_date_list= os.listdir(path3)
stateignore=os.listdir("C:/Users/HP/pulse/data/aggregated/transaction/country/india/state/")

columns4={"Years":[],"Quarter":[], "Transaction_type":[],"Transaction_count":[], "Transaction_amount":[]}

for year in agg_date_list:
    if year not in stateignore:
        cur_year = os.path.join(path3, year)
        for file in os.listdir(cur_year):
            cur_file = os.path.join(cur_year, file)
           #to remove PermissionError: [Errno 13] Permission denied: 'C:/Users/HP/pulse/data/aggregated/transaction/country/india/state\\andaman-&-nicobar-islands'
            try:
                with open(cur_file, "r") as data:
                    #Opening a file in read mode ('r') allows you to read the contents of the file without modifying it
                    C = json.load(data)
                    #When you read a JSON file using Python's json.load() function, you are essentially converting the JSON data from the file into a Python data structure
                    for i in C["data"]["transactionData"]:
                        name=i["name"]
                        count=i["paymentInstruments"][0]["count"]
                        amount=i["paymentInstruments"][0]["amount"]
                        columns4["Transaction_type"].append(name)
                        columns4["Transaction_count"].append(count)
                        columns4["Transaction_amount"].append(amount)
                    
                        columns4["Years"].append(year)
                        columns4["Quarter"].append(int(file.strip(".json")))
            except:
                    pass
aggre_country_year_quarter=pd.DataFrame(columns4)

import psycopg2
from psycopg2 import Error

#code 2.1
# Establishing a connection to the PostgreSQL database
try:
    connection = psycopg2.connect(
        host='localhost',
        database='phonepe',
        user='postgres',
        password='Sanchit@1995',
        port='5432'
    )

    cursor = connection.cursor()

    # Create table if it doesn't exist
    create_table_query = """
        CREATE TABLE IF NOT EXISTS country_transaction_data (
            Years INT,
            Quarter INT,
            Transaction_type VARCHAR(255),
            Transaction_count BIGINT,
            Transaction_amount FLOAT
        )
    """
    cursor.execute(create_table_query)

    # Insert data into the PostgreSQL database
    for index, row in aggre_country_year_quarter.iterrows():
        cursor.execute(
            '''INSERT INTO country_transaction_data
            (Years, Quarter, Transaction_type, Transaction_count, Transaction_amount) 
            VALUES (%s, %s, %s, %s, %s)''',
            (row['Years'], row['Quarter'], row['Transaction_type'], 
                row['Transaction_count'], row['Transaction_amount'])
        )
    connection.commit()

except Error as e:
    print("Error while connecting to PostgreSQL", e)



    
#Once created the clone of GIT-HUB repository then,
#Required libraries for the program
#code 3

import pandas as pd
import json
import os

#This is to direct the path to get the data as states
#'''Users data broken down by devices at country level and state wise.'''

path2="C:/Users/HP/pulse/data/aggregated/user/country/india/state/"
agg_user_list= os.listdir(path2)

columns2={"States":[],"Years":[],"Quarter":[], "Brand":[],"Count":[], "Percentage":[]}
columns3={"States":[],"Years":[],"Quarter":[],"Registered_users":[]}
for state in agg_user_list:
    cur_states=path2+state+"/"
    agg_year_list=os.listdir(cur_states)

    for year in agg_year_list:
        cur_year=cur_states+year+"/"
        agg_file_list=os.listdir(cur_year)

        for file in agg_file_list:
            cur_file=cur_year+file
            data=open(cur_file,"r")
            
            B=json.load(data)
            columns3["States"].append(state)
            columns3["Years"].append(year)
            columns3["Quarter"].append(int(file.strip(".json")))
            columns3["Registered_users"].append(B["data"]["aggregated"]["registeredUsers"])
            
            #TypeError: 'NoneType' object is not iterable
            # Check if 'usersByDevice' is not None before iterating
            if B["data"]["usersByDevice"] is not None:
                for i in B["data"]["usersByDevice"]:
                    brand=i["brand"]
                    count=i["count"]
                    percentage=i["percentage"]
                    columns2["Brand"].append(brand)
                    columns2["Count"].append(count)
                    columns2["Percentage"].append(percentage)
                    columns2["States"].append(state)
                    columns2["Years"].append(year)
                    columns2["Quarter"].append(int(file.strip(".json")))

user_by_device=pd.DataFrame(columns2)
totalregusers=pd.DataFrame(columns3)

import psycopg2
from psycopg2 import Error

# Establishing a connection to the PostgreSQL database
#code 3.1
try:
    connection = psycopg2.connect(
        host='localhost',
        database='phonepe',
        user='postgres',
        password='Sanchit@1995',
        port='5432'
    )

    cursor = connection.cursor()

    # Create user_by_device table
    # state,year,quarter wise brand and count
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_by_device (
            States VARCHAR(255),
            Years INT,
            Quarter INT,
            Brand VARCHAR(255),
            Count INT,
            Percentage FLOAT
        )
    """)

    # Create total_registered_users table
    # state,year,quarter wise registered users
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS total_registered_users (
            States VARCHAR(255),
            Years INT,
            Quarter INT,
            Registered_users INT
        )
    """)

    # Insert data into user_by_device table
    for index, row in user_by_device.iterrows():
        cursor.execute(
            '''INSERT INTO user_by_device 
            (States, Years, Quarter, Brand, Count, Percentage) 
            VALUES (%s, %s, %s, %s, %s, %s)''',
            (row['States'], row['Years'], row['Quarter'], 
             row['Brand'], row['Count'], row['Percentage'])
        )

    # Insert data into total_registered_users table
    for index, row in totalregusers.iterrows():
        cursor.execute(
            '''INSERT INTO total_registered_users 
            (States, Years, Quarter, Registered_users) 
            VALUES (%s, %s, %s, %s)''',
            (row['States'], row['Years'], row['Quarter'], 
             row['Registered_users'])
        )

    connection.commit()

except Error as e:
    print("Error while connecting to PostgreSQL", e)

#Once created the clone of GIT-HUB repository then,
#Required libraries for the program
# code 4

import pandas as pd
import json
import os

#This is to direct the path to get the data as states
#'''Maps Total number of transactions and total value of all transactions at the district level.useful for intra state info'''

path4="C:/Users/HP/pulse/data/map/transaction/hover/country/india/state/"
agg_map_transaction_list= os.listdir(path4)

columns5={"States":[],"Years":[],"Quarter":[], "District_Name":[],"Transaction_count":[], "Transaction_amount":[]}

for state in agg_map_transaction_list:
    cur_states=path4+state+"/"
    agg_year_list=os.listdir(cur_states)

    for year in agg_year_list:
        cur_year=cur_states+year+"/"
        agg_file_list=os.listdir(cur_year)

        for file in agg_file_list:
            cur_file=cur_year+file
            data=open(cur_file,"r")
            
            E=json.load(data)
            
            for i in E["data"]["hoverDataList"]:
                name=i["name"]
                count=i["metric"][0]["count"]
                amount=i["metric"][0]["amount"]
                columns5["District_Name"].append(name)
                columns5["Transaction_count"].append(count)
                columns5["Transaction_amount"].append(amount)
                columns5["States"].append(state)
                columns5["Years"].append(year)
                columns5["Quarter"].append(int(file.strip(".json")))

maps_total_district=pd.DataFrame(columns5)

import psycopg2
from psycopg2 import Error

#'''Maps Total number of transactions and total value of all transactions at the district level.useful for intra state info'''


# Establishing a connection to the PostgreSQL database
#code 4.1
try:
    connection = psycopg2.connect(
        host='localhost',
        database='phonepe',
        user='postgres',
        password='Sanchit@1995',
        port='5432'
    )

    cursor = connection.cursor()

    # Create maps_total table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS maps_total_district (
            States VARCHAR(255),
            Years INT,
            Quarter INT,
            District_Name VARCHAR(255),
            Transaction_count INT,
            Transaction_amount FLOAT
        )
    """)

    # Insert data into the maps_total table
    for index, row in maps_total_district.iterrows():
        cursor.execute(
            '''INSERT INTO maps_total_district 
            (States, Years, Quarter, District_Name, 
            Transaction_count, Transaction_amount) 
            VALUES (%s, %s, %s, %s, %s, %s)''',
            (row['States'], row['Years'], row['Quarter'], 
             row['District_Name'], row['Transaction_count'], 
             row['Transaction_amount'])
        )

    connection.commit()

except Error as e:
    print("Error while connecting to PostgreSQL", e)

#transastions at national level statewise
#Once created the clone of GIT-HUB repository then,
#Required libraries for the program

import pandas as pd
import json
import os

#This is to direct the path to get the data as states
#code 5
#'''Maps Total number of transactions and total value of all transactions at the national level per quarter and year'''


pathstate="C:/Users/HP/pulse/data/map/transaction/hover/country/india/"
state_map_transaction_list= os.listdir(pathstate)

columns20={"Years":[],"Quarter":[], "State_name":[],"Transaction_count":[], "Transaction_amount":[]}
stateignore2=os.listdir("C:/Users/HP/pulse/data/map/transaction/hover/country/india/state/")

for year in state_map_transaction_list:
    if year not in stateignore2:
        cur_year=pathstate+year+"/"
        per_year_list=os.listdir(cur_year)
        
        for file in per_year_list:
            try:
                cur_file=cur_year+file
                data=open(cur_file,"r")
                
                Z=json.load(data)
                
                for i in Z["data"]["hoverDataList"]:
                    name=i["name"]
                    count=i["metric"][0]["count"]
                    amount=i["metric"][0]["amount"]
                    columns20["State_name"].append(name)
                    columns20["Transaction_count"].append(count)
                    columns20["Transaction_amount"].append(amount)
                    columns20["Years"].append(year)
                    columns20["Quarter"].append(int(file.strip(".json")))
            except:
                pass
maps_total_transaction=pd.DataFrame(columns20)



import psycopg2
from psycopg2 import Error

#'''Maps Total number of transactions and total value of all transactions at the national level per quarter and year'''


# Establishing a connection to the PostgreSQL database
#code 5.1
try:
    connection = psycopg2.connect(
        host='localhost',
        database='phonepe',
        user='postgres',
        password='Sanchit@1995',
        port='5432'
    )

    cursor = connection.cursor()

    # Create maps_total_transaction table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS maps_total_transaction (
            Years INT,
            Quarter INT,
            State_name VARCHAR(255),
            Transaction_count BIGINT,
            Transaction_amount FLOAT
        )
    """)

    # Insert data into the maps_total_transaction table
    for index, row in maps_total_transaction.iterrows():
        cursor.execute(
            '''INSERT INTO maps_total_transaction 
            (Years, Quarter, State_name, Transaction_count, Transaction_amount) 
            VALUES (%s, %s, %s, %s, %s)''',
            (row['Years'], row['Quarter'], row['State_name'], 
             row['Transaction_count'], row['Transaction_amount'])
        )

    connection.commit()

except Error as e:
    print("Error while connecting to PostgreSQL", e)


#Once created the clone of GIT-HUB repository then,
#Required libraries for the program

#code 6
import pandas as pd
import json
import os

#This is to direct the path to get the data as states
#'''Total number of registered users and number of app opens by these registered users at the district level.'''

path5="C:/Users/HP/pulse/data/map/user/hover/country/india/state/"
agg_map_reg_user_list= os.listdir(path5)

columns6={"States":[],"Years":[],"Quarter":[], "Registered_Users":[],"app_Opens":[],"District":[]}

for state in agg_map_reg_user_list:
    cur_states=path5+state+"/"
    agg_year_list=os.listdir(cur_states)

    for year in agg_year_list:
        cur_year=cur_states+year+"/"
        agg_file_list=os.listdir(cur_year)

        for file in agg_file_list:
            cur_file=cur_year+file
            data=open(cur_file,"r")
            
            F=json.load(data)
            
            for district, data in F["data"]["hoverData"].items():
                reg_user = data["registeredUsers"]
                app_opens = data["appOpens"]
                columns6["app_Opens"].append(app_opens)
                columns6["Registered_Users"].append(reg_user)
                columns6["District"].append(district)
                columns6["States"].append(state)
                columns6["Years"].append(year)
                columns6["Quarter"].append(int(file.strip(".json")))
                
total_registered_users_districtlevel=pd.DataFrame(columns6)


import psycopg2
from psycopg2 import Error

#'''Total number of registered users and number of app opens by these registered users at the district level.'''


# Establishing a connection to the PostgreSQL database
#code 6.1
try:
    connection = psycopg2.connect(
        host='localhost',
        database='phonepe',
        user='postgres',
        password='Sanchit@1995',
        port='5432'
    )

    cursor = connection.cursor()

    # Create total_registered_users_districtlevel table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS 
        total_registered_users_districtlevel (
            States VARCHAR(255),
            Years INT,
            Quarter INT,
            District VARCHAR(255),
            Registered_Users INT,
            App_Opens INT
        )
    """)

    # Insert data into the total_registered_users_districtlevel table
    for index, row in total_registered_users_districtlevel.iterrows():
        cursor.execute(
            '''INSERT INTO total_registered_users_districtlevel 
            (States, Years, Quarter, District, 
            Registered_Users, App_Opens) 
            VALUES (%s, %s, %s, %s, %s, %s)''',
            (row['States'], row['Years'], row['Quarter'], 
             row['District'], row['Registered_Users'], 
             row['app_Opens'])
        )

    connection.commit()

except Error as e:
    print("Error while connecting to PostgreSQL", e)


#Once created the clone of GIT-HUB repository then,
#Required libraries for the program

#code 7
import pandas as pd
import json
import os

#This is to direct the path to get the data as states
#'''Top 10  districts / pin codes in one state where the most number of the transactions happened for a selected year-quarter combination.display this in transaction button'''

path6="C:/Users/HP/pulse/data/top/transaction/country/india/state/"
top_10_agg= os.listdir(path6)

columns7={"State":[],"Years":[],"Quarter":[], "District":[],"Transaction_count":[], "Transaction_amount":[]}
columns8={"State":[],"Years":[],"Quarter":[], "Pincode":[],"Transaction_count":[], "Transaction_amount":[]}

for state in top_10_agg:
    cur_states=path6+state+"/"
    agg_year_list=os.listdir(cur_states)

    for year in agg_year_list:
        cur_year=cur_states+year+"/"
        agg_file_list=os.listdir(cur_year)

        for file in agg_file_list:
            cur_file=cur_year+file
            data=open(cur_file,"r")
            
            F=json.load(data)
            
            for i in F["data"]["districts"]:
                district=i["entityName"]
                count=i["metric"]["count"]
                amount=i["metric"]["amount"]
                columns7["District"].append(district)
                columns7["Transaction_count"].append(count)
                columns7["Transaction_amount"].append(amount)
                columns7["State"].append(state)
                columns7["Years"].append(year)
                columns7["Quarter"].append(int(file.strip(".json")))
                
            for i in F["data"]["pincodes"]:
                pincode=i["entityName"]
                count=i["metric"]["count"]
                amount=i["metric"]["amount"]
                columns8["Pincode"].append(pincode)
                columns8["Transaction_count"].append(count)
                columns8["Transaction_amount"].append(amount)
                columns8["State"].append(state)
                columns8["Years"].append(year)
                columns8["Quarter"].append(int(file.strip(".json")))
                
top_10_districts_trans=pd.DataFrame(columns7)
top_10_pincode_trans=pd.DataFrame(columns8)


import psycopg2
from psycopg2 import Error

#'''Top 10  districts / pin codes in one state where the most number of the transactions happened for a selected year-quarter combination.display this in transaction button'''


#code 7.1
# Establishing a connection to the PostgreSQL database
try:
    connection = psycopg2.connect(
        host='localhost',
        database='phonepe',
        user='postgres',
        password='Sanchit@1995',
        port='5432'
    )

    cursor = connection.cursor()

    # Create top_10_districts_trans table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS top_10_districts_trans (
            State VARCHAR(255),
            Years INT,
            Quarter INT,
            District VARCHAR(255),
            Transaction_count INT,
            Transaction_amount FLOAT
        )
    """)

    # Insert data into the top_10_districts_trans table
    for index, row in top_10_districts_trans.iterrows():
        cursor.execute(
            '''INSERT INTO top_10_districts_trans 
            (State, Years, Quarter, 
            District, Transaction_count, Transaction_amount) 
            VALUES (%s, %s, %s, %s, %s, %s)''',
            (row['State'], row['Years'], row['Quarter'], 
             row['District'], row['Transaction_count'], 
             row['Transaction_amount'])
        )

    # Create top_10_pincode_trans table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS top_10_pincode_trans (
            State VARCHAR(255),
            Years INT,
            Quarter INT,
            Pincode VARCHAR(255),
            Transaction_count INT,
            Transaction_amount FLOAT
        )
    """)

    # Insert data into the top_10_pincode_trans table
    for index, row in top_10_pincode_trans.iterrows():
        cursor.execute(
            '''INSERT INTO top_10_pincode_trans 
            (State, Years, Quarter, Pincode, 
            Transaction_count, Transaction_amount) 
            VALUES (%s, %s, %s, %s, %s, %s)''',
            (row['State'], row['Years'], row['Quarter'], 
             row['Pincode'], row['Transaction_count'], 
             row['Transaction_amount'])
        )

    connection.commit()

except Error as e:
    print("Error while connecting to PostgreSQL", e)


#Once created the clone of GIT-HUB repository then,
#Required libraries for the program

import pandas as pd
import json
import os

#code 8
#'''top ten states in one quarter per annum'''

path7="C:/Users/HP/pulse/data/top/transaction/country/india/"
agg_year_list= os.listdir(path7)
stateignore1=os.listdir("C:/Users/HP/pulse/data/top/transaction/country/india/state/")

columns9={"Years":[],"Quarter":[], "State_name":[],"Transaction_count":[], "Transaction_amount":[]}

for year in agg_year_list:
    if year not in stateignore1:
        cur_year = os.path.join(path7, year)
        for file in os.listdir(cur_year):
            cur_file = os.path.join(cur_year, file)
           #to remove PermissionError: [Errno 13] Permission denied: 'C:/Users/HP/pulse/data/top/transaction/country/india/state\\andaman-&-nicobar-islands'
            try:
                with open(cur_file, "r") as data:
                    #Opening a file in read mode ('r') allows you to read the contents of the file without modifying it
                    G = json.load(data)
                    #When you read a JSON file using Python's json.load() function, you are essentially converting the JSON data from the file into a Python data structure
                    for i in G["data"]["states"]:
                        state=i["entityName"]
                        count=i["metric"]["count"]
                        amount=i["metric"]["amount"]
                        columns9["State_name"].append(state)
                        columns9["Transaction_count"].append(count)
                        columns9["Transaction_amount"].append(amount)
                    
                        columns9["Years"].append(year)
                        columns9["Quarter"].append(int(file.strip(".json")))
            except:
                pass
            
top_10_states=pd.DataFrame(columns9)


import psycopg2

#'''top ten states in one quarter per annum'''

# Connect to the PostgreSQL database
#code 8.1
connection = psycopg2.connect(
    host='localhost',
    database='phonepe',
    user='postgres',
    password='Sanchit@1995',
    port='5432'
)

# Create a cursor object to interact with the database
cursor = connection.cursor()

# Create the table if it doesn't exist
cursor.execute('''CREATE TABLE IF NOT EXISTS top_ten_states (
    Years VARCHAR(255),
    Quarter INT,
    State_name VARCHAR(255),
    Transaction_count BIGINT,
    Transaction_amount BIGINT
)''')

# Insert data into the table
for index, row in top_10_states.iterrows():
    sql = '''INSERT INTO top_ten_states 
    (Years, Quarter, State_name, 
    Transaction_count, Transaction_amount) 
    VALUES (%s, %s, %s, %s, %s)'''
    
    cursor.execute(sql, (row["Years"], row["Quarter"], row["State_name"], 
                         row["Transaction_count"], row["Transaction_amount"]))

# Commit the changes to the database
connection.commit()


#Once created the clone of GIT-HUB repository then,
#Required libraries for the program

#code 9
import pandas as pd
import json
import os

#'''Top 10 states / districts / pin codes where most number of users registered from, for a selected year-quarter combination.'''

path8="C:/Users/HP/pulse/data/top/user/country/india/"
agg_year1_list= os.listdir(path8)
stateignore2=os.listdir("C:/Users/HP/pulse/data/top/user/country/india/state/")

columns11={"Years":[],"Quarter":[], "State_name":[],"Registered_user":[]}
columns12={"Years":[],"Quarter":[], "District_name":[],"Registered_user":[]}
columns13={"Years":[],"Quarter":[], "Pincode":[],"Registered_user":[]}

for year in agg_year1_list:
    if year not in stateignore2:
        cur_year = os.path.join(path8, year)
        for file in os.listdir(cur_year):
            cur_file = os.path.join(cur_year, file)
           #to remove PermissionError: [Errno 13] Permission denied: 'C:/Users/HP/pulse/data/top/user/country/india/state\\andaman-&-nicobar-islands'
            try:
                with open(cur_file, "r") as data:
                    #Opening a file in read mode ('r') allows you to read the contents of the file without modifying it
                    H = json.load(data)
                    #When you read a JSON file using Python's json.load() function, you are essentially converting the JSON data from the file into a Python data structure
                    for i in H["data"]["states"]:
                        state=i["name"]
                        reg_user2=i["registeredUsers"]
                        columns11["State_name"].append(state)
                        columns11["Registered_user"].append(reg_user2)
                        columns11["Years"].append(year)
                        columns11["Quarter"].append(int(file.strip(".json")))
                    
                    for i in H["data"]["districts"]:
                        district=i["name"]
                        reg_user3=i["registeredUsers"]
                        columns12["District_name"].append(district)
                        columns12["Registered_user"].append(reg_user3)
                        columns12["Years"].append(year)
                        columns12["Quarter"].append(int(file.strip(".json")))
                    
                    for i in H["data"]["pincodes"]:
                        pincode=i["name"]
                        reg_user3=i["registeredUsers"]
                        columns13["Pincode"].append(pincode)
                        columns13["Registered_user"].append(reg_user3)
                        columns13["Years"].append(year)
                        columns13["Quarter"].append(int(file.strip(".json")))
                        
            except:
                pass

top_10_user_states=pd.DataFrame(columns11)
top_10_user_districts=pd.DataFrame(columns12)
top_10_user_pincode=pd.DataFrame(columns13)



import psycopg2

#code 9.1
#'''Top 10 states / districts / pin codes where most number of users registered from, for a selected year-quarter combination.'''

# Connect to the PostgreSQL database
connection = psycopg2.connect(
    host='localhost',
    database='phonepe',
    user='postgres',
    password='Sanchit@1995',
    port='5432'
)

# Create a cursor object to interact with the database
cursor = connection.cursor()

# Create the top_10_user_states table if it doesn't exist
cursor.execute('''CREATE TABLE IF NOT EXISTS top_10_user_states (
    Years VARCHAR(255),
    Quarter INT,
    State_name VARCHAR(255),
    Registered_user INT
)''')

# Insert data into the top_10_user_states table
for index, row in top_10_user_states.iterrows():
    sql = '''INSERT INTO top_10_user_states 
    (Years, Quarter, State_name, Registered_user) 
    VALUES (%s, %s, %s, %s)'''
    
    cursor.execute(sql, (row["Years"], row["Quarter"], row["State_name"], row["Registered_user"]))

# Create the top_10_user_districts table if it doesn't exist
cursor.execute('''CREATE TABLE IF NOT EXISTS top_10_user_districts (
    Years VARCHAR(255),
    Quarter INT,
    District_name VARCHAR(255),
    Registered_user INT
)''')

# Insert data into the top_10_user_districts table
for index, row in top_10_user_districts.iterrows():
    sql = '''INSERT INTO top_10_user_districts 
    (Years, Quarter, District_name, Registered_user) 
    VALUES (%s, %s, %s, %s)'''
    
    cursor.execute(sql, (row["Years"], row["Quarter"], row["District_name"], row["Registered_user"]))

# Create the top_10_user_pincode table if it doesn't exist
cursor.execute('''CREATE TABLE IF NOT EXISTS top_10_user_pincode (
    Years VARCHAR(255),
    Quarter INT,
    Pincode VARCHAR(255),
    Registered_user INT
)''')

# Insert data into the top_10_user_pincode table
for index, row in top_10_user_pincode.iterrows():
    sql = '''INSERT INTO top_10_user_pincode 
    (Years, Quarter, Pincode, Registered_user) 
    VALUES (%s, %s, %s, %s)'''
    
    cursor.execute(sql, (row["Years"], row["Quarter"], row["Pincode"], row["Registered_user"]))

# Commit the changes to the database
connection.commit()





import requests

# Load the GeoJSON file
url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
response = requests.get(url)
geojson_data = response.json()

# Extract and store the state names
state_names = []
for feature in geojson_data['features']:
    state_name = feature['properties']['ST_NM']
    state_names.append(state_name)

# Sort the state names alphabetically
state_names.sort()


import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2

# Function to fetch data for a specific year and quarter
def fetch_data(year, quarter):#code 1.1
    conn = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Sanchit@1995",
        database="phonepe",
        port="5432"
    )
    cursor = conn.cursor()

    cursor.execute(f"""
        SELECT States, SUM(Transaction_amount) AS Total_amount
        FROM payment_transact_districtlevel
        WHERE Years = {year} AND Quarter = {quarter}
        GROUP BY States
        ORDER BY States;
    """)

    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['States', 'Total_amount'])

    cursor.close()
    conn.close()

    return df

def fetch_data_transact(year, quarter):#code 2.1
    conn = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="Sanchit@1995",
    database="phonepe",
    port="5432"
    )
    
    cursor = conn.cursor()
    cursor.execute("""
        SELECT Transaction_type, SUM(Transaction_count) AS Total_transaction_count, SUM(Transaction_amount) AS Total_transaction_amount
        FROM country_transaction_data
        WHERE Years = %s AND Quarter = %s
        GROUP BY Transaction_type
        ORDER BY Transaction_type;
    """, (year, quarter))
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['Transaction_type', 'Total_transaction_count', 'Total_transaction_amount'])
    return df

# Function to display the map
def display_map(df, year, quarter):#code 1.1
    #state_names = [feature['properties']['ST_NM'] for feature in geojson_data['features']]
    df['States'] = state_names

    fig = px.choropleth(df, geojson=geojson_data,
                        featureidkey="properties.ST_NM",
                        locations="States",
                        color="Total_amount",
                        color_continuous_scale="RdBu_r",
                        range_color=(df["Total_amount"].min(), df["Total_amount"].max()))

    fig.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig)
    
def fetch_data_bar(year, quarter):#code 1.1
    conn = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Sanchit@1995",
        database="phonepe",
        port="5432"
    )
    cursor = conn.cursor()

    # Execute the query to fetch the data and group by States
    cursor.execute(f"""
        SELECT States, SUM(Transaction_amount) AS Total_amount
        FROM payment_transact_districtlevel
        WHERE Years = {year} AND Quarter = {quarter}
        GROUP BY States
        ORDER BY Total_amount DESC;
    """)

    # Fetch the results and store them in a DataFrame
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['States', 'Total_amount'])

    cursor.close()
    conn.close()

    return df

def get_registered_users(year, quarter):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
       host="localhost",
        user="postgres",
        password="Sanchit@1995",
        database="phonepe",
        port="5432"
    )
    cursor = conn.cursor()

    # Execute the query to fetch the total registered users per state for the specified year and quarter
    cursor.execute("""
        SELECT States, SUM(Registered_users) AS Total_registered_users
        FROM total_registered_users
        WHERE Years = %s AND Quarter = %s
        GROUP BY States
        ORDER BY States; -- Order by States alphabetically
    """, (year, quarter))

    # Fetch the results and store them in a DataFrame
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['States', 'Total_registered_users'])

    # Close the cursor and connection
    cursor.close()
    conn.close()

    return df

def get_brand_count(year, quarter):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Sanchit@1995",
        database="phonepe",
        port="5432"
    )
    cursor = conn.cursor()

    # Execute the query to fetch the brand count for every brand per quarter and year
    cursor.execute("""
        SELECT Brand, SUM(Count) AS Total_Count, 
               ROUND((SUM(Count) / SUM(SUM(Count)) OVER ()) * 100, 2) AS Percentage
        FROM user_by_device
        WHERE Years = %s AND Quarter = %s
        GROUP BY Brand
        ORDER BY Total_Count DESC;
    """, (year, quarter))

    # Fetch the results and store them in a DataFrame
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['Brand', 'Total_Count','Percentage'])

    # Close the cursor and connection
    cursor.close()
    conn.close()

    return df


def get_brand_percentage(year, quarter):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Sanchit@1995",
        database="phonepe",
        port="5432"
    )
    cursor = conn.cursor()

    # Execute the query to fetch the brand count for every brand per quarter and year
    cursor.execute("""
        SELECT States, Brand, SUM(Count) AS Total_Count, 
               ROUND((SUM(Count) / SUM(SUM(Count)) OVER ()) * 100, 2) AS Percentage
        FROM user_by_device
        WHERE Years = %s AND Quarter = %s
        GROUP BY States, Brand
        ORDER BY States, Total_Count DESC;
    """, (year, quarter))

    # Fetch the results and store them in a DataFrame
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['States', 'Brand', 'Total_Count', 'Percentage'])

    # Close the cursor and connection
    cursor.close()
    conn.close()

    return df

def get_registered_users_pie_chart(state, year, quarter):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host="localhost",
        database="phonepe",
        user="postgres",
        password="Sanchit@1995",
        port="5432"
    )
    cursor = conn.cursor()

    # Execute the query to fetch the data for the specified state, year, and quarter
    cursor.execute("""
        SELECT Registered_Users, District
        FROM total_registered_users_districtlevel
        WHERE States = %s AND Years = %s AND Quarter = %s
    """, (state, year, quarter))

    # Fetch the results and store them in a DataFrame
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['Registered_Users', 'District'])

    # Close the cursor and connection
    cursor.close()
    conn.close()

    # Create a pie chart using Plotly with different colors for each district
    fig = px.pie(df, values='Registered_Users', names='District',
                 title=f'Registered Users Distribution in {state} for {year} Q{quarter}',
                 color_discrete_sequence=px.colors.qualitative.Set3)
    return fig


def get_top_10_user_states(year, quarter):
    conn = psycopg2.connect(
       host="localhost",
        database="phonepe",
        user="postgres",
        password="Sanchit@1995",
        port="5432"
    )
    cursor = conn.cursor()

    # Fetch the top 10 user states for the specified year and quarter
    cursor.execute(f"""
        SELECT Years, Quarter, State_name, Registered_user
        FROM top_10_user_states
        WHERE Years = '{year}' AND Quarter = {quarter}
        LIMIT 10;
        
    """)

    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['Years', 'Quarter', 'State_name', 'Registered_user'])

    return df

def get_top_10_user_districts(year, quarter):
    conn = psycopg2.connect(
       host="localhost",
        database="phonepe",
        user="postgres",
        password="Sanchit@1995",
        port="5432"
    )
    cursor = conn.cursor()

    # Fetch the top 10 user districts for the specified year and quarter
    cursor.execute(f"""
        SELECT Years, Quarter, District_name, Registered_user
        FROM top_10_user_districts
        WHERE Years = '{year}' AND Quarter = {quarter}
        LIMIT 10;
        
    """)

    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['Years', 'Quarter', 'District_name', 'Registered_user'])

    cursor.close()
    conn.close()

    return df

def get_top_10_user_pincode(year, quarter):
    conn = psycopg2.connect(
       host="localhost",
        database="phonepe",
        user="postgres",
        password="Sanchit@1995",
        port="5432"
    )
    cursor = conn.cursor()

    # Fetch the top 10 user pincode for the specified year and quarter
    cursor.execute(f"""
        SELECT Years, Quarter, Pincode, Registered_user
        FROM top_10_user_pincode
        WHERE Years = '{year}' AND Quarter = {quarter}
        LIMIT 10;
        
    """)

    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['Years', 'Quarter', 'Pincode', 'Registered_user'])

    cursor.close()
    conn.close()

    return df

def fetch_district_data(state, year, quarter):
    try:
        connection = psycopg2.connect(
            host='localhost',
            database='phonepe',
            user='postgres',
            password='Sanchit@1995',
            port='5432'
        )

        # Fetch data
        cursor = connection.cursor()
        cursor.execute(f"""
            SELECT DISTINCT ON (District_Name) District_Name, Transaction_count, Transaction_amount
            FROM maps_total_district
            WHERE States = '{state}' AND Years = {year} AND Quarter = {quarter}
        """)
        
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=['District_Name', 'Transaction_count', 'Transaction_amount'])

        return df

    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL database:", e)
        return None
 

def display_transaction_pie_charts(year, quarter, state):
    try:
        connection = psycopg2.connect(
            host='localhost',
            database='phonepe',
            user='postgres',
            password='Sanchit@1995',
            port='5432'
        )

        # Fetch data
        cursor = connection.cursor()
        cursor.execute(f"""
            SELECT District_Name, Transaction_count, Transaction_amount
            FROM maps_total_district
            WHERE Years = {year} AND Quarter = {quarter} AND States = '{state}'
        """)
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=['District_Name', 'Transaction_count', 'Transaction_amount'])

        # Create pie chart for transaction count with different colors
        fig_count = px.pie(df, values='Transaction_count', names='District_Name', title='Transaction Count by District',
                           color_discrete_sequence=px.colors.qualitative.Set3)

        # Create pie chart for transaction amount with different colors
        fig_amount = px.pie(df, values='Transaction_amount', names='District_Name', title='Transaction Amount by District',
                            color_discrete_sequence=px.colors.qualitative.Set3)

        st.plotly_chart(fig_count)
        st.plotly_chart(fig_amount)

    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL database:", e)   


def fetch_top_ten_data(state, year, quarter):
    try:
        connection = psycopg2.connect(
            host='localhost',
            database='phonepe',
            user='postgres',
            password='Sanchit@1995',
            port='5432'
        )

        # Fetch top ten districts
        cursor = connection.cursor()
        cursor.execute(f"""
            SELECT DISTINCT ON (District) State, Years, Quarter, District, Transaction_count, Transaction_amount
            FROM top_10_districts_trans
            WHERE State = '{state}' AND Years = {year} AND Quarter = {quarter}
        """)
        data = cursor.fetchall()
        top_10_districts = pd.DataFrame(data, columns=['State', 'Years', 'Quarter', 'District', 'Transaction_count', 'Transaction_amount'])

        # Fetch top ten pincodes
        cursor.execute(f"""
            SELECT DISTINCT ON (Pincode) State, Years, Quarter, Pincode, Transaction_count, Transaction_amount
            FROM top_10_pincode_trans
            WHERE State = '{state}' AND Years = {year} AND Quarter = {quarter}
        """)
        data = cursor.fetchall()
        top_10_pincodes = pd.DataFrame(data, columns=['State', 'Years', 'Quarter', 'Pincode', 'Transaction_count', 'Transaction_amount'])

        return top_10_districts, top_10_pincodes

    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL database:", e)
        return None, None
    
def fetch_top_states_data(year, quarter):
    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(
            host='localhost',
            database='phonepe',
            user='postgres',
            password='Sanchit@1995',
            port='5432'
        )

        # Fetch distinct data for the specified year and quarter
        cursor = connection.cursor()
        cursor.execute(f"""
            SELECT DISTINCT Years, Quarter, State_name, Transaction_count, Transaction_amount
            FROM top_ten_states
            WHERE Years = '{year}' AND Quarter = {quarter}
            ORDER BY Transaction_amount DESC
        """)
        data = cursor.fetchall()
        top_states_df = pd.DataFrame(data, columns=['Years', 'Quarter', 'State_name', 'Transaction_count', 'Transaction_amount'])

        return top_states_df

    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL database:", e)
        return None
    


# Main Streamlit app
def main():

    
    st.set_page_config(layout= "wide")
    st.markdown("<h1 style='color: violet;'>PHONEPE DATA VISUALIZATION AND EXPLORATION</h1>", unsafe_allow_html=True)


    st.header("Which data to display?")
    # Create a radio button to select between Transactions and User data
    selected_data = st.selectbox("",["Transactions", "User"])

    # Display the selected data type as a title in bold
    st.markdown(f"### **{selected_data} Data**")
    
    if selected_data == "Transactions":
        # Load the GeoJSON file
        geojson_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        geojson_data = requests.get(geojson_url).json()

        # Select year and quarter
        year = st.slider("Select Year", min_value=2018, max_value=2022, step=1)
        quarter = st.slider("Select Quarter", min_value=1, max_value=4, step=1)
        
        # Display tables side by side
        col1, col2 = st.columns(2)
        with col1:
            #top 10 states in one year
            top_states_df = fetch_top_states_data(year, quarter)
            st.subheader('Top ten states in one year')
            st.write(top_states_df)


            # Fetch data for the selected year and quarter
            df = fetch_data(year, quarter)#code 1.1
            df2=fetch_data_transact(year, quarter)#code 2.1
            df3=fetch_data_bar(year,quarter)#code 1.1

            # Display the map code 1.1
            st.subheader(f"Map for Year: {year}, Quarter: {quarter}")
            display_map(df, year, quarter)
            
        with col2:
            
            # Fetch data for the selected year and quarter
            df = fetch_data(year, quarter)#code 1.1
            df2=fetch_data_transact(year, quarter)#code 2.1
            df3=fetch_data_bar(year,quarter)#code 1.1
            # Display all transaction types for all states
            st.subheader("Sum of Transaction Types for All States")
            st.write(df)
            
            # Display sum of transaction types for entire country
            st.subheader("Sum of all Transaction Types for entire country per year and quarter")
            st.write(df2)

            # Display the bar chart
            st.subheader(f"Bar Chart for Year: {year}, Quarter: {quarter}")
            fig = px.bar(df3, x='States', y='Total_amount', color='States',
                        color_discrete_sequence=px.colors.qualitative.Set1,
                        text='Total_amount')
            fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
            fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
            st.plotly_chart(fig)
        
        states = ['andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh', 'assam',
            'bihar', 'chandigarh', 'chhattisgarh', 'dadra-&-nagar-haveli-&-daman-&-diu',
            'delhi', 'goa', 'gujarat', 'haryana', 'himachal-pradesh', 'jammu-&-kashmir',
            'jharkhand', 'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh',
            'maharashtra', 'manipur', 'meghalaya', 'mizoram', 'nagaland', 'odisha',
            'puducherry', 'punjab', 'rajasthan', 'sikkim', 'tamil-nadu', 'telangana',
            'tripura', 'uttar-pradesh', 'uttarakhand', 'west-bengal']

        # Streamlit code
        st.subheader("**Select a State to View Transaction_amount and Transaction_count of every district**")
        selected_state = st.selectbox('Select State', states)
        
        col1, col2 = st.columns(2)
        with col1:
        
            # Fetch and display data using Streamlit district data
            st.subheader("Total Transactions and Transaction Amounts by District")
            df9 = fetch_district_data(selected_state, year, quarter)
            st.write(df9)
            
            #pie chart for district
            st.subheader('Transaction Analysis by District')

            display_transaction_pie_charts(year, quarter, selected_state)

        with col2:

            # Display top ten districts and top ten pincodes
            
            top_10_districts, top_10_pincodes = fetch_top_ten_data(selected_state, year, quarter)
            st.subheader('Top Ten Districts:')
            st.write(top_10_districts)
            st.subheader('Top Ten Pincodes:')
            st.write(top_10_pincodes)

        
            
    else:
        # Load the GeoJSON file
        geojson_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        geojson_data = requests.get(geojson_url).json()

        # Select year and quarter
        year = st.slider("Select Year", min_value=2018, max_value=2022, step=1)
        quarter = st.slider("Select Quarter", min_value=1, max_value=4, step=1)
        
        # Display other charts and tables using columns layout
        col1, col2 = st.columns(2)

        with col1:
        
            #maximum user devices are xiaomi followed by samsung earlier now vivo
            st.write("Top User Brands:")

            brand_count_df = get_brand_count(year, quarter)
            st.write(brand_count_df) 
            registered_users_df = get_registered_users(year, quarter)
            registered_users_df['States'] = state_names
            
            # Create space between rows
            #st.markdown("<br>", unsafe_allow_html=True)
            
           
            # Fetch the top 10 user districts for the specified year and quarter
            top_10_user_districts = get_top_10_user_districts(year, quarter)
            st.write("Top 10 User Districts:")
            st.table(top_10_user_districts)
            
            # Create space between columns
            st.markdown("<div style='padding: 20px;'></div>", unsafe_allow_html=True)

        
            
            # Create the choropleth map
            fig2 = px.choropleth(
                registered_users_df,
                geojson=geojson_data,
                featureidkey="properties.ST_NM",
                locations="States",
                color="Total_registered_users",
                color_continuous_scale="RdBu_r",  # Use RdBu_r for a diverging scale
                range_color=(registered_users_df["Total_registered_users"].min(), registered_users_df["Total_registered_users"].max())
            )

            # Update layout
            fig2.update_geos(fitbounds="locations", visible=False)        
            st.plotly_chart(fig2)

            

            #pie chart for different districts within state
            states = ['andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh', 'assam',
            'bihar', 'chandigarh', 'chhattisgarh', 'dadra-&-nagar-haveli-&-daman-&-diu',
            'delhi', 'goa', 'gujarat', 'haryana', 'himachal-pradesh', 'jammu-&-kashmir',
            'jharkhand', 'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh',
            'maharashtra', 'manipur', 'meghalaya', 'mizoram', 'nagaland', 'odisha',
            'puducherry', 'punjab', 'rajasthan', 'sikkim', 'tamil-nadu', 'telangana',
            'tripura', 'uttar-pradesh', 'uttarakhand', 'west-bengal']

            # Streamlit code
            st.markdown("**Select a State to View Registered Users Distribution**")
            selected_state = st.selectbox('Select State', states)

            # Display the pie chart for the selected state
            fig = get_registered_users_pie_chart(selected_state,year,quarter)
            st.plotly_chart(fig)
            
            
        with col2:

            # Fetch the top 10 user states for the specified year and quarter
            top_10_user_states = get_top_10_user_states(year, quarter)
            st.write("Top 10 User States:")
            st.table(top_10_user_states)
            
            # Fetch the top 10 user pincode for the specified year and quarter
            top_10_user_pincode = get_top_10_user_pincode(year, quarter)
            st.write("Top 10 User Pincode:")
            st.table(top_10_user_pincode)  
            
            # Fetch brand percentage 
            brand_percentage_df = get_brand_percentage(year, quarter)
            
            # Define a color sequence for the brands
            color_sequence = px.colors.qualitative.Set1

            # Create the bar chart
            fig = px.bar(brand_percentage_df, x='States', y='Percentage', color='Brand',
                        color_discrete_map={brand: color_sequence[i % len(color_sequence)] for i, brand in enumerate(brand_percentage_df['Brand'].unique())},
                        labels={'States': 'States', 'Percentage': 'Brand Percentage', 'Brand': 'Brand'},
                        title=f'Brand Percentage for {year} Q{quarter}',
                        category_orders={'States': brand_percentage_df['States'].unique()[::-1]})

            # Update layout
            fig.update_layout(bargap=0.1, height=600, width=1000,
                            xaxis_title="States", yaxis_title="Brand Percentage")
            st.plotly_chart(fig)      

    
if __name__ == "__main__":
    main()

