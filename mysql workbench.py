#!/usr/bin/env python
# coding: utf-8

# In[10]:


#import libraries
import mysql.connector
from mysql.connector import Error
import pandas as pd


# In[11]:


df = pd.read_csv('April.csv')


# In[12]:


df.head()


# In[13]:


df.rename(columns = {'d':'Dates','Google Ad Sense':'Adsense', 'Google ADX':'Adx', 'B Code':'Bcode', 'Daily Total':'Daily_total', 'BrightCom':'Brightcom'}, inplace = True)


# In[22]:


df.head()


# In[23]:


df.tail()


# In[25]:


df = df.drop(['Unfilled Impression'], axis=1)


# In[26]:


df.drop([30, 31, 32], inplace=True)


# In[27]:


df.tail()


# In[28]:


def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host = host_name,
            user = user_name,
            passwd = user_password
        )
        print("MySQL Database connection Successful")
    except Error as err:
            print(f"Error: '{err}'")
    return connection
        
# Mysql terminal password
pw = "electelect"

# Database Name
db = "revenue"
connection = create_server_connection("localhost", "root", pw)


# In[29]:


# create database revenue

def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")
create_database_query = "Create database revenue"
create_database(connection, create_database_query)


# In[30]:


# connect to database
def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
           host = host_name,
            user = user_name,
            passwd = user_password,
            database = db_name
        )
        print("Mysql database connection successfully")
    except Error as err:
        print(f"Error: '{err}'")
    return connection


# In[31]:


# execute sql queries
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query was successful")
    except Error as err:
        print(f"Error: '{err}'")


# In[34]:


data_create = """
CREATE TABLE April(
id INT AUTO_INCREMENT,
   Dates varchar(80),
   Amazon varchar(80),
   Eskimi varchar(80),
   Adsense varchar(80),
   Revcontent varchar(80),
   Dot varchar(80),
   Adx varchar(80),
   Brightcom varchar(80),
   Bcode varchar(80),
   Primis varchar(80),
   Daily_total varchar(80),
   PRIMARY KEY(id)
);
"""
#connect to database
connection = create_db_connection("localhost", "root", pw, db)
execute_query(connection, data_create)


# In[37]:


#insert data

for i, row in df.iterrows():
    cursor = connection.cursor()

    add_revenue = '''INSERT INTO April (Dates, Amazon, Eskimi, Adsense, Revcontent, Dot, Adx, Brightcom, Bcode, Primis, Daily_total)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                '''
    cursor.execute(add_revenue, tuple(row))

connection.commit()

