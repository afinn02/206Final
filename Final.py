import unittest
import sqlite3
import json
import os
import requests

def open_database(db_name):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS Obesity (cityname TEXT PRIMARY KEY, data_value REAL, stateabbr TEXT)")
    conn.commit()
    return cur, conn
def getData(cur, conn):
    base_url = f'https://chronicdata.cdc.gov/resource/bjvu-3y7d.json?'
    vars = {'year':'2017'}
    r = requests.get(base_url, params=vars)
    j= json.loads(r.text)
    i = 0
    state_dict = {}
    for city in j:
        if city['stateabbr'] not in state_dict:
            state_dict[city['stateabbr']] = i
            i+=1
        try:
            cur.execute("INSERT OR IGNORE INTO Obesity (cityname, data_value, stateabbr) VALUES (?,?,?)",(city['cityname'], city['data_value'], state_dict[city['stateabbr']]))
        except:
            continue
    conn.commit()
def main(): 
    open_database('data.db')
    cur, conn = open_database('data.db')
    getData(cur, conn)
    pass

main()
