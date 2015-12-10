#!/usr/bin/python
import sqlite3
import datetime
import requests
import json
from bs4 import BeautifulSoup

def Initial():
    try:
        f = open('stock.db', 'r+')
        f.close()
        print "database stock.db exist"
    except:
        conn = sqlite3.connect('stock.db')
        cur=conn.cursor()
        cur.execute("create table inout ( code text, cname text, fbuy real, fsell real, tbuy real, tsell real, sbuy real, ssell real)")
        conn.close()


def connectDatabase():
    try:
        conn = sqlite3.connect('stock.db')
        return conn
    except:
        print "Can't connect to database: stock.db"
        
    
def getYYYMMDD():
    now = datetime.datetime.now()
    #print now
    return "%d/%02d/%02d" %( now.year-1911,now.month,now.day-1)



def constructPayload(Date, Select):
    payload = { 
        "Host": "http://www.twse.com.tw",
        "Referer": "http://www.twse.com.tw/ch/trading/fund/T86/T86.php",
        "input_date": "104/12/04",
        "select2": "02",
        "sorting": "by_issue",
        "login_btn": "+%ACd%B8%DF+-body"
        }
    if len(Date) != 0:
        payload['input_date']=Date
    if len(Select) != 0:
        payload['select2']=Select
    #print payload
    return payload


url="http://www.twse.com.tw/ch/trading/fund/T86/T86.php"


payload=constructPayload(Date=getYYYMMDD(), Select="02")

response = requests.post(url, data=payload).text

Initial()

conn=sqlite3.connect('stock.db')
cur= conn.cursor()

soup = BeautifulSoup(response, 'html.parser')
for row in soup.find_all("tr", bgcolor='#FFFFFF'):
	tds = row.find_all("td")
	if len(tds) != 1:
		try:
			sid = tds[0].get_text().encode('utf8')
			cname = tds[1].get_text().encode('latin1','ignore').decode('big5')
			fin = tds[2].get_text().encode('utf8')
			fout = tds[3].get_text().encode('utf8')
			tin = tds[4].get_text().encode('utf8')
			tout = tds[5].get_text().encode('utf8')
			sin = tds[6].get_text().encode('utf8')
			sout = tds[7].get_text().encode('utf8')
         		print sid, cname, fin, fout, tin, tout, sin, sout
			cur.execute("INSERT INTO inout ( code , cname , fbuy , fsell , tbuy , tsell , sbuy , ssell ) VALUES (?,?,?,?,?,?,?,?)",(sid, cname, fin, fout, tin, tout, sin, sout))
		except:
			print "bad data"
			continue


#cur= conn.cursor()
#row = cur.fetchone()
#print row
conn.commit()
conn.close()


