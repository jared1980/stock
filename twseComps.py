#!/usr/bin/python
#-*- coding: utf-8 -*-
import sqlite3
import datetime
import requests
import json
from bs4 import BeautifulSoup
from grs import TWSENo
import time

def CreateDatabaseAndTable(DBName, List):
    try:
        f = open(DBName, 'r+')
        f.close()
    except:
        print "create database " + DBName + " DONE"
        conn = sqlite3.connect(DBName)
        cur=conn.cursor()
        for stockId in List:
          try:
            cur.execute("create table Id" + stockId + " ( Date TEXT PRIMARY KEY, TradingVolume INTEGER, TradingValue INTEGER, OpeningPrice REAL, HighestPrice REAL, FloorPrice REAL, ClosingPrice REAL, DifferencePrices REAL, TradingCount INTEGER, ForeignBuying INTEGER, ForeignSell INTEGER, InvestmentBuy INTEGER, InvestmentSell INTEGER, DealersBuy INTEGER, DealersSell INTEGER, DealersBuyHedging INTEGER, DealersSellHedging INTEGER, TotalTradingVolume INTEGER)")
            #print "create table Id" + stockId + " DONE"
          except:
            continue
        conn.close()

def GetDBName (IndustryCode):
  dbname = "Industry" + IndustryCode + ".db"
  #print "DBName = " + dbname + "."
  return dbname

  
def GetIndustryCodeFromStockId(StockId):
  Comps = TWSENo().industry_comps
  for IndustryCode in TWSENo().industry_code:
    try:
      if len(Comps[str(IndustryCode)])>0:
        if StockId in Comps[str(IndustryCode)]:
          #print "Found " + StockId + " in " + IndustryCode
          return IndustryCode
        else:
          continue
    except:
      #print "Comps with key " + IndustryCode + " empty"
      continue


def Initial():
  twseIndustryCode = TWSENo().industry_code
  twseIndustryComps = TWSENo().industry_comps
  #for Code in twseIndustryComps:
  for Code in ['01','02','13']:
    if len(twseIndustryComps[str(Code)])>0:
      CreateDatabaseAndTable( GetDBName(Code), twseIndustryComps[str(Code)])
    else:
      print "Industry" + Code + " has empty comps"
      continue

def GetStockTradeInfo(StockId, Year, Month):
  URL="http://www.twse.com.tw/ch/trading/exchange/STOCK_DAY/genpage/Report" + Year + Month + "/" + Year + Month + "_F3_1_8_" + StockId + ".php?STK_NO=" + StockId + "&myear=" + Year + "&mmon=" + Month
  print URL
  return requests.get(URL).text


def GetStockInfoAndInsert(Stock, Year, Month):
    industryCode = GetIndustryCodeFromStockId(Stock)
    response = GetStockTradeInfo(Stock, Year, Month)
    soup = BeautifulSoup(response, 'html.parser')
    try:
        conn = sqlite3.connect(GetDBName(industryCode))
        cur= conn.cursor()
        for row in soup.find_all("tr", bgcolor='#FFFFFF'):
            tds = row.find_all("td")
            if len(tds) == 9:
                try:
                    date = tds[0].get_text().encode('utf8')
                    vol  = tds[1].get_text().encode('utf8').replace(',', '')
                    val  = tds[2].get_text().encode('utf8').replace(',', '')
                    open = tds[3].get_text().encode('utf8').replace(',', '')
                    high = tds[4].get_text().encode('utf8').replace(',', '')
                    low  = tds[5].get_text().encode('utf8').replace(',', '')
                    end  = tds[6].get_text().encode('utf8').replace(',', '')
                    diff = tds[7].get_text().encode('utf8').replace(',', '')
                    tc   = tds[8].get_text().encode('utf8').replace(',', '')
                    #print Stock, date, vol, val, open, high, low, end, tc
                    try:
                        CMD1="INSERT INTO Id" + Stock + " (Date, TradingVolume, TradingValue, OpeningPrice, HighestPrice, FloorPrice, ClosingPrice, DifferencePrices, TradingCount) VALUES (?,?,?,?,?,?,?,?,?)"
                        #print CMD1
                        cur.execute(CMD1,(date, vol, val, open, high, low, end, diff,tc))
                    except:
                        CMD1="UPDATE Id" + Stock + " set TradingVolume=" + vol + ", TradingValue=" + val + ", OpeningPrice=" + open + ", HighestPrice=" + high + ", FloorPrice=" + low + ", ClosingPrice=" + end + " , DifferencePrices=" + diff + " , TradingCount=" + tc + " WHERE Date = '" + date + "'"
                        #print CMD1
                        cur.execute(CMD1)
                        continue
                    
                except:
                    #print "Can't insert table Id" + Stock
                    continue
            else:
                print "HERE"
                continue
        conn.commit()
        conn.close()
    except:
        print "Can't connect to database " + GetDBName(industryCode) + "."
        return

def GetBigThreeTrandingInfo(Year, Month, Day, IndustryCode):
    payload = { 
        "Host": "http://www.twse.com.tw",
        "Referer": "http://www.twse.com.tw/ch/trading/fund/T86/T86.php",
        "input_date": "104/12/04",
        "select2": "02",
        "sorting": "by_issue",
        "login_btn": "+%ACd%B8%DF+-body"
    }
    YYYMMDD=Year + "/" + Month + "/" + Day
    #print "YYYMMDD = " + YYYMMDD
    payload['input_date'] = YYYMMDD
    if len(IndustryCode) != 0:
        payload['select2']=IndustryCode
    URL="http://www.twse.com.tw/ch/trading/fund/T86/T86.php"
    response = requests.post(URL, data=payload)
    if response.status_code == requests.codes.ok:
        #print response
        print "POST data ", payload," to " + URL + " OK"
        soup = BeautifulSoup(response.text, 'html.parser')
        for row in soup.find_all("tr", bgcolor='#FFFFFF'):
            tds = row.find_all("td")
            if len(tds) == 11:
                try:
                    sid = tds[0].get_text().encode('utf8')
                    cname = tds[1].get_text().encode('latin1','ignore').decode('big5')
                    fin = tds[2].get_text().encode('utf8').replace(',', '')
                    fout = tds[3].get_text().encode('utf8').replace(',', '')
                    tin = tds[4].get_text().encode('utf8').replace(',', '')
                    tout = tds[5].get_text().encode('utf8').replace(',', '')
                    sin = tds[6].get_text().encode('utf8').replace(',', '')
                    sout = tds[7].get_text().encode('utf8').replace(',', '')
                    shin = tds[8].get_text().encode('utf8').replace(',', '')
                    shout = tds[9].get_text().encode('utf8').replace(',', '')
                    total = tds[10].get_text().encode('utf8').replace(',', '')
                    #print sid, cname, fin, fout, tin, tout, sin, sout, shin, shout, total
                    dbname = "Industry" + GetIndustryCodeFromStockId(sid) + ".db"
                    conn = sqlite3.connect(dbname)
                    cur= conn.cursor()
                    try:
                        CMD="INSERT INTO Id" + sid + " ( Date, ForeignBuying , ForeignSell , InvestmentBuy , InvestmentSell , DealersBuy , DealersSell , DealersBuyHedging , DealersSellHedging , TotalTradingVolume  ) VALUES (?,?,?,?,?,?,?,?,?,?)"
                        #print CMD
                        cur.execute(CMD,( YYYMMDD, fin, fout, tin, tout, sin, sout, shin, shout, total))
                    except:
                        CMD="UPDATE Id" + sid + " set ForeignBuying="+ fin + ", ForeignSell=" + fout + ", InvestmentBuy=" + tin + ", InvestmentSell=" + tout + ", DealersBuy=" + sin + ", DealersSell=" + sout + " , DealersBuyHedging=" + shin + " , DealersSellHedging=" + shout + ", TotalTradingVolume=" + total + " WHERE Date = '" + YYYMMDD + "'"
                        #print CMD
                        cur.execute(CMD)
                    conn.commit()
                    conn.close()
                except:
                    #print "GetBigThreeTrandingInfo: Can't get " + Stock + " Info."
                    continue
            else:
                continue
    else:
        print "Can't POST dat to " + URL + "."


Initial()

#for stockId industry13
#  for year in 2013,2014,2015
#    for month [1,2,3,4,5,6,7,8,9,10,11,12]
twseIndustryCode = TWSENo().industry_code
twseIndustryComps = TWSENo().industry_comps

for stockId in ['3596']:
    for year in ['2015']:
        for month in ['11']:
            #GetStockInfoAndInsert(stockId, year, month)
            #time.sleep(2)
            print stockId, year, month

#for year in ['102','103','104']:
for year in ['104']:
  #for month in ['01','02','03','04','05','06','07','08','09','10','11','12']:
  for month in ['12']:
    for day in ['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31']:
        GetBigThreeTrandingInfo(year,month,day,'27')
