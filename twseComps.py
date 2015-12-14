#!/usr/bin/python
#-*- coding: utf-8 -*-
import sqlite3
import datetime
import requests
import json
from bs4 import BeautifulSoup
from grs import TWSENo

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
            cur.execute("create table Id" + stockId + " ( Date TEXT PRIMARY KEY, TradingVolume REAL, TradingValue REAL, OpeningPrice REAL, HighestPrice REAL, FloorPrice REAL, ClosingPrice REAL, DifferencePrices REAL, TradingCount REAL, ForeignBuying REAL, ForeignSell REAL, InvestmentBuy REAL, InvestmentSell REAL, DealersBuy REAL, DealersSell REAL, DealersBuyHedging REAL, DealersSellHedging REAL, TotalTradingVolume REAL)")
            print "create table Id" + stockId + " DONE"
          except:
            continue
        conn.close()

def GetDBName (IndustryCode):
  dbname = "Industry" + IndustryCode + ".db"
  print "DBName = " + dbname + "."
  return dbname

  
def GetIndustryCodeFromStockId(StockId):
  Comps = TWSENo().industry_comps
  for IndustryCode in TWSENo().industry_code:
    try:
      if len(Comps[str(IndustryCode)])>0:
        if StockId in Comps[str(IndustryCode)]:
          print "Found " + StockId + " in " + IndustryCode
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
                    vol  = tds[1].get_text().encode('utf8')
                    val  = tds[2].get_text().encode('utf8')
                    open = tds[3].get_text().encode('utf8')
                    high = tds[4].get_text().encode('utf8')
                    low  = tds[5].get_text().encode('utf8')
                    end  = tds[6].get_text().encode('utf8')
                    diff = tds[7].get_text().encode('utf8')
                    tc   = tds[7].get_text().encode('utf8')
                    print date, vol, val, open, high, low, end, tc
                    cur.execute("INSERT INTO Id" + Stock + " (Date , TradingVolume , TradingValue , OpeningPrice , HighestPrice , FloorPrice , ClosingPrice , DifferencePrices , TradingCount) VALUES (?,?,?,?,?,?,?,?,?)",(date, vol, val, open, high, low, end, diff,tc))
                except:
                    print "Can't insert table Id" + Stock
                    continue
            else:
                print "HERE"
                continue
        conn.commit()
        conn.close()
    except:
        print "Can't connect to database " + GetDBName(industryCode) + "."
        return

def GetBigThreeTrandingInfo(Stock, Year, Month):
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
                    vol  = tds[1].get_text().encode('utf8')
                    val  = tds[2].get_text().encode('utf8')
                    open = tds[3].get_text().encode('utf8')
                    high = tds[4].get_text().encode('utf8')
                    low  = tds[5].get_text().encode('utf8')
                    end  = tds[6].get_text().encode('utf8')
                    diff = tds[7].get_text().encode('utf8')
                    tc   = tds[7].get_text().encode('utf8')
                    print date, vol, val, open, high, low, end, tc
                    cur.execute("INSERT INTO Id" + Stock + " (Date , TradingVolume , TradingValue , OpeningPrice , HighestPrice , FloorPrice , ClosingPrice , DifferencePrices , TradingCount) VALUES (?,?,?,?,?,?,?,?,?)",(date, vol, val, open, high, low, end, diff,tc))
                except:
                    print "Can't insert table Id" + Stock
                    continue
            else:
                print "HERE"
                continue
        conn.commit()
        conn.close()
    except:
        print "Can't connect to database " + GetDBName(industryCode) + "."
        return


Initial()
GetStockInfoAndInsert('3596', '2015', '12')
