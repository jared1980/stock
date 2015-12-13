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
            cur.execute("create table Id" + stockId + " ( code text, cname text, fbuy real, fsell real, tbuy real, tsell real, sbuy real, ssell real)")
            print "create table Id" + stockId + " DONE"
          except:
            continue
        conn.close()

def GetDBName (IndustryCode):
  return "Industry" + IndustryCode + ".db"

  
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
      print "Comps with key " + IndustryCode + " empty"
      continue


def Initial():
  twseIndustryCode = TWSENo().industry_code
  twseIndustryComps = TWSENo().industry_comps
  for Code in twseIndustryComps:
    if len(twseIndustryComps[str(Code)])>0:
      CreateDatabaseAndTable( GetDBName(Code), twseIndustryComps[str(Code)])
    else:
      print "Industry" + Code + " has empty comps"
      continue

def GetStockTradeInfo(StockId, Year, Month):
  URL="http://www.twse.com.tw/ch/trading/exchange/STOCK_DAY/STOCK_DAY.php"

Initial()
GetIndustryCodeFromStockId('3596')
