#!/usr/bin/python
#-*- coding: utf-8 -*-
import sqlite3
import datetime as dtime
from datetime import datetime
import requests
import json
from bs4 import BeautifulSoup
from grs import TWSENo
from grs import TWTime
from grs import TWSEOpen
import time
import calendar

def GetTWSEIsOpen(YYYY, MM, DD):
    return  TWSEOpen().d_day(datetime(int(YYYY),int(MM),int(DD)))
    
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
            print "create table Id" + stockId + " DONE"
          except:
            continue
        conn.close()

def GetDBName (IndustryCode):
  dbname = "Industry" + IndustryCode + ".db"
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
          #print "Stock ID: " + StockId + " isn't in Industry Code: " + IndustryCode
          continue
    except:
      continue
  #print "Stock Id: " + StockId + " isn't found in any Industry Code!!!!!!"
  return '0'

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
  URL="http://www.twse.com.tw/ch/trading/exchange/STOCK_DAY/genpage/Report%s%s/%s%s_F3_1_8_%s.php?STK_NO=%s&myear=%s&mmon=%s" % ( (Year-1911),Month,(Year-1911),Month,StockId, StockId, (Year-1911),Month)
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
                    openning = tds[3].get_text().encode('utf8').replace(',', '')
                    high = tds[4].get_text().encode('utf8').replace(',', '')
                    low  = tds[5].get_text().encode('utf8').replace(',', '')
                    end  = tds[6].get_text().encode('utf8').replace(',', '')
                    diff = tds[7].get_text().encode('utf8').replace(',', '')
                    tc   = tds[8].get_text().encode('utf8').replace(',', '')
                    # print Stock, date, vol, val, open, high, low, end, tc
                    # Date , TradingVolume , TradingValue , OpeningPrice , HighestPrice , FloorPrice , ClosingPrice , DifferencePrices , TradingCount ,
                    # ForeignBuying , ForeignSell ,
                    # InvestmentBuy , InvestmentSell ,
                    # DealersBuy , DealersSell ,
                    # DealersBuyHedging , DealersSellHedging ,
                    # TotalTradingVolume 
                    try:
                        CMD1="INSERT OR REPLACE INTO Id%s (Date, TradingVolume, TradingValue, OpeningPrice, HighestPrice, FloorPrice, ClosingPrice, DifferencePrices, TradingCount, ForeignBuying, ForeignSell, InvestmentBuy, InvestmentSell, DealersBuy, DealersSell, DealersBuyHedging, DealersSellHedging, TotalTradingVolume) VALUES ('%s',%s,%s,%s,%s,%s,%s,%s,%s,(SELECT ForeignBuying FROM Id%s WHERE Date ='%s'),(SELECT ForeignSell FROM Id%s WHERE Date ='%s'),(SELECT InvestmentBuy FROM Id%s WHERE Date ='%s'),(SELECT InvestmentSell FROM Id%s WHERE Date ='%s'),(SELECT DealersBuy FROM Id%s WHERE Date ='%s'),(SELECT DealersSell FROM Id%s WHERE Date ='%s'),(SELECT DealersBuyHedging FROM Id%s WHERE Date ='%s'),(SELECT DealersSellHedging FROM Id%s WHERE Date ='%s'),(SELECT TotalTradingVolume FROM Id%s WHERE Date ='%s'))" % (Stock, date, vol, val, openning, high, low, end, diff, tc,Stock, date,Stock, date,Stock, date,Stock, date,Stock, date,Stock, date,Stock, date,Stock, date,Stock, date)
                        #print CMD1 
                        cur.execute(CMD1)
                    except:
                        print "Can't insert or replace "
                        continue
                    
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

def GetBigThreeTrandingInfo(Year, Month, Day):
    payload = { 
        "Host": "http://www.twse.com.tw",
        "Referer": "http://www.twse.com.tw/ch/trading/fund/T86/T86.php",
        "input_date": "104/12/04",
        "select2": "ALLBUT0999",
        "sorting": "by_issue",
        "login_btn": "+%ACd%B8%DF+-body"
    }
    date="%s/%s/%s" %(Year-1911, Month, Day)
    #print "YYYMMDD = " + date
    payload['input_date'] = date
    payload['select2'] = '27'
    URL="http://www.twse.com.tw/ch/trading/fund/T86/T86.php"
    #print payload
    response = requests.post(URL, data=payload)
    if response.status_code == requests.codes.ok:
        #print response
        #print "POST data ", payload," to " + URL + " OK"
        soup = BeautifulSoup(response.text, 'html.parser')
        for row in soup.find_all("tr", bgcolor='#FFFFFF'):
            tds = row.find_all("td")
            #print len(tds)
            if len(tds) == 11:
                try:
                    Stock = tds[0].get_text().encode('utf8')
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
                    IndustryCode = GetIndustryCodeFromStockId(Stock)
                    if len(IndustryCode) <= 1:
                        print "Can't get IndustryCode for StockID: " + Stock + "."
                        print "Don't update info into database: "
                        continue
                    dbname = "Industry" + IndustryCode + ".db"
                    print "Update Info: %s %s %s %s %s %s %s %s %s %s %s into database %s" %(Stock, cname, fin, fout, tin, tout, sin, sout, shin, shout, total, dbname)
                    #print "DBName=" + dbname
                    conn = sqlite3.connect(dbname)
                    cur= conn.cursor()
                    try:
                        CMD1="INSERT OR REPLACE INTO Id%s (Date, TradingVolume, TradingValue, OpeningPrice, HighestPrice, FloorPrice, ClosingPrice, DifferencePrices, TradingCount,ForeignBuying, ForeignSell,InvestmentBuy, InvestmentSell,DealersBuy, DealersSell,DealersBuyHedging, DealersSellHedging,TotalTradingVolume) VALUES ('%s',(SELECT TradingVolume FROM Id%s WHERE Date ='%s'),(SELECT TradingValue FROM Id%s WHERE Date ='%s'),(SELECT OpeningPrice FROM Id%s WHERE Date ='%s'),(SELECT HighestPrice FROM Id%s WHERE Date ='%s'),(SELECT FloorPrice FROM Id%s WHERE Date ='%s'),(SELECT ClosingPrice FROM Id%s WHERE Date ='%s'),(SELECT DifferencePrices FROM Id%s WHERE Date ='%s'),(SELECT TradingCount FROM Id%s WHERE Date ='%s'),%s,%s,%s,%s,%s,%s,%s,%s,%s)" % (Stock,date,Stock, date,Stock, date,Stock, date,Stock, date,Stock, date,Stock, date,Stock, date,Stock, date,fin,fout,tin,tout,sin,sout,shin,shout,total)
                        #print CMD1
                        cur.execute(CMD1)
                        conn.commit()
                        conn.close()
                    except:
                        print "sqlite error..."
                        continue
                        
                except:
                    continue
            elif len(tds) == 9:
                try:
                    #print tds
                    Stock = tds[0].get_text().encode('utf8')
                    cname = tds[1].get_text().encode('latin1','ignore').decode('big5')
                    fin = tds[2].get_text().encode('utf8').replace(',', '')
                    fout = tds[3].get_text().encode('utf8').replace(',', '')
                    tin = tds[4].get_text().encode('utf8').replace(',', '')
                    tout = tds[5].get_text().encode('utf8').replace(',', '')
                    sin = tds[6].get_text().encode('utf8').replace(',', '')
                    sout = tds[7].get_text().encode('utf8').replace(',', '')
                    shin = 0
                    shout = 0
                    total = tds[8].get_text().encode('utf8').replace(',', '')
                    IndustryCode = GetIndustryCodeFromStockId(Stock)
                    if len(IndustryCode) <= 1:
                        print "Can't get IndustryCode for StockID: " + Stock + "."
                        print "Don't update info into database: "
                        print Stock, cname, fin, fout, tin, tout, sin, sout, shin, shout, total
                        continue
                    dbname = "Industry" + IndustryCode + ".db"
                    print "Update Info: %s %s %s %s %s %s %s %s %s %s %s into database %s" %(Stock, cname, fin, fout, tin, tout, sin, sout, shin, shout, total, dbname)
                    conn = sqlite3.connect(dbname)
                    cur= conn.cursor()
                    try:
                        CMD1="INSERT OR REPLACE INTO Id%s (Date, TradingVolume, TradingValue, OpeningPrice, HighestPrice, FloorPrice, ClosingPrice, DifferencePrices, TradingCount,ForeignBuying, ForeignSell,InvestmentBuy, InvestmentSell,DealersBuy, DealersSell,DealersBuyHedging, DealersSellHedging,TotalTradingVolume) VALUES ('%s',(SELECT TradingVolume FROM Id%s WHERE Date ='%s'),(SELECT TradingValue FROM Id%s WHERE Date ='%s'),(SELECT OpeningPrice FROM Id%s WHERE Date ='%s'),(SELECT HighestPrice FROM Id%s WHERE Date ='%s'),(SELECT FloorPrice FROM Id%s WHERE Date ='%s'),(SELECT ClosingPrice FROM Id%s WHERE Date ='%s'),(SELECT DifferencePrices FROM Id%s WHERE Date ='%s'),(SELECT TradingCount FROM Id%s WHERE Date ='%s'),%s,%s,%s,%s,%s,%s,%s,%s,%s)" % (Stock,date,Stock, date,Stock, date,Stock, date,Stock, date,Stock, date,Stock, date,Stock, date,Stock, date,fin,fout,tin,tout,sin,sout,shin,shout,total)
                        #print CMD1
                        cur.execute(CMD1)
                        conn.commit()
                        conn.close()
                    except:
                        #print "sqlite failed."
                        continue
                except:
                    #print ""
                    #print "2: GetBigThreeTrandingInfo: Can't get Info."
                    print tds
                    #print ""
                    continue
            else:
                #print "len: " + str(len(tds))
                continue
    else:
        print "Can't POST dat to " + URL + "."

def GetLastModifiedDate():
    yyymmdd = []
    try:
        fo = open("LastModifiedDate.txt", "r")
        str = fo.read(10)
        a=str.split('/')
        yyymmdd.append(int(a[0]))
        yyymmdd.append(int(a[1]))
        yyymmdd.append(int(a[2]))
    except:
        yyymmdd.append(2012)
        yyymmdd.append(5)
        yyymmdd.append(2)
    return yyymmdd

def GetTodayYYYMMDD():
    YYYMMDDOfToday = []
    #YYYMMDDOfToday.append(dtime.date.today().year)
    #YYYMMDDOfToday.append(dtime.date.today().month)
    #YYYMMDDOfToday.append(dtime.date.today().day)
    YYYMMDDOfToday.append(2012)
    YYYMMDDOfToday.append(6)
    YYYMMDDOfToday.append(4)
    return YYYMMDDOfToday

def SetLastModifiedDate(yyymmdd):
    try:
        fo = open("LastModifiedDate.txt", "w+")
        written="%s/%s/%s\n" %(yyymmdd[0],yyymmdd[1],yyymmdd[2])
        print written
        fo.write(written)
        fo.closed()
    except:
        print "Can't SetLastModifiedDate"

def GetStockTradingInfoFrom(StockId, YearOfStart, MonthOfStart):
    for year in range(YearOfStart, YYYMMDDOfToday[0]+1, 1):
        for month in range(1, 13, 1):
            if (year == YearOfStart) and (month < MonthOfStart):
                #print "Skip %s/%s" %(year, month)
                continue
            if (year == YYYMMDDOfToday[0]) and (month > YYYMMDDOfToday[1]):
                #print "Skip %s/%s" %(year, month)
                return
            else:
                print "Get %s Info of year: %s and month: %s." %(StockId, year, month)
                GetStockInfoAndInsert(StockId, year, month)
                


def GetYearDataBigThree( Year, Month, Day):
  for year in range (Year, YYYMMDDOfToday[0]+1, 1):
    for month in range(1,13,1):
      if (year==Year) and (month<YYYMMDDOfStart[1]):
        #print "Skip %s/%s" %(year, month)
        continue
      monthrange=calendar.monthrange(year, month)
      for day in range(1,monthrange[1]+1,1):
        if (year==Year) and (month==YYYMMDDOfStart[1]) and (day < YYYMMDDOfStart[2]):
          #print "Skip %s/%s/%s" %(year, month, day)
          continue
        elif (year==YYYMMDDOfToday[0]) and (month==YYYMMDDOfToday[1]) and (day > YYYMMDDOfToday[2]):
          #print "Skip %s/%s/%s" %(year, month, day)
          return
        elif (GetTWSEIsOpen(year, month, day)!=True):
            #print "TWSE not open: %s/%s/%s" %(year, month, day)
            continue
        else:
            print "Get Big three Trading Info on %s/%s/%s" % (year, month, day)
            GetBigThreeTrandingInfo(year,month,day)


def GetAllStockTradingInfoFrom(YYYY, MM):
    twseIndustryCode = TWSENo().industry_code
    twseIndustryComps = TWSENo().industry_comps
    for industryCode in twseIndustryCode:
        print "Industry Code: %s" %(industryCode)
        try:
            if len(twseIndustryComps[str(industryCode)]) > 0:
                for stock in twseIndustryComps[str(industryCode)]:
                    GetStockTradingInfoFrom(stock, YYYY,MM)
        except:
            print "Comps[%s] is empty." %(industryCode)
            continue


if __name__ == '__main__':
    Initial()
    YYYMMDDOfStart = GetLastModifiedDate()
    YYYMMDDOfToday = GetTodayYYYMMDD()
    SetLastModifiedDate(YYYMMDDOfToday)
    print "Start day: %s" %(YYYMMDDOfStart)
    print "Today: %s" %(YYYMMDDOfToday)

    GetYearDataBigThree(YYYMMDDOfStart[0],YYYMMDDOfStart[1],YYYMMDDOfStart[2])
    GetAllStockTradingInfoFrom(YYYMMDDOfStart[0],YYYMMDDOfStart[1])
