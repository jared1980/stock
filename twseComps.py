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
    # GET /ch/trading/exchange/STOCK_DAY/genpage/Report201506/201506_F3_1_8_3596.php?STK_NO=3596&myear=2015&mmon=06 HTTP/1.1
    # Host: www.twse.com.tw
    # Connection: keep-alive
    # Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8
    # Upgrade-Insecure-Requests: 1
    # User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36
    # Referer: http://www.twse.com.tw/ch/trading/exchange/STOCK_DAY/STOCK_DAY.php
    # Accept-Encoding: gzip, deflate, sdch
    # Accept-Language: zh-TW,zh;q=0.8,zh-CN;q=0.6,en;q=0.4,de;q=0.2,ja;q=0.2
    # Cookie: __utma=193825960.1668213010.1450343217.1450544037.1450601202.8; __utmc=193825960; __utmz=193825960.1450343217.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)
    URL="http://www.twse.com.tw/ch/trading/exchange/STOCK_DAY/genpage/Report%s%02d/%s%02d_F3_1_8_%s.php?STK_NO=%s&myear=%s&mmon=%02d" % ( Year,Month,Year,Month,StockId, StockId, Year,Month)
    print URL
    return requests.get(URL).text

def GetStockInfoAndInsert(Stock, Year, Month):
    industryCode = GetIndustryCodeFromStockId(Stock)
    response = GetStockTradeInfo(Stock, Year, Month)
    soup = BeautifulSoup(response, 'html.parser')
    try:
        print "DBName %s" %(GetDBName(industryCode))
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
                    print Stock, date, vol, val, openning, high, low, end, tc
                    # Date , TradingVolume , TradingValue , OpeningPrice , HighestPrice , FloorPrice , ClosingPrice , DifferencePrices , TradingCount ,
                    # ForeignBuying , ForeignSell ,
                    # InvestmentBuy , InvestmentSell ,
                    # DealersBuy , DealersSell ,
                    # DealersBuyHedging , DealersSellHedging ,
                    # TotalTradingVolume
                    CMD1="INSERT OR REPLACE INTO Id%s (Date, TradingVolume, TradingValue, OpeningPrice, HighestPrice, FloorPrice, ClosingPrice, DifferencePrices, TradingCount, ForeignBuying, ForeignSell, InvestmentBuy, InvestmentSell, DealersBuy, DealersSell, DealersBuyHedging, DealersSellHedging, TotalTradingVolume) VALUES ('%s',%s,%s,%s,%s,%s,%s,%s,%s,(SELECT ForeignBuying FROM Id%s WHERE Date ='%s'),(SELECT ForeignSell FROM Id%s WHERE Date ='%s'),(SELECT InvestmentBuy FROM Id%s WHERE Date ='%s'),(SELECT InvestmentSell FROM Id%s WHERE Date ='%s'),(SELECT DealersBuy FROM Id%s WHERE Date ='%s'),(SELECT DealersSell FROM Id%s WHERE Date ='%s'),(SELECT DealersBuyHedging FROM Id%s WHERE Date ='%s'),(SELECT DealersSellHedging FROM Id%s WHERE Date ='%s'),(SELECT TotalTradingVolume FROM Id%s WHERE Date ='%s'))" % (Stock, date, vol, val, openning, high, low, end, diff, tc,Stock, date,Stock, date,Stock, date,Stock, date,Stock, date,Stock, date,Stock, date,Stock, date,Stock, date)
                    print CMD1 
                    try:
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
  # input_date=104%2F06%2F08&select2=ALLBUT0999&sorting=by_issue&login_btn=+%ACd%B8%DF+
    payload = { 
        "Host": "http://www.twse.com.tw",
        "Referer": "http://www.twse.com.tw/ch/trading/fund/T86/T86.php",
        "input_date": "104/12/04",
        "select2": "ALLBUT0999",
        "sorting": "by_issue",
        "login_btn": "+%ACd%B8%DF+-body"
    }
    date="%d/%02d/%02d" %(Year-1911, Month, Day)
    print "YYYMMDD = " + date
    payload['input_date'] = date
    #payload['select2'] = '27'
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
    yyyymmdd = []
    try:
        fo = open("LastModifiedDate.txt", "r")
        str = fo.read(10)
        a=str.split('/')
        yyyymmdd.append(int(a[0]))
        yyyymmdd.append(int(a[1]))
        yyyymmdd.append(int(a[2]))
    except:
        yyyymmdd.append(2015)
        yyyymmdd.append(1)
        yyyymmdd.append(1)
    return yyyymmdd

def GetTodayYYYYMMDD():
    YYYYMMDDOfToday = []
    YYYYMMDDOfToday.append(dtime.date.today().year)
    YYYYMMDDOfToday.append(dtime.date.today().month)
    YYYYMMDDOfToday.append(dtime.date.today().day)
    #YYYYMMDDOfToday.append(2015)
    #YYYYMMDDOfToday.append(9)
    #YYYYMMDDOfToday.append(31)
    return YYYYMMDDOfToday

def SetLastModifiedDate(yyyymmdd):
    try:
        fo = open("LastModifiedDate.txt", "w+")
        written="%s/%s/%s\n" %(yyyymmdd[0],yyyymmdd[1],yyyymmdd[2])
        print written
        fo.write(written)
        fo.closed()
    except:
        print "Can't SetLastModifiedDate"

def GetStockTradingInfoFrom(StockId, YearOfStart, MonthOfStart):
    for year in range(YearOfStart, YYYYMMDDOfToday[0]+1, 1):
        for month in range(1, 13, 1):
            if (year == YearOfStart) and (month < MonthOfStart):
                #print "Skip %s/%s" %(year, month)
                continue
            if (year == YYYYMMDDOfToday[0]) and (month > YYYYMMDDOfToday[1]):
                #print "Skip %s/%s" %(year, month)
                return
            else:
                print ""
                print "Get %s Info of year: %s and month: %s." %(StockId, year, month)
                GetStockInfoAndInsert(StockId, year, month)
                


def GetYearDataBigThree( Year, Month, Day):
    print "GetYearDataBigThree(%s, %s, %s)" %(Year, Month, Day)
    for year in range (Year, YYYYMMDDOfToday[0]+1, 1):
        for month in range(1,13,1):
            if (year==Year) and (month<YYYYMMDDOfStart[1]):
                print "Skip %s/%s" %(year, month)
                continue
            elif (year==YYYYMMDDOfToday[0]) and (month>YYYYMMDDOfToday[1]):
                print "Skip %s/%s" %(year, month)
                return
            else:
                monthrange=calendar.monthrange(year, month)
                for day in range(1,monthrange[1]+1,1):
                    if (year==Year) and (month==YYYYMMDDOfStart[1]) and (day < YYYYMMDDOfStart[2]):
                        print "Skip %s/%s/%s" %(year, month, day)
                        continue
                    elif (year==YYYYMMDDOfToday[0]) and (month==YYYYMMDDOfToday[1]) and (day > YYYYMMDDOfToday[2]):
                        print "Skip %s/%s/%s" %(year, month, day)
                        return
                    elif (GetTWSEIsOpen(year, month, day)!=True):
                        print "TWSE not open: %s/%s/%s" %(year, month, day)
                        continue
                    else:
                        print "Get Big three Trading Info on %s/%s/%s" % (year, month, day)
                        GetBigThreeTrandingInfo(year,month,day)


def GetAllStockTradingInfoFrom(YYYY, MM):
    Code = TWSENo().industry_code
    Comps= TWSENo().industry_comps
    for code in Code.keys():
        print "Industry Code: %s, %s" %(code, Code.get(code))
        try:
            if (len(Comps[str(code)]) > 0):
                for stock in Comps[str(code)]:
                    #print "Stock: " + stock
                    GetStockTradingInfoFrom(stock, YYYY, MM)
                print "Industry Code: %s is not empty" %(code)
        except:
            print "Industry Code: %s is empty" %(code)
            
def printIndustryCodeAndTheirStocks():
    fo = open("IndustryCodesAndTheirStocks.txt", "wb")
    Code = TWSENo().industry_code
    Codes = Code.keys()
    Codes.sort()
    Comps= TWSENo().industry_comps
    for code in Codes:
        #print "Industry Code: %s, %s" %(code, Code.get(code))
        a="Industry Code: %s, %s\n" %(code, Code.get(code))
        #fo.write("Industry Code: " + str(code) + ": " + Code.get(code) )
        fo.write(a.encode("utf8"))
        try:
            if (len(Comps[str(code)]) > 0):
                for stock in Comps[str(code)]:
                    print stock
                    fo.write(str(stock) + " ")
                fo.write("\n\n");
                #print "Industry Code: %s is not empty" %(code)
        except:
            print "Industry Code: %s is empty" %(code)
            fo.write("Industry Code: " + str(code) + " is empty\n")
    fo.close()

if __name__ == '__main__':
    Initial()
    YYYYMMDDOfStart = GetLastModifiedDate()
    YYYYMMDDOfToday = GetTodayYYYYMMDD()
    SetLastModifiedDate(YYYYMMDDOfToday)
    print "Start day: %s" %(YYYYMMDDOfStart)
    print "Today: %s" %(YYYYMMDDOfToday)

    #printIndustryCodeAndTheirStocks()
    GetYearDataBigThree(YYYYMMDDOfStart[0],YYYYMMDDOfStart[1],YYYYMMDDOfStart[2])
    GetAllStockTradingInfoFrom(YYYYMMDDOfStart[0],YYYYMMDDOfStart[1])
    
