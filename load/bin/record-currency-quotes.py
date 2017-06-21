#!/opt/anaconda3/bin/python -u

import getopt
import os
import pandas as pd
import requests
import sys
from sqlalchemy import create_engine, exc
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from xml.dom import minidom
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn
from extract.lib.investing import InvestingHistorical

def read_config(config):
    info("Reading file " + config)
    config_map = {}
    xmldoc = minidom.parse(config)
    for indexNode in xmldoc.getElementsByTagName('currency'):
        indexKey = indexNode.getAttribute('key')
        indexVal = indexNode.getAttribute('value')
        config_map[indexKey] = indexVal
        
    return (config_map)
       
def download_quotes(ticker, start=None, end=None, iteration=1, max_tries=2):
    quotes = {}
    payload = {'action': 'historical_data', 
           'curr_id': ticker,
           'interval_sec': 'Daily'}
    if not start is None:
        payload['st_date'] = datetime.strftime(start, '%m/%d/%Y')
    if not end is None:
        payload['end_date'] = datetime.strftime(end, '%m/%d/%Y')

    headers = {'Host': 'www.investing.com',
           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:53.0) Gecko/20100101 Firefox/53.0',
           'Accept': 'text/plain, */*; q=0.01',
           'Accept-Language': 'en-US,en;q=0.5',
           'Accept-Encoding': 'gzip, deflate, br',
           'Referer': 'https://www.investing.com/',
           'Content-Type': 'application/x-www-form-urlencoded',
           'X-Requested-With': 'XMLHttpRequest',
           'Content-Length': '101',
           'Connection': 'keep-alive',
           'Pragma': 'no-cache',
           'Cache-Control': 'no-cache'
            }

    url = "https://www.investing.com/instruments/HistoricalDataAjax"
    info("Requesting url [%s][%s]" % (url, ticker))
    r = requests.post(url, data=payload, headers=headers)
    htmlParser = InvestingHistorical()
    htmlParser.feed(r.text)
    quotes.update(htmlParser.data)
    return(quotes)
        
def insert_quotes(ticker, dbconn, quotes):
    if len(quotes) > 0:
        try:
            info("Creating MariaDB Engine...")
            db = create_engine(dbconn, echo=False)    
            conn = db.connect()  
            
            sql = "Select * From prices Where ticker = '%s'" % (ticker)
            info("Executing query [%s]" % (sql))
            df = pd.read_sql_query(sql, conn, index_col='date')
            for quote_date, quote_vals in sorted(quotes.items()):
                if quote_date.date() in df.index:
                    info("Record already exists [%s][%s]...skipping" % (ticker, quote_date))
                else:
                    try:
                        sql = "INSERT INTO prices VALUES ('%s','%s',%s,%s,%s,%s,'G')" % (ticker, datetime.strftime(quote_date, '%Y-%m-%d'), quote_vals['OP'], quote_vals['HI'], quote_vals['LO'], quote_vals['CL'])
                        info("Executing query [%s]" % (sql))
                        db.execute(sql)   
                    except exc.IntegrityError as e:
                        error("DB Integrity Error: %s, sql=%s" % (e, sql))  
                    except exc.SQLAlchemyError as e:
                        error("DB SQLAlchemy Error: %s, sql=%s" % (e, sql))   
            conn.close()
        except exc.SQLAlchemyError as e:
            error("Database Error: %s, conn=%s" % (e, dbconn))   
        except:
            error("Unknown Error: %s, conn=%s" % (sys.exc_info()[0], dbconn))   
    else:
        warn("No quotes found for ticker %s" % (ticker))
        
def record_quotes(config, dbconn, batch=False):
    index_map = read_config(config)
    for k,v in sorted(index_map.items()):
        if batch:
            current_date = datetime.strptime('1980-01-01', '%Y-%m-%d')
            while True:
                end_date = (current_date + relativedelta(months=6)) - timedelta(days=1)
                #start = current_date.strftime("%b %d, %Y")
                #end = end_date.strftime("%b %d, %Y")    
                insert_quotes(k, dbconn, download_quotes(v, current_date, end_date))
                if end_date > datetime.today():
                    break
                
                current_date = end_date + timedelta(days=1)
        else:
            insert_quotes(k, dbconn, download_quotes(v))
           
def print_usage():
    print  ("  Usage: %s [options]" % (os.path.basename(__file__))) 
    print  ("  Options:")
    print  ("  \t-c, --config\tconfiguration file")
    print  ("  \t-d, --database\tdatabase connection string")
    print  ("  \t-b, --batch\tbatch mode")
    print  ("  \t-h,\t\thelp")
    
def main(argv):
    argCfg = ""
    argDBConn = ""
    optBatch = False
    
    try:
        opts, args = getopt.getopt(argv,"hbc:d:",["batch","config=","database="])
    except getopt.GetoptError:
        print_usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print_usage()
            sys.exit()                   
        elif opt in ("-c", "--config"):
            argCfg = arg
        elif opt in ("-d", "--database"):
            argDBConn = arg   
        elif opt in ("-b", "--batch"):
            optBatch = True            

    if len(argDBConn) == 0 or len(argCfg) == 0:
        print_usage()
        sys.exit(0)
        
    if not os.path.exists(argCfg):
        fatal("File %s not found" % (argCfg))
        
    record_quotes(argCfg, argDBConn, optBatch)
  

if __name__ == '__main__':
    main(sys.argv[1:])                