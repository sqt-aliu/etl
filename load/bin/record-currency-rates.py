#!/opt/anaconda3/bin/python -u

import getopt
import os
import sys
from time import sleep
from sqlalchemy import create_engine, exc
from datetime import datetime, timedelta
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
from xml.dom import minidom
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn
from extract.lib.xe import XEHistorical
from common.lib.cal import calendar_days

def read_config(config):
    info("Reading file " + config)
    config_map = {}
    xmldoc = minidom.parse(config)
    for indexNode in xmldoc.getElementsByTagName('rate'):
        indexKey = indexNode.getAttribute('key')
        config_map[indexKey] = indexKey
        
    return (config_map)
       
def download_rates(hdate, iteration=1, max_tries=2):
    url = "http://www.xe.com/currencytables/?from=USD&date=%s" % (datetime.strftime(hdate, '%Y-%m-%d'))
    rates = {}
    
    try:
        info("Requesting url [%s]" % (url))
        urlResp = urlopen(url, timeout=30)
        html = urlResp.read()
        urlResp.close()
        
        htmlparser = XEHistorical()
        htmlparser.feed(str(html))
        rates.update(htmlparser.data)    
    except HTTPError as e:
        warn("HTTP Error: %s, url=%s, attempt=%i" % (str(e.code), url, iteration))
        if iteration <= max_tries:
            download_rates(hdate, iteration=iteration+1)           
    except URLError as e:
        warn("URL Error: %s, url=%s, attempt=%i" % (e.reason, url, iteration))
        if iteration <= max_tries:
            download_rates(hdate, iteration=iteration+1)                
    except OSError as e:
        warn("OS Error: %s, url=%s, attempt=%i" % (e, url, iteration))      
        if iteration <= max_tries:
            download_rates(hdate, iteration=iteration+1)             
    except:
        warn("Unknown Error: %s, url=%s, attempt=%i" % (sys.exc_info()[0], url, iteration))   
        if iteration <= max_tries:
            download_rates(hdate, iteration=iteration+1)  
    return (rates)
    
def insert_rates(idate, currencies, dbconn, rates):
    if len(rates) > 0:
        try:
            info("Creating MariaDB Engine...")
            db = create_engine(dbconn, echo=False)    
            conn = db.connect()  
            for ticker, rate in sorted(rates.items()):
                if ticker in currencies:
                    try:
                        sql = "REPLACE INTO rates VALUES ('%s','%s',%s,'X')" % (ticker, datetime.strftime(idate, '%Y-%m-%d'), rate)
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
        warn("No quotes found for date %s" % (datetime.strftime(idate, '%Y-%m-%d')))
        
def record_quotes(config, dbconn, batch=False):
    rates_map = read_config(config)
    start_date = datetime.today() - timedelta(days=7)
    if batch:
        start_date = datetime.strptime('1995-11-16', '%Y-%m-%d')
        
    end_date = datetime.today()
    cal_dates = calendar_days(start_date, end_date)
    for cal_date in cal_dates:
        insert_rates(cal_date, sorted(rates_map.keys()), dbconn, download_rates(cal_date))
        sleep(0.1)

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