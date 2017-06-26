#!/opt/anaconda3/bin/python -u

import getopt
import os
import pandas as pd
import numpy as np
import sys
from sqlalchemy import create_engine, exc
from datetime import datetime, date
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn

def insert_products(inputfile, dbconn):
    try:
        info("Creating MariaDB Engine...")
        db = create_engine(dbconn, echo=False)    
        conn = db.connect()  
        
        sql = "SELECT * FROM products WHERE ticker like '%%.HK'" 
        info("Executing query [%s]" % (sql))
        dfSQL = pd.read_sql_query(sql, conn, index_col='ticker')  
        info("Reading file [%s]" % (inputfile))
        with open(inputfile, 'r', encoding="ISO-8859-1") as fh:
            lines = fh.readlines()[2:] #remove first 2 lines
            for line in lines:
                line = line.strip()
                if len(line) > 0:
                    parts = line.split('\t')
                    if parts[0].startswith('\"') and parts[0].endswith('\"'):
                        ticker = parts[0].strip('\"')
                        ticker = ticker[1:] + ".HK" if ticker.startswith('0') else ticker+ ".HK"
                        if not ticker in dfSQL.index:
                            sql = "INSERT INTO products VALUES ('%s','etf')" % (ticker)
                            try:
                                info("Executing query [%s]" % (sql))
                                db.execute(sql)   
                            except exc.IntegrityError as e:
                                error("DB Integrity Error: %s, sql=%s" % (e, sql))  
                            except exc.SQLAlchemyError as e:
                                error("DB SQLAlchemy Error: %s, sql=%s" % (e, sql))     
                        else:
                            warn("Record already exists for '%s'" % (ticker))

        conn.close()
    except exc.SQLAlchemyError as e:
        error("Database Error: %s, conn=%s" % (e, dbconn))   
    except:
        error("Unknown Error: %s, conn=%s" % (sys.exc_info()[0], dbconn))   
        error(str(sys.exc_info()))
        
def record_products(inputpath, dbconn):
    today = datetime.strftime(date.today(), "%Y%m%d")
    inputfile = inputpath + "/" + today + "/" + today + ".etfs.csv"
    
    if os.path.exists(inputfile):
        insert_products(inputfile, dbconn)
    else:
        warn("No products file found [%s]" % (inputfile))
                 
def print_usage():
    print  ("  Usage: %s [options]" % (os.path.basename(__file__))) 
    print  ("  Options:")
    print  ("  \t-i, --input\tinput directory")
    print  ("  \t-d, --database\tdatabase connection string")
    print  ("  \t-h,\t\thelp")
    
def main(argv):
    argInput = "/dfs/raw/live/hkex/"
    argDBConn = ""
    
    try:
        opts, args = getopt.getopt(argv,"hi:d:",["input=","database="])
    except getopt.GetoptError:
        print_usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print_usage()
            sys.exit()     
        elif opt in ("-i", "--input"):
            argInput = arg                 
        elif opt in ("-d", "--database"):
            argDBConn = arg   

    if len(argDBConn) == 0 or len(argInput) == 0:
        print_usage()
        sys.exit(0)
                
    record_products(argInput, argDBConn)
  
if __name__ == '__main__':
    main(sys.argv[1:])                