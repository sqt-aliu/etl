#!/opt/anaconda3/bin/python -u

import getopt
import os
import pandas as pd
import numpy as np
import sys
from sqlalchemy import create_engine, exc
from datetime import datetime
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn

def insert_quotes(inputfile, dbconn):
     
    try:
        info("Creating MariaDB Engine...")
        db = create_engine(dbconn, echo=False)    
        conn = db.connect()  
        dateparse = lambda x: pd.datetime.strptime(x, "%Y%m%d").date()
        info("Reading file [%s]" % (inputfile))
        dfCSV = pd.read_csv(inputfile, parse_dates=['date'], date_parser=dateparse)
        for index, row in dfCSV.iterrows():
            try:
                sql = record_builder(row, "REPLACE")
                info("Executing query [%s]" % (sql))
                #db.execute(sql)   
            except exc.IntegrityError as e:
                error("DB Integrity Error: %s, sql=%s" % (e, sql))  
            except exc.SQLAlchemyError as e:
                error("DB SQLAlchemy Error: %s, sql=%s" % (e, sql))
        conn.close()
    except exc.SQLAlchemyError as e:
        error("Database Error: %s, conn=%s" % (e, dbconn))   
    except:
        error("Unknown Error: %s, conn=%s" % (sys.exc_info()[0], dbconn))   
        error(str(sys.exc_info()))
        
def record_builder(row, query="INSERT"):
    sql = "%s INTO prices VALUES ('%s','%s',%s,%s,%s,%s,%s,'O')" % (query, row['ticker'],
        datetime.strftime(row['date'], '%Y-%m-%d'), 
        'NULL' if np.isnan(row['open']) else round(row['open'], 3),
        'NULL' if np.isnan(row['high']) else round(row['high'], 3),
        'NULL' if np.isnan(row['low']) else round(row['low'], 3),
        'NULL' if np.isnan(row['close']) else round(row['close'], 3),   
        'NULL' if np.isnan(row['volume']) else int(row['volume']))
    return (sql)
    
def record_quotes(inputpath, dbconn):
    override_file = "%s/stockquotes.csv" % (inputpath)
    if os.path.exists(override_file):
        insert_quotes(override_file, dbconn)
    else:
        fatal("Override file is missing [%s]" % (override_file))   
           
def print_usage():
    print  ("  Usage: %s [options]" % (os.path.basename(__file__))) 
    print  ("  Options:")
    print  ("  \t-i, --input\tinput directory")
    print  ("  \t-d, --database\tdatabase connection string") 
    print  ("  \t-h,\t\thelp")
    
def main(argv):  
    argInput = "/dfs/stage/override/"
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
                
    record_quotes(argInput, argDBConn)
  

if __name__ == '__main__':
    main(sys.argv[1:])                