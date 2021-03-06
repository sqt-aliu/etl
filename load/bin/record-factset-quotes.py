#!/opt/anaconda3/bin/python -u

import getopt
import glob
import os
import pandas as pd
import sys
from sqlalchemy import create_engine, exc, DATE
from datetime import datetime, date, timedelta
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn
        
def check_quotes(dbconn):
    count = 0
    try:
        info("Creating MariaDB Engine...")
        db = create_engine(dbconn, echo=False)    
        conn = db.connect()  
        sql = "SELECT COUNT(*) FROM prices"
        info("Executing query [%s]" % (sql))
        rs = db.execute(sql)
        for row in rs:
            count = row[0]
        conn.close()
    except exc.SQLAlchemyError as e:
        error("Database Error: %s, conn=%s" % (e, dbconn))   
    except:
        error("Unknown Error: %s, conn=%s" % (sys.exc_info()[0], dbconn))   
        error(str(sys.exc_info()))
    return (count)
        
def insert_quotes(inputfile, dbconn, startdate):
    basename = os.path.basename(inputfile)
    basedate = datetime.strptime(basename.split('.')[0], '%Y%m%d')
    if basedate < startdate:
        return    
    
    try:
        info("Creating MariaDB Engine...")
        db = create_engine(dbconn, echo=False)    
        conn = db.connect()  
        dateparse = lambda x: pd.datetime.strptime(x, "%Y%m%d").date()
        info("Reading file [%s]" % (inputfile))
        df = pd.read_csv(inputfile, parse_dates=['date'], date_parser=dateparse)
        df['source'] = 'F'   
        df.to_sql('prices', conn, index=False, if_exists='append', dtype={'date': DATE})
    
        conn.close()
    except exc.SQLAlchemyError as e:
        error("Database Error: %s, conn=%s" % (e, dbconn))   
    except:
        error("Unknown Error: %s, conn=%s" % (sys.exc_info()[0], dbconn))   
        error(str(sys.exc_info()))
        
def record_quotes(inputpath, dbconn, startdate):
    inputsearch = "%s/*/*.stockquotes.csv" % (inputpath)
    info("Searching pattern [%s]" % (inputsearch))
    files = sorted(glob.glob(inputsearch))
    
    for file in files:
        insert_quotes(file, dbconn, startdate)
           
def print_usage():
    print  ("  Usage: %s [options]" % (os.path.basename(__file__))) 
    print  ("  Options:")
    print  ("  \t-i, --input\tinput directory")
    print  ("  \t-d, --database\tdatabase connection string")
    print  ("  \t-s, --startdate\tstart date (YYYMMDD)")
    print  ("  \t-h,\t\thelp")
    
def main(argv):
    argInput = "/dfs/stage/factset/"
    argDBConn = ""
    argStartDate = datetime.strptime('20000101', '%Y%m%d')
    
    try:
        opts, args = getopt.getopt(argv,"hi:d:s:",["input=","database=","startdate="])
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
        elif opt in ("-s", "--startdate"):
            argStartDate = datetime.strptime(arg, '%Y%m%d')               

    if len(argDBConn) == 0 or len(argInput) == 0:
        print_usage()
        sys.exit(0)
                
    if check_quotes(argDBConn) > 0:
        fatal("Prices table must be empty before inserting!")
    else:
        record_quotes(argInput, argDBConn, argStartDate)
  

if __name__ == '__main__':
    main(sys.argv[1:])                