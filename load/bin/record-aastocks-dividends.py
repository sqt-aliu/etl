#!/opt/anaconda3/bin/python -u

import getopt
import glob
import os
import pandas as pd
import numpy as np
import sys
from sqlalchemy import create_engine, exc
from datetime import datetime
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn

supported_types = ['hkex','sse', 'szse']

def insert_dividends(inputfile, rtype, dbconn, startdate):
        
    try:
        info("Creating MariaDB Engine...")
        db = create_engine(dbconn, echo=False)    
        conn = db.connect()  
        
        sql = "SELECT * FROM dvd"
        if rtype == "hkex":
            sql = "SELECT * FROM dvd where ticker like '%.HK'"
        elif rtype == "szse":
            sql = "SELECT * FROM dvd where ticker like '%.SZ'"
        elif rtype == "sse":
            sql = "SELECT * FROM dvd where ticker like '%.SS'"            
        info("Executing query [%s]" % (sql))
        dfSQL = pd.read_sql_query(sql, conn, index_col=['ticker','date'])  
        info("Reading file [%s]" % (inputfile))
        dateparse = lambda x: pd.datetime.strptime(x, "%Y%m%d").date()
        dfCSV = pd.read_csv(inputfile, dtype={'code':str}, parse_dates=['date'], date_parser=dateparse)
        if rtype == "hkex":
            dfCSV['ticker'] = dfCSV['code'].astype(int).astype(str).str.zfill(4) + ".HK"
        elif rtype == "szse":
            dfCSV['ticker'] = dfCSV['code'] + ".SZ"
        elif rtype == "sse":
            dfCSV['ticker'] = dfCSV['code'] + ".SS"       
        
        dfCSV = dfCSV.set_index(['ticker','date'])
        for index, row in dfCSV.iterrows():
            if index[1] > startdate:
                if index in dfSQL.index:
                    diff = False
                    sdiv = round(dfSQL.ix[index]['dividend'], 4) if not np.isnan(dfSQL.ix[index]['dividend']) else dfSQL.ix[index]['dividend']
                    ssplit = round(dfSQL.ix[index]['split'], 4) if not np.isnan(dfSQL.ix[index]['split']) else dfSQL.ix[index]['split']
                    cdiv = round(row['dividend'], 4) if not np.isnan(row['dividend']) else row['dividend']
                    csplit = round(row['split'], 4) if not np.isnan(row['split']) else row['split']
                    
                    if cdiv > sdiv or (cdiv > 0. and np.isnan(sdiv)):
                        diff = True
                        warn("Difference in [%s][%s] dividend %s <> %s" % (index[0], index[1], sdiv, cdiv))
                    if (csplit != ssplit and not (np.isnan(csplit) and np.isnan(ssplit))) or (csplit > 0. and np.isnan(ssplit)):
                        diff = True
                        warn("Difference in [%s][%s] split %s <> %s" % (index[0], index[1], ssplit, csplit))
                   
                    if diff:
                        try:
                            sql = record_builder(index, row, "REPLACE")
                            info("Executing query [%s]" % (sql))
                            db.execute(sql)   
                        except exc.IntegrityError as e:
                            error("DB Integrity Error: %s, sql=%s" % (e, sql))  
                        except exc.SQLAlchemyError as e:
                            error("DB SQLAlchemy Error: %s, sql=%s" % (e, sql))                    
    
                else:
                    try:
                        sql = record_builder(index, row, "INSERT")
                        info("Executing query [%s]" % (sql))
                        db.execute(sql)   
                    except exc.IntegrityError as e:
                        error("DB Integrity Error: %s, sql=%s" % (e, sql))  
                    except exc.SQLAlchemyError as e:
                        error("DB SQLAlchemy Error: %s, sql=%s" % (e, sql))       
            else:
                warn("Skipping %s < %s" % (index[1], startdate))
    except:
        error("Unknown Error: %s, conn=%s" % (sys.exc_info()[0], dbconn))   
        error(str(sys.exc_info()))    
             
def record_builder(index, row, query="INSERT"):
    sql = "%s INTO dvd VALUES ('%s','%s',%s,%s,'A')" % (query, index[0],
        datetime.strftime(index[1], '%Y-%m-%d'), 
        'NULL' if np.isnan(row['dividend']) else row['dividend'],
        'NULL' if np.isnan(row['split']) else row['split'])
    return (sql)
    
def record_dividends(rtype, inputpath, dbconn, startdate):
    records = record_check(inputpath)
    inputsearch = "%s/*/*.%s.divs.csv" % (inputpath, rtype)
    info("Searching pattern [%s]" % (inputsearch))
    files = sorted(glob.glob(inputsearch))
    for file in files:
        if file in records:
            warn("File has already been recorded [%s]" % (file))
        else:
            insert_dividends(file, rtype, dbconn, startdate)
            record_add(inputpath, file)
            
def record_check(inputpath):
    recordfile = "%s/.record" % (inputpath)
    recordlist = []
    if os.path.exists(recordfile):
        recordlist = open(recordfile, 'r').read().split('\n')   
    return(recordlist)        
    
def record_add(inputpath, newrecord):
    recordfile = "%s/.record" % (inputpath)
    with open(recordfile, 'a+') as fh:
        fh.write("%s\n" % (newrecord))
        fh.close()
           
def print_usage():
    print  ("  Usage: %s [options]" % (os.path.basename(__file__))) 
    print  ("  Options:")
    print  ("  \t-t, --type\texchange types[%s]" % ("|".join(supported_types)))
    print  ("  \t-i, --input\tinput directory")
    print  ("  \t-d, --database\tdatabase connection string")
    print  ("  \t-s, --startdate\tstart date (YYYMMDD)")    
    print  ("  \t-h,\t\thelp")
    
def main(argv):
    argType = ""    
    argInput = "/dfs/stage/aastocks/"
    argDBConn = ""
    argStartDate = datetime.strptime('20000101', '%Y%m%d')
    
    try:
        opts, args = getopt.getopt(argv,"ht:i:d:s:",["type=","input=","database=","startdate="])
    except getopt.GetoptError:
        print_usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print_usage()
            sys.exit()     
        elif opt in ("-t", "--type"):
            argType = arg               
        elif opt in ("-i", "--input"):
            argInput = arg                 
        elif opt in ("-d", "--database"):
            argDBConn = arg   
        elif opt in ("-s", "--startdate"):
            argStartDate = datetime.strptime(arg, '%Y%m%d')               

    if len(argType) == 0 or argType not in supported_types or len(argDBConn) == 0 or len(argInput) == 0:
        print_usage()
        sys.exit(0)
                
    record_dividends(argType, argInput, argDBConn, argStartDate)
  

if __name__ == '__main__':
    main(sys.argv[1:])                