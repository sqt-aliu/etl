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

supported_types = ['jpx','asx','hkex','sse', 'szse']

def insert_quotes(inputfile, dbconn, startdate):
    allnulls = lambda x: True if np.isnan(x['open']) and np.isnan(x['high']) and np.isnan(x['low']) and np.isnan(x['close']) and np.isnan(x['volume'])  else False
    basename = os.path.basename(inputfile)
    basedate = datetime.strptime(basename.split('.')[0], '%Y%m%d')
    if basedate < startdate:
        return    
        
    try:
        info("Creating MariaDB Engine...")
        db = create_engine(dbconn, echo=False)    
        conn = db.connect()  
        
        sql = "SELECT * FROM prices WHERE date = '%s'" % (datetime.strftime(basedate, '%Y-%m-%d'))
        info("Executing query [%s]" % (sql))
        dfSQL = pd.read_sql_query(sql, conn, index_col='ticker')  
        info("Reading file [%s]" % (inputfile))
        dfCSV = pd.read_csv(inputfile)
        dfCSV['date'] = basedate
        dfCSV['blank'] = dfCSV.apply(allnulls, axis=1)
        
        for index, row in dfCSV.iterrows():
            ticker = row['ticker']
            if not row['blank']:
                if ticker in dfSQL.index:
                    diff = False
                    scnt = 0
                    ccnt = 0
                    for field in ['open', 'high', 'low', 'close', 'volume']:
                        sval = dfSQL.ix[ticker][field]
                        cval = row[field]
                        if not (sval is None or np.isnan(sval)):
                            scnt += 1
                        if not (cval is None or np.isnan(cval)):
                            ccnt += 1
                            cval = int(cval) if field == 'volume' else round(cval, 3)
                        if not sval == cval:
                            diff = True
                            warn("Difference in %s's %s %s <> %s" % (ticker, field, sval, cval))
        
                    if diff:
                        if ccnt >= scnt:
                            try:
                                sql = record_builder(row, "REPLACE")
                                info("Executing query [%s]" % (sql))
                                db.execute(sql)   
                            except exc.IntegrityError as e:
                                error("DB Integrity Error: %s, sql=%s" % (e, sql))  
                            except exc.SQLAlchemyError as e:
                                error("DB SQLAlchemy Error: %s, sql=%s" % (e, sql))
                        else:
                            warn("No update required %s sql=%i, csv=%i" % (ticker, scnt, ccnt))
                else:
                    try:
                        sql = record_builder(row, "INSERT")
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
        error(str(sys.exc_info()))
        
def record_builder(row, query="INSERT"):
    sql = "%s INTO prices VALUES ('%s','%s',%s,%s,%s,%s,%s,'Y')" % (query, row['ticker'],
        datetime.strftime(row['date'], '%Y-%m-%d'), 
        'NULL' if np.isnan(row['open']) else round(row['open'], 3),
        'NULL' if np.isnan(row['high']) else round(row['high'], 3),
        'NULL' if np.isnan(row['low']) else round(row['low'], 3),
        'NULL' if np.isnan(row['close']) else round(row['close'], 3),   
        'NULL' if np.isnan(row['volume']) else int(row['volume']))
    return (sql)
    
def record_quotes(rtype, inputpath, dbconn, startdate):
    records = record_check(inputpath)
    inputsearch = "%s/*/*.%s.quotes.csv" % (inputpath, rtype)
    info("Searching pattern [%s]" % (inputsearch))
    files = sorted(glob.glob(inputsearch))
    for file in files:
        if file in records:
            warn("File has already been recorded [%s]" % (file))
        else:
            insert_quotes(file, dbconn, startdate)
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
    argInput = "/dfs/stage/yahoo/"
    argDBConn = ""
    argStartDate = datetime.strptime('20161001', '%Y%m%d')
    
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
                
    record_quotes(argType, argInput, argDBConn, argStartDate)
  

if __name__ == '__main__':
    main(sys.argv[1:])                