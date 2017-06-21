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

def insert_quotes(inputfile, dbconn):
    allnulls = lambda x: True if np.isnan(x['open']) and np.isnan(x['high']) and np.isnan(x['low']) and np.isnan(x['close']) and np.isnan(x['volume'])  else False
    try:
        
        basename = os.path.basename(inputfile)
        basedate = datetime.strptime(basename.split('.')[0], '%Y%m%d')
        info("Creating MariaDB Engine...")
        db = create_engine(dbconn, echo=False)    
        conn = db.connect()  
        
        sql = "SELECT * FROM prices WHERE date = '%s'" % (datetime.strftime(basedate, '%Y-%m-%d'))
        info("Executing query [%s]" % (sql))
        dfSQL = pd.read_sql_query(sql, conn, index_col='ticker')  
        info("Reading file [%s]" % (inputfile))
        dfCSV = pd.read_csv(inputfile)
        dfCSV['ticker'] = dfCSV['code'].astype(str) + ".T"
        dfCSV['date'] = basedate
        dfCSV['blank'] = dfCSV.apply(allnulls, axis=1)
        
        for index, row in dfCSV.iterrows():
            ticker = row['ticker']
            if not row['blank']:
                if ticker in dfSQL.index:
                    diff = False
                    for field in ['open', 'high', 'low', 'close', 'volume']:
                        sval = dfSQL.ix[ticker][field]
                        cval = row[field]
                        if not sval == cval:
                            diff = True
                            warn("%s %s %f <> %f" % (ticker, field, sval, cval))
        
                    if diff:
                        try:
                            sql = record_builder(row, "REPLACE")
                            info("Executing query [%s]" % (sql))
                            db.execute(sql)   
                        except exc.IntegrityError as e:
                            error("DB Integrity Error: %s, sql=%s" % (e, sql))  
                        except exc.SQLAlchemyError as e:
                            error("DB SQLAlchemy Error: %s, sql=%s" % (e, sql))   
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
    sql = "%s INTO prices VALUES ('%s','%s',%s,%s,%s,%s,%s,'J')" % (query, row['ticker'],
        datetime.strftime(row['date'], '%Y-%m-%d'), 
        'NULL' if np.isnan(row['open']) else row['open'],
        'NULL' if np.isnan(row['high']) else row['high'],
        'NULL' if np.isnan(row['low']) else row['low'],
        'NULL' if np.isnan(row['close']) else row['close'],   
        'NULL' if np.isnan(row['volume']) else row['volume'])
    return (sql)
    
def record_quotes(inputpath, dbconn):
    records = record_check(inputpath)
    inputsearch = "%s/*/*.stockquotes.csv" % (inputpath)
    info("Searching pattern [%s]" % (inputsearch))
    files = sorted(glob.glob(inputsearch))
    
    for file in files:
        if file in records:
            warn("File has already been recorded [%s]" % (file))
        else:
            insert_quotes(file, dbconn)
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
    print  ("  \t-i, --input\tinput directory")
    print  ("  \t-d, --database\tdatabase connection string")
    print  ("  \t-h,\t\thelp")
    
def main(argv):
    argInput = "/dfs/stage/jpx/"
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