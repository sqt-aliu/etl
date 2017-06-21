#!/opt/anaconda3/bin/python -u
import csv
import getopt
import glob
import os
import pprint
import shlex
import subprocess
import sys
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn

tickers = {}

def check_path(path):
    if not os.path.exists(path):
        info("Creating directory %s" % (path))
        os.makedirs(path)   

def system_call(cmd):
    try:
        subprocess.call(shlex.split(cmd))
    except OSError as e:
        error("System call %s" % (str(e)))
        
def build_tickers(src, out, reg):
    # first find the latest full file
    full_pattern = "%s/datafeeds/edm/h_security_ticker/h_security_ticker_full_*.zip" % (src)
    full_files = sorted(glob.glob(full_pattern))
    # take the lastest file
    full_latest = full_files[-1]
    info ("Parsing latest ticker full file [%s]" % (full_latest))
    full_latest_basename = os.path.splitext(os.path.basename(full_latest))[0]
    
    full_unzip_path = "/var/tmp/%s" % (full_latest_basename)
    check_path(full_unzip_path) 
        
    # unzip file
    system_call("unzip -o %s -d %s" % (full_latest, full_unzip_path))
    
    # read the exchange file
    full_ticker_exch = "%s/h_security_ticker_region.txt" % (full_unzip_path)
    if os.path.exists(full_ticker_exch):
        # parse file
        fh = open(full_ticker_exch, 'r', encoding='ISO-8859-1')
        try:
            reader = csv.reader(fh, delimiter = '|')
            for row in reader:
                if not row[0] == "FS_PERM_SEC_ID": #ignore header 
                    # parse ticker exchange
                    secid = row[0] # FS_PERM_SEC_ID
                    exchcode = row[0].split("-")[2] # FS_PERM_SEC_ID
                    ticker = row[1].split("-")[0] # TICKER_REGION
                    sectype = row[7]  #FREF_SECURITY_TYPE
                    valid = True
                    if "." in ticker:
                        sfx = ticker.split('.')[1]
                        if sfx.startswith('XX'):
                            valid = False
                    if exchcode in reg and sectype in ['ETF_ETF', 'SHARE', 'DR'] and valid:
                        if exchcode == 'HK':
                            tickers[secid] = ticker.zfill(4) + ".HK"
                        elif exchcode == 'CN':
                            if ticker.startswith('5') or ticker.startswith('6') or ticker.startswith('9'):
                                tickers[secid] = ticker + ".SS"
                            elif ticker.startswith('0') or ticker.startswith('1') or ticker.startswith('2') or ticker.startswith('3'):
                                tickers[secid] = ticker + ".SZ"
                            else:
                                error("Unrecognized ticker [%s]" % (ticker))
                        elif exchcode == 'AU':
                            tickers[secid] = ticker + ".AX"
                        elif exchcode == 'JP':
                            tickers[secid] = ticker + ".T"      
                        else:
                            error("Unrecognized exchange code [%s]" % (exchcode))
        finally:
            fh.close()

        # clean up
        info("Cleaning up directory [%s]" % (full_unzip_path))
        system_call("rm -rf %s" % (full_unzip_path))
    else:
        fatal("Cannot find exchange file >> %s" % (full_ticker_exch))
        
    #pp = pprint.PrettyPrinter(indent=4)
    #pp.pprint(tickers)     
        
def build_prices(src, out, reg, date):
    # first find the latest full file
    full_pattern = "%s/datafeeds/prices/fp_basic_ap/fp_basic_prices_ap_full_*.zip" % (src)
    full_files = sorted(glob.glob(full_pattern))
    # take the lastest file
    full_latest = full_files[-1]
    info ("Parsing latest prices full file >> %s" % (full_latest))
    full_latest_basename = os.path.splitext(os.path.basename(full_latest))[0]
    
    full_unzip_path = "/var/tmp/%s" % (full_latest_basename)
    check_path(full_unzip_path)
        
    # unzip file
    system_call("unzip -o %s -d %s" % (full_latest, full_unzip_path))
    
    # list out files
    full_pattern = "%s/fp_basic_bd_ap*.txt" % (full_unzip_path)
    full_files = glob.glob(full_pattern)
    
    for full_file in full_files:
        info ("Reading price file >> %s" % (full_file))
        price_map = {}    
        # parse file
        with open(full_file, 'r') as fh:
            reader = csv.reader(fh, delimiter = '|')
            for row in reader:
                if not row[0] == "FS_PERM_SEC_ID": #ignore header 
                    recsym = row[0] # symbol
                    recpar = recsym.split('-')
                    if len(recpar) >= 3:
                        recreg = recpar[2]
                        recdate = row[1].replace('-','') # date
                        if int(date) < int(recdate.replace('-','')):
                            if (recreg in reg) and (recsym in tickers):
                                if recdate not in price_map:
                                    price_map[recdate] = {}
                                if recsym in price_map[recdate]:
                                    warn ("Duplicate symbol for the same date found >> %s" % ("|".join(row)))
                                else:
                                    price_map[recdate][tickers[recsym]] = [ tickers[recsym],
                                              recdate,
                                              row[5], 
                                              row[6],
                                              row[7],
                                              row[4],
                                              str(int(float(row[8])*1000)) ]  #ticker,date,open,high,low,close,volume
                    else:
                        warn ("Bad FS_PERM_SEC_ID format >> %s" % (row[0]))
            fh.close()

        for full_date in sorted(price_map.keys()):
            out_batch_prices = "%s/%s/%s.stockquotes.csv" % (out, full_date, full_date)
            check_path(os.path.dirname(out_batch_prices))     
            info ("Writing to prices file >> %s" % (out_batch_prices))   
            with open(out_batch_prices, 'w') as fw:   
                writer = csv.writer(fw, delimiter =  ',')      
                writer.writerow( ["ticker","date","open","high","low","close","volume" ])
                full_recs = price_map[full_date]
                for full_sym in sorted(full_recs.keys()):
                    full_row = full_recs[full_sym]
                    writer.writerow(full_row)      
                fw.close()            
              
            info ("Write completed >> %s" % (out_batch_prices))   
     
        del price_map
        # clean up
    info ("Cleaning up directory >> %s" % (full_unzip_path))
    system_call("rm -rf %s" % (full_unzip_path))                
                    
def build_corpactions(src, out, reg, date):
    # first find the latest full file
    full_pattern = "%s/datafeeds/prices/fp_basic_ap/fp_basic_ap_full_*.zip" % (src)
    full_files = sorted(glob.glob(full_pattern))
    # take the lastest file
    full_latest = full_files[-1]
    info ("Parsing latest corpactions full file >> %s" % (full_latest))
    full_latest_basename = os.path.splitext(os.path.basename(full_latest))[0]
    
    full_unzip_path = "%s/%s" % (out, full_latest_basename)
    check_path(full_unzip_path)   
        
    # unzip file
    system_call("unzip -o %s -d %s" % (full_latest, full_unzip_path))
         
    ca_map = {}
    
    # parse the divs file
    full_divs = "%s/fp_basic_dividends_ap.txt" % (full_unzip_path)
    # parse file
    with open(full_divs, 'r') as fh:
        reader = csv.reader(fh, delimiter = '|')
        for row in reader:
            if not row[0] == "FS_PERM_SEC_ID": #ignore header 
                recsym = row[0] # symbol
                if recsym in tickers:  
                    ticker = tickers[recsym]
                    recdate = row[1].replace('-','') # date
                    if int(date) < int(recdate):
                        if recdate not in ca_map:
                            ca_map[recdate] = {}
                        if ticker not in ca_map[recdate]:
                            ca_map[recdate][ticker] = {}
                            
                        ca_map[recdate][ticker]['DIVAMT'] = row[4]
                        ca_map[recdate][ticker]['DIVCCY'] = row[2]
        fh.close()
           
    # parse the splits file
    full_splits = "%s/fp_basic_splits_ap.txt" % (full_unzip_path)
    # parse file
    with open(full_splits, 'r') as fh:
        reader = csv.reader(fh, delimiter = '|')
        for row in reader:
            if not row[0] == "FS_PERM_SEC_ID": #ignore header 
                recsym = row[0] # symbol
                if recsym in tickers:  
                    ticker = tickers[recsym]
                    recdate = row[1].replace('-','') # date
                    if int(date) < int(recdate):
                        if recdate not in ca_map:
                            ca_map[recdate] = {}
                        if ticker not in ca_map[recdate]:
                            ca_map[recdate][ticker] = {}
                        ca_map[recdate][ticker]['SPLIT'] = row[2]
        fh.close()
  
    for full_date in sorted(ca_map.keys()):
        out_batch_prices = "%s/%s/%s.stockdivs.csv" % (out, full_date, full_date)
        check_path(os.path.dirname(out_batch_prices))     
        info ("Writing to prices file >> %s" % (out_batch_prices))   
        with open(out_batch_prices, 'w') as fw:   
            writer = csv.writer(fw, delimiter =  ',')      
            writer.writerow( ["ticker","date","dvd","split"])
            full_recs = ca_map[full_date]
            for full_sym in sorted(full_recs.keys()):
                full_row = full_recs[full_sym]
                writer.writerow([ full_sym, full_date, full_row['DIVAMT'] if 'DIVAMT' in full_row else '', full_row['SPLIT'] if 'SPLIT' in full_row else ''] )   
            fw.close()            
          
        info ("Write completed >> %s" % (out_batch_prices))   
                
    del ca_map
                     
    # clean up
    info ("Removing directory >> %s" % (full_unzip_path))   
    system_call("rm -rf %s" % (full_unzip_path))    
  

def print_usage():
    print  ("  Usage: %s [options]" % (os.path.basename(__file__))) 
    print  ("  Options:")
    print  ("  \t-p, --prices\tbuild prices")
    print  ("  \t-c, --corpactions\tbuild corporate actions")
    print  ("  \t-s, --source\tsource directory")
    print  ("  \t-o, --output\toutput directory")
    print  ("  \t-r, --region\tregion(s)") 
    print  ("  \t-d, --date\tmin date") 
    print  ("  \t-x, --suffix\tticker suffix") 
    print  ("  \t-h,\t\thelp")
    
def main(argv):   
    optprices = 0
    optcorpactions = 0
    output = "/dfs/stage/factset/"
    region = "AU,CN,HK,JP".split(',')
    source = "/ftp/raw/batch/fact.day/"
    date = "19800101"
    
    try:
        opts, args = getopt.getopt(argv,"cpd:o:s:r:x:",["prices", "corpactions", "date=","output=","source=","region="])
    except getopt.GetoptError:
        print_usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print_usage()
            sys.exit()
        elif opt in ("-p", "--prices"):
            optprices = 1
        elif opt in ("-c", "--corpactions"):
            optcorpactions = 1  
        elif opt in ("-d", "--date"):
            date = arg                
        elif opt in ("-o", "--output"):
            output = arg    
        elif opt in ("-s", "--source"):
            source = arg   
        elif opt in ("-r", "--region"):
            region = arg.upper().split(',')             

    if len(output.strip()) == 0 or len(source.strip()) == 0 or len(region) == 0 or len(date.strip()) != 8:
        print_usage()
        sys.exit()
        
    build_tickers(source, output, region)
    if optprices:
        build_prices(source, output, region, date)
    if optcorpactions:
        build_corpactions(source, output, region, date)
        
if __name__ == '__main__':
    main(sys.argv[1:])
    
