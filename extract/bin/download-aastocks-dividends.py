#!/opt/anaconda3/bin/python -u
import csv
import json
import sys
import getopt
import os.path
from lxml import etree
from datetime import datetime, date
from time import sleep
from urllib.request import urlopen, urlretrieve
from urllib.error import URLError, HTTPError
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn
from extract.lib.aastocks import AAStocksDividends, AAStocksDividendsCNHK

supported_types = ['hkex','sse', 'szse']

def build_summary(exchtype, output):
    sumdict = {}
    today = datetime.strftime(date.today(), "%Y%m%d")
    tickers = []
    shortlist = []
    if exchtype == "hkex":
        productsfile = "/dfs/raw/live/hkex/" + today + "/" + today + ".products.csv"
        if os.path.exists(productsfile):
            with open(productsfile, 'r') as fh:
                lines = fh.readlines()[1:]
                reader = csv.reader(lines)
                for row in reader:
                    tickers.append(row[0])
                fh.close()
        else:
            error("Products file is missing [%s]" % (productsfile))     
    elif exchtype == "sse":
        productsfile = "/dfs/raw/live/sse/" + today + "/" + today + ".products.txt"
        if os.path.exists(productsfile):
            with open(productsfile, 'r', encoding="ISO-8859-1") as fh: 
                lines = fh.read().splitlines()[1:]
                for line in lines:
                    if len(line) > 6:
                        tickers.append(line[:6])
                fh.close()         
        else:
            error("Products file is missing [%s]" % (productsfile))       
    elif exchtype == "szse":
        productsfile = "/dfs/raw/live/szse/" + today + "/" + today + ".stockproducts.xlsx"
        if os.path.exists(productsfile):
            html = open(productsfile, 'r', encoding="ISO-8859-1").read()
            table = etree.HTML(html).find("*/table")
            rows = iter(table)
            for row in rows:
                values = [col.text for col in row]
                if not "CODE" in values[0].upper():
                    tickers.append(values[0])
        else:
            error("Products file is missing [%s]" % (productsfile))   
        productsfile = "/dfs/raw/live/szse/" + today + "/" + today + ".fundproducts.xlsx"
        if os.path.exists(productsfile):
            html = open(productsfile, 'r', encoding="ISO-8859-1").read()
            table = etree.HTML(html).find("*/table")
            rows = iter(table)
            for row in rows:
                values = [col.text for col in row]
                if not "CODE" in values[0].upper():
                    tickers.append(values[0])
        else:
            error("Products file is missing [%s]" % (productsfile))    

    for ticker in sorted(tickers):
        if ticker in shortlist:
            sumdict.update(download_summary(ticker), iteration=1, max_tries=1)
        else:
            sumdict.update(download_summary(ticker))
        sleep(0.1)
            
    # try one more time
    for ticker in sorted(tickers):
        if ticker not in sumdict:
            info("Last attempt to retrieve %s" % (ticker))
            sumdict.update(download_summary(ticker))
            sleep(0.1)         
            
    # print missing tickers
    for ticker in sorted(tickers):
        if ticker not in sumdict:
            error("Unable to retrieve %s" % (ticker))
            
    sumfile = output + "/" + today + "/" + today + "." + exchtype + ".dividends.json"
    write_summary(sumfile,sumdict)
    
def write_summary(file, map):
    if not os.path.exists(os.path.dirname(file)):
        info("Creating directory %s" % (os.path.dirname(file)))
        os.makedirs(os.path.dirname(file))       
    info("Saving download to %s" % (file))
    with open(file, "w") as fh:
        j = json.dumps(map, indent=4, sort_keys=True)
        fh.write(j)
        fh.close()
        
def download_summary(ticker, iteration=1, max_tries=2):
    url = "http://www.aastocks.com/en/stocks/analysis/dividend.aspx?symbol=%s" % (ticker)
    if len(ticker) == 6:
        url = "http://www.aastocks.com/en/cnhk/analysis/dividend.aspx?shsymbol=%s" % (ticker)
    summary = {}
    try:
        info("Requesting url [%s]" % (url))
        urlResp = urlopen(url, timeout=30)
        html = urlResp.read().decode('utf-8', 'ignore')
        urlResp.close()
        #print (html)
        if len(ticker) == 6:
            htmlparser = AAStocksDividendsCNHK(ticker)
            htmlparser.feed(html)
            summary.update(htmlparser.data)            
        else:
            htmlparser = AAStocksDividends(ticker)
            htmlparser.feed(html)
            summary.update(htmlparser.data)
    #handle errors
    except HTTPError as e:
        warn("HTTP Error: %s, url=%s, attempt=%i" % (str(e.code), url, iteration))
        if iteration <= max_tries:
            download_summary(ticker, iteration=iteration+1)           
    except URLError as e:
        warn("URL Error: %s, url=%s, attempt=%i" % (e.reason, url, iteration))
        if iteration <= max_tries:
            download_summary(ticker, iteration=iteration+1)           
    except OSError as e:
        warn("OS Error: %s, url=%s, attempt=%i" % (e, url, iteration))      
        if iteration <= max_tries:
            download_summary(ticker, iteration=iteration+1)           
    except:
        warn("Unknown Error: %s, url=%s, attempt=%i" % (sys.exc_info()[0], url, iteration))   
        if iteration <= max_tries:
            download_summary(ticker, iteration=iteration+1)   
    return (summary)
    
def print_usage():
    print  ("  Usage: %s [options]" % (os.path.basename(__file__))) 
    print  ("  Options:")
    print  ("  \t-t, --type\texchange types[%s]" % ("|".join(supported_types)))
    print  ("  \t-o, --output\toutput directory")
    print  ("  \t-h,\t\thelp")
                          
def main(argv):
    argType = ""
    argOutput = "/dfs/raw/live/aastocks/"
    try:
        opts, args = getopt.getopt(argv,"ht:o:",["type=","output="])
    except getopt.GetoptError:
        print_usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print_usage()
            sys.exit()           
        elif opt in ("-t", "--type"):
            argType = arg         
        elif opt in ("-o", "--output"):
            argOutput = arg                

    if len(argType) == 0 or argType not in supported_types:
        print_usage()
        sys.exit()

    build_summary(argType, argOutput)

if __name__ == '__main__':
    main(sys.argv[1:])
