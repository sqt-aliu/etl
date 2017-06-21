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
from extract.lib.yahoo import YahooSummary

supported_types = ['jpx','asx','hkex','sse', 'szse', 'krx']

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
                    tickers.append(row[0][1:] + ".HK" if row[0].startswith('0') else row[0]+ ".HK")
                fh.close()
        else:
            error("Products file is missing [%s]" % (productsfile))
    elif exchtype == "asx":
        productsfile = "/dfs/raw/live/asx/" + today + "/" + today + ".products.csv"
        if os.path.exists(productsfile):
            with open(productsfile, 'r') as fh:
                lines = fh.readlines()[3:]
                reader = csv.reader(lines)
                for row in reader:
                    tickers.append(row[1] + ".AX")
                    if row[2] in ["Not Applic", "Class Pend"]:
                        shortlist.append(row[1] + ".AX")    
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
                        tickers.append(line[:6]+".SS")
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
                    tickers.append(values[0] + ".SZ")
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
                    tickers.append(values[0] + ".SZ")
        else:
            error("Products file is missing [%s]" % (productsfile))    
    elif exchtype == "krx":
        productsfile = "/dfs/raw/live/krx/" + today + "/" + today + ".products.json"
        if os.path.exists(productsfile):
            jsondata=open(productsfile, 'r', encoding="ISO-8859-1").read()
            jsonmap = json.loads(jsondata)
            if 'block1' in jsonmap:
                for jsonitem in jsonmap['block1']:    
                    if 'marketName' in jsonitem and 'short_code' in jsonitem:
                        if jsonitem['marketName'] == 'KOSDAQ':
                            tickers.append(jsonitem['short_code'][1:] + ".KQ")
                        elif jsonitem['marketName'] == 'KOSPI':
                            tickers.append(jsonitem['short_code'][1:] + ".KS")
                        else:
                            warn("Unsupported market [%s][%s]" % (jsonitem['short_code'], jsonitem['marketName']))
            else:
                error("Missing 'block1' key")
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
        elif len(sumdict[ticker]) == 0:
            info("Last attempt to retrieve %s" % (ticker))
            sumdict.update(download_summary(ticker))
            sleep(0.1)             
            
    # print missing tickers
    for ticker in sorted(tickers):
        if ticker not in sumdict:
            error("Unable to retrieve %s" % (ticker))
            
    sumfile = output + "/" + today + "/" + today + "." + exchtype + ".summary.json"
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
    url = "http://finance.yahoo.com/q?s=%s" % (ticker)
    summary = {}
    try:
        info("Requesting url [%s]" % (url))
        urlResp = urlopen(url, timeout=30)
        html = urlResp.read().decode('utf-8', 'ignore')
        urlResp.close()
        #print (html)
        htmlparser = YahooSummary(ticker)
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
    argOutput = "/dfs/raw/live/yahoo/"
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
