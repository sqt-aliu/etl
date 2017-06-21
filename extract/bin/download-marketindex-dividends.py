#!/opt/anaconda3/bin/python -u

import json
import re
import sys
import getopt
import os.path
from datetime import datetime, date
from urllib.request import urlopen, urlretrieve
from urllib.error import URLError, HTTPError
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn
from extract.lib.marketindex import MarketIndexDividends

def download_page(url, iteration=1, max_tries=5):
    pagedata = []
    try:
        urlResp = urlopen(url, timeout=30)
        html = urlResp.read()
        urlResp.close()

        # instantiate the parser and fed it some HTML
        htmlparser = MarketIndexDividends()
        htmlparser.feed(str(html))
        pagedata = pagedata +htmlparser.data
    #handle errors
    except HTTPError as e:
        error("HTTP Error: %s, url=%s, attempt=%i" % (str(e.code), url, iteration))
        if iteration <= max_tries:
            download_page(url, iteration=iteration+1)           
    except URLError as e:
        error("URL Error: %s, url=%s, attempt=%i" % (e.reason, url, iteration))
        if iteration <= max_tries:
            download_page(url, iteration=iteration+1)           
    except OSError as e:
        error("OS Error: %s, url=%s, attempt=%i" % (e, url, iteration))      
        if iteration <= max_tries:
            download_page(url, iteration=iteration+1)           
    except:
        error("Unknown Error: %s, url=%s, attempt=%i" % (sys.exc_info()[0], url, iteration))   
        if iteration <= max_tries:
            download_page(url, iteration=iteration+1)      
    return pagedata
    
def download_dividends(output):
    url = "http://www.marketindex.com.au/upcoming-dividends"
    
    divlist = download_page(url)
    if len(divlist) > 0:
        today = datetime.strftime(date.today(), "%Y%m%d")
        jsonfile = output + "/" + today + "/" + today + ".dividends.json"
        write_dividends(jsonfile, divlist)
    else:
        error("No Securities Found!")
        
def write_dividends(file, list):
    if not os.path.exists(os.path.dirname(file)):
        info("Creating directory %s" % (os.path.dirname(file)))
        os.makedirs(os.path.dirname(file))       
    info("Saving download to %s" % (file))
    with open(file, "w") as fh:
        j = json.dumps(list, indent=4, sort_keys=True)
        fh.write(j)
        fh.close()    
 
def print_usage():
    print  ("  Usage: %s [options]" % (os.path.basename(__file__))) 
    print  ("  Options:")
    print  ("  \t-o,\t\toutput directory")
    print  ("  \t-h,\t\thelp")
                          
def main(argv):
    argOutput = "/dfs/raw/live/marketindex/"
    try:
        opts, args = getopt.getopt(argv,"ho:",["output="])
    except getopt.GetoptError:
        print_usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print_usage()
            sys.exit()                  
        elif opt in ("-o", "--output"):
            argOutput = arg                

    download_dividends(argOutput)

if __name__ == '__main__':
    main(sys.argv[1:])
