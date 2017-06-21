#!/opt/anaconda3/bin/python -u

import sys
import getopt
import os.path
from datetime import datetime, date
from urllib.request import urlopen, urlretrieve, Request
from urllib.error import URLError, HTTPError
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn
from extract.lib.asx import ASXHistorical

def download_products(output, download_url):
    if not os.path.exists(output):
        if not os.path.exists(os.path.dirname(output)):
            info("Creating directory %s" % (os.path.dirname(output)))
            os.makedirs(os.path.dirname(output))            
        info("Retrieving url %s" % (download_url))
        
        hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
               'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
               'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
               'Accept-Encoding': 'gzip, deflate',
               'Accept-Language': 'en-US,en;q=0.8',
               'Referer': 'http://english.sse.com.cn/listed/company/',
               'Connection': 'keep-alive'}    
        req = Request(download_url, headers=hdr)            
        urlResp = urlopen(req, timeout=60)
        info("Saving file [%s]" % (output))
        with open(output, 'wb') as fh:
            fh.write(urlResp.read())
            fh.close()
        urlResp.close()
    else:
        warn("File already found %s" % (output))            
    
def print_usage():
    print  ("  Usage: %s [options]" % (os.path.basename(__file__))) 
    print  ("  Options:")
    print  ("  \t-o,\t\toutput directory")
    print  ("  \t-h,\t\thelp")
                          
def main(argv):
    argOutput = "/dfs/raw/live/sse/"
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

    today = datetime.strftime(date.today(), "%Y%m%d")
    xlsfile = argOutput + "/" + today + "/" + today + ".products.txt"
    download_products(xlsfile, "http://query.sse.com.cn/listedcompanies/companylist/downloadCompanyInfoList.do")

if __name__ == '__main__':
    main(sys.argv[1:])
