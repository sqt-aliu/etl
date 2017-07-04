#!/opt/anaconda3/bin/python -u

import sys
import getopt
import os.path
from time import sleep
from datetime import datetime, date
from urllib.request import urlopen, urlretrieve
from urllib.error import URLError, HTTPError
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn
from extract.lib.hkex import HKEXSecurityList

def download_page(url, iteration=1, max_tries=5):
    pagedata = {}
    try:
        urlResp = urlopen(url, timeout=30)
        html = urlResp.read()
        urlResp.close()

        # instantiate the parser and fed it some HTML
        htmlparser = HKEXSecurityList()
        htmlparser.feed(str(html))
        pagedata.update(htmlparser.data)
    #handle errors
    except HTTPError as e:
        error("HTTP Error: %s, url=%s, attempt=%i" % (str(e.code), url, iteration))
        if iteration <= max_tries:
            sleep(iteration)
            download_page(url, iteration=iteration+1)           
    except URLError as e:
        error("URL Error: %s, url=%s, attempt=%i" % (e.reason, url, iteration))
        if iteration <= max_tries:
            sleep(iteration)
            download_page(url, iteration=iteration+1)           
    except OSError as e:
        error("OS Error: %s, url=%s, attempt=%i" % (e, url, iteration))      
        if iteration <= max_tries:
            sleep(iteration)
            download_page(url, iteration=iteration+1)           
    except:
        error("Unknown Error: %s, url=%s, attempt=%i" % (sys.exc_info()[0], url, iteration))   
        if iteration <= max_tries:
            sleep(iteration)
            download_page(url, iteration=iteration+1)      
    return pagedata
    
def download_list(output):
    urls = ["http://www.hkex.com.hk/eng/market/sec_tradinfo/stockcode/eisdeqty.htm",
            "http://www.hkex.com.hk/eng/market/sec_tradinfo/stockcode/eisdetf.htm",
            "http://www.hkex.com.hk/eng/market/sec_tradinfo/stockcode/eisdreit.htm"]
    seclist = {}
    for url in urls:        
        seclist.update(download_page(url))
        
    if len(seclist) > 0:
        today = datetime.strftime(date.today(), "%Y%m%d")
        csvfile = output + "/" + today + "/" + today + ".products.csv"
        write_list(csvfile, seclist)
    else:
        error("No Securities Found!")
        
def write_list(file, map):
    if not os.path.exists(os.path.dirname(file)):
        info("Creating directory %s" % (os.path.dirname(file)))
        os.makedirs(os.path.dirname(file))       
    info("Saving download to %s" % (file))
    with open(file, "w") as fh:
        fh.write("code,name,lotsize,ccass,short,sso,ssf\n")
        for k,v in sorted(map.items()):
            fh.write("%s,%s,%s,%s,%s,%s,%s\n" % (k, v['NAME'], v['LOTSIZE'], v['CCASS'], v['SHORT'], v['SSO'], v['SSF']))
        fh.close()       
 
def print_usage():
    print  ("  Usage: %s [options]" % (os.path.basename(__file__))) 
    print  ("  Options:")
    print  ("  \t-o,\t\toutput directory")
    print  ("  \t-h,\t\thelp")
                          
def main(argv):
    argOutput = "/dfs/raw/live/hkex/"
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

    download_list(argOutput)

if __name__ == '__main__':
    main(sys.argv[1:])
