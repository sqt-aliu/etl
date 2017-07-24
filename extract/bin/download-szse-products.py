#!/opt/anaconda3/bin/python -u

import sys
import getopt
import os.path
from datetime import datetime, date
from time import sleep
from urllib.request import urlopen, urlretrieve, Request
from urllib.error import URLError, HTTPError
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn

def download_products(output, download_url, force, iteration=1, max_tries=5):
    try:
        if not os.path.exists(output) or force:
            if not os.path.exists(os.path.dirname(output)):
                info("Creating directory %s" % (os.path.dirname(output)))
                os.makedirs(os.path.dirname(output))            
            info("Retrieving url %s" % (download_url))
            
            hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
                   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                   'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
                   'Accept-Language': 'en-US,en;q=0.8',
                   'Accept-Encoding': 'gzip, deflate, br',
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
    #handle errors
    except HTTPError as e:
        error("HTTP Error: %s, url=%s, attempt=%i" % (str(e.code), download_url, iteration))
        if iteration <= max_tries:
            sleep(iteration)
            download_products(output, download_url, force, iteration=iteration+1)           
    except URLError as e:
        error("URL Error: %s, url=%s, attempt=%i" % (e.reason, download_url, iteration))
        if iteration <= max_tries:
            sleep(iteration)
            download_products(output, download_url, force, iteration=iteration+1)
    except OSError as e:
        error("OS Error: %s, url=%s, attempt=%i" % (e, download_url, iteration))      
        if iteration <= max_tries:
            sleep(iteration)
            download_products(output, download_url, force, iteration=iteration+1)       
    except:
        error("Unknown Error: %s, url=%s, attempt=%i" % (sys.exc_info()[0], download_url, iteration))   
        if iteration <= max_tries:
            sleep(iteration)
            download_products(output, download_url, force, iteration=iteration+1)
            
    
def print_usage():
    print  ("  Usage: %s [options]" % (os.path.basename(__file__))) 
    print  ("  Options:")
    print  ("  \t-o,\t\toutput directory")
    print  ("  \t-h,\t\thelp")
                          
def main(argv):
    argOutput = "/dfs/raw/live/szse/"
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
    xlsfile = argOutput + "/" + today + "/" + today + ".stockproducts.xlsx"
    download_products(xlsfile, "http://www.szse.cn/szseWeb/ShowReport.szse?SHOWTYPE=EXCEL&CATALOGID=1693&tab1PAGENUM=1&ENCODE=1&TABKEY=tab1", True)
    xlsfile = argOutput + "/" + today + "/" + today + ".fundproducts.xlsx"
    download_products(xlsfile, "http://www.szse.cn/szseWeb/ShowReport.szse?SHOWTYPE=EXCEL&CATALOGID=1956&tab1PAGENUM=1&ENCODE=1&TABKEY=tab1", True)

if __name__ == '__main__':
    main(sys.argv[1:])
