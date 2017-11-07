#!/opt/anaconda3/bin/python -u

import sys
import json
import math
import getopt
import os.path
from time import sleep
from datetime import datetime, date
from urllib.request import urlopen, urlretrieve
from urllib.error import URLError, HTTPError
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn

def download_pagecount(url, iteration=1, max_tries=5):
    pageCount = 50
    try:
        info("Requesting url %s" % (url))
        urlResp = urlopen(url, timeout=30)
        html = urlResp.read()
        urlResp.close()
        
        html = html.decode('UTF-8')
        htmljson = json.loads(html)
        if 'PageSize' in htmljson and 'TotalCount' in htmljson:
            info("TotalCount=%s" % (htmljson['TotalCount']))         
            info("PageSize=%s" % (htmljson['PageSize']))
            
            pageCount = math.ceil(int(htmljson['TotalCount']) / int(htmljson['PageSize']))
            info("PageCount=%d" % (pageCount))
    #handle errors
    except HTTPError as e:
        error("HTTP Error: %s, url=%s, attempt=%i" % (str(e.code), url, iteration))
        if iteration <= max_tries:
            sleep(iteration)
            download_pagecount(url, iteration=iteration+1)           
    except URLError as e:
        error("URL Error: %s, url=%s, attempt=%i" % (e.reason, url, iteration))
        if iteration <= max_tries:
            sleep(iteration)
            download_pagecount(url, iteration=iteration+1)           
    except OSError as e:
        error("OS Error: %s, url=%s, attempt=%i" % (e, url, iteration))      
        if iteration <= max_tries:
            sleep(iteration)
            download_pagecount(url, iteration=iteration+1)           
    except:
        error("Unknown Error: %s, url=%s, attempt=%i" % (sys.exc_info()[0], url, iteration))   
        if iteration <= max_tries:
            sleep(iteration)
            download_pagecount(url, iteration=iteration+1)      
    return pageCount

def download_page(url, iteration=1, max_tries=5):
    pagedata = {}
    try:
        info("Requesting url %s" % (url))
        urlResp = urlopen(url, timeout=30)
        html = urlResp.read()
        urlResp.close()
        
        html = html.decode('UTF-8')
        htmljson = json.loads(html)      
        
        if "data" in htmljson:
            if "content" in htmljson["data"][0]:                
                if "table" in htmljson["data"][0]["content"][0]:
                    if "tr" in htmljson["data"][0]["content"][0]["table"][0]:
                        items = htmljson["data"][0]["content"][0]["table"][0]["tr"]
                        for item in items:
                            if not item['thead']:
                                if "td" in item:
                                    productlist = item["td"][0]
                                    symbol = productlist[1]
                                    symbol = symbol.strip('*').strip()
                                    symbol = symbol.zfill(5)
                                    name = productlist[2]
                                    name = name.replace(",", "_")
                                    mktcap = productlist[3]
                                    mktcap = mktcap.replace(",","")
                                    pagedata[symbol] = {}
                                    pagedata[symbol]['NAME'] = name
                                    pagedata[symbol]['MKTCAP'] = mktcap
                                    
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
    

def download_main(output):
    securities = {}
    url = "http://www.hkex.com.hk/eng/csm/ws/Result.asmx/GetData?location=companySearch&SearchMethod=2&LangCode=en&StockCode=&StockName=&Ranking=ByMC&StockType=ALL&mkt=hk&PageNo=%s&ATypeSHEx=&AType=&FDD=&FMM=&FYYYY=&TDD=&TMM=&TYYYY="
    page_count = download_pagecount(url % ("1"))
    for i in range(page_count):
        securities.update(download_page(url % (str(i+1))))

    if len(securities) > 0:
        today = datetime.strftime(date.today(), "%Y%m%d")
        csvfile = output + "/" + today + "/" + today + ".products.csv"
        write_list(csvfile, securities)
    else:
        error("No Securities Found!")
        
def write_list(file, map):
    if not os.path.exists(os.path.dirname(file)):
        info("Creating directory %s" % (os.path.dirname(file)))
        os.makedirs(os.path.dirname(file))       
    info("Saving download to %s" % (file))
    with open(file, "w") as fh:
        fh.write("code,name,mktcap\n")
        for k,v in sorted(map.items()):
            fh.write("%s,%s,%s\n" % (k, v['NAME'], v['MKTCAP']))
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

    download_main(argOutput)

if __name__ == '__main__':
    main(sys.argv[1:])
