#!/opt/anaconda3/bin/python -u
import json
import sys
import getopt
import os.path
from time import sleep
from datetime import datetime, date
from urllib.request import urlopen, urlretrieve, Request
from urllib.parse import urlencode
from urllib.error import URLError, HTTPError
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn


def download_key(iteration=1, max_tries=5):
    keygen = ""
    try:
        url = "http://global.krx.co.kr/contents/COM/GenerateOTP.jspx?bld=COM/finder_stkisu_en&name=form&_=1497515946793"
        info("Retrieving url %s" % (url))
        urlResp = urlopen(url, timeout=60)
        charset=urlResp.info().get_content_charset()
        keygen = urlResp.read().decode(charset)
        urlResp.close()

    #handle errors
    except HTTPError as e:
        error("HTTP Error: %s, url=%s, attempt=%i" % (str(e.code), url, iteration))
        if iteration <= max_tries:
            sleep(iteration)
            download_key(iteration=iteration+1)           
    except URLError as e:
        error("URL Error: %s, url=%s, attempt=%i" % (e.reason, url, iteration))
        if iteration <= max_tries:
            sleep(iteration)
            download_key(iteration=iteration+1)               
    except OSError as e:
        error("OS Error: %s, url=%s, attempt=%i" % (e, url, iteration))      
        if iteration <= max_tries:
            sleep(iteration)
            download_key(iteration=iteration+1)               
    except:
        error("Unknown Error: %s, url=%s, attempt=%i" % (sys.exc_info()[0], url, iteration))   
        if iteration <= max_tries:
            sleep(iteration)
            download_key(iteration=iteration+1)     
    return keygen
    
def download_json(keygen, iteration=1, max_tries=5):
    products = None
    try:
        params = {"code":keygen,
                  "isuCd":"",
                  "no":"P1",
                  "mktsel":"ALL",
                  "searchText":"",
                  "pagePath":"/contents/COM/FinderStkIsu.jsp",
                  "pageFirstCall":"Y"}

        params = urlencode(params)
        params = params.encode('ascii') # data should be bytes        
        url = "http://global.krx.co.kr/contents/GLB/99/GLB99000001.jspx"
        req = Request(url, params)     
        info("Retrieving url %s" % (url))
        urlResp = urlopen(req, timeout=120)
        charset=urlResp.info().get_content_charset()
        jsontext = urlResp.read().decode(charset)
        urlResp.close()

        products = json.loads(jsontext)
    #handle errors
    except HTTPError as e:
        error("HTTP Error: %s, url=%s, attempt=%i" % (str(e.code), url, iteration))
        if iteration <= max_tries:
            sleep(iteration)
            download_json(keygen, iteration=iteration+1)           
    except URLError as e:
        error("URL Error: %s, url=%s, attempt=%i" % (e.reason, url, iteration))
        if iteration <= max_tries:
            sleep(iteration)
            download_json(keygen, iteration=iteration+1)                         
    except OSError as e:
        error("OS Error: %s, url=%s, attempt=%i" % (e, url, iteration))      
        if iteration <= max_tries:
            sleep(iteration)
            download_json(keygen, iteration=iteration+1)                     
    except:
        error("Unknown Error: %s, url=%s, attempt=%i" % (sys.exc_info()[0], url, iteration))   
        if iteration <= max_tries:
            sleep(iteration)
            download_json(keygen, iteration=iteration+1)            
    return products
    
        
def write_products(file, map):
    if not os.path.exists(os.path.dirname(file)):
        info("Creating directory %s" % (os.path.dirname(file)))
        os.makedirs(os.path.dirname(file))       
    info("Saving download to %s" % (file))
    with open(file, "w") as fh:
        j = json.dumps(map, indent=4, sort_keys=True)
        fh.write(j)
        fh.close()
        
def download_products(output):
    key = download_key()
    if len(key) > 0:
        products = download_json(key)
        today = datetime.strftime(date.today(), "%Y%m%d")
        jsonfile = output + "/" + today + "/" + today + ".products.json"        
        write_products(jsonfile,products)        
    else:
        error("Unable to retrieve valid key")
        
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
    argOutput = "/dfs/raw/live/krx/"
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

    download_products(argOutput)

if __name__ == '__main__':
    main(sys.argv[1:])
