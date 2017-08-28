#!/opt/anaconda3/bin/python -u

import re
import sys
import getopt
import os.path
from urllib.request import urlopen, urlretrieve
from urllib.error import URLError, HTTPError
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn
from extract.lib.hkex import HKEXShorts

def download_file(output, download_url):
    basename = os.path.basename(download_url)
    match = re.search("(20[0-2][0-9]{5})", basename)
    if match:
        download_output = output + "/" + match.group() + "/" + match.group() + ".shorts.csv"
        if not os.path.exists(download_output):
            if not os.path.exists(os.path.dirname(download_output)):
                info("Creating directory %s" % (os.path.dirname(download_output)))
                os.makedirs(os.path.dirname(download_output))            
            info("Retrieving url %s" % (download_url))
            urlretrieve(download_url, download_output)   
        else:
            warn("File already found %s" % (download_output))
        
def download_daily(output):
    url = "https://www.hkex.com.hk/eng/market/sec_tradinfo/dslist.htm"
    batch = []
     
    try:
        info ("Opening url [%s]"% (url))
        urlResp = urlopen(url, timeout=60)
        html = urlResp.read()
        urlResp.close()
        
        # instantiate the parser and fed it some HTML
        htmlparser = HKEXShorts()
        htmlparser.feed(str(html))
        batch = batch + htmlparser.data
        print (batch)
    #handle errors
    except HTTPError as e:
        error("HTTP Error: %s, url=%s" % (str(e.code), url))         
    except URLError as e:
        error("URL Error: %s, url=%s" % (e.reason, url))       
    except OSError as e:
        error("OS Error: %s, url=%s" % (e, url))               
    except:
        error("Unknown Error: %s, url=%s" % (sys.exc_info()[0], url))      
        
    for batch_download in batch:
        download_file(output, batch_download)


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

    download_daily(argOutput)

if __name__ == '__main__':
    main(sys.argv[1:])
