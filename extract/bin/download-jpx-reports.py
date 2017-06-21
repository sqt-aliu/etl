#!/opt/anaconda3/bin/python -u

import re
import sys
import getopt
import os.path
from urllib.request import urlopen, urlretrieve
from urllib.error import URLError, HTTPError
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn
from extract.lib.jpx import JPXReports

def download_file(output, download_url):
    basename = os.path.basename(download_url)
    match = re.search("(20[0-2][0-9]{5})", basename)
    if match:
        download_output = output + "/" + match.group() + "/" + basename
        if not os.path.exists(download_output):
            if not os.path.exists(os.path.dirname(download_output)):
                info("Creating directory %s" % (os.path.dirname(download_output)))
                os.makedirs(os.path.dirname(download_output))            
            info("Retrieving url %s" % (download_url))
            urlretrieve(download_url, download_output)   
        else:
            warn("File already found %s" % (download_output))
        
def download_daily(output):
    url = "http://www.jpx.co.jp/english/markets/statistics-equities/daily/index.html"
    batch = []
     
    try:
        info ("Opening url [%s]"% (url))
        urlResp = urlopen(url, timeout=60)
        html = urlResp.read()
        urlResp.close()
        
        # instantiate the parser and fed it some HTML
        htmlparser = JPXReports()
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

 
def download_batch(output):
    indexUrl = "http://www.jpx.co.jp/english/markets/statistics-equities/daily/index.html"
    archiveUrl = "http://www.jpx.co.jp/english/markets/statistics-equities/daily/00-archives-%s.html"
    
    batch = []
    urls = [ indexUrl ]
    for i in range(0, 12):
        urls.append(archiveUrl % ('%02d' % (i+1)))
        
    for url in urls:        
        try:
            info("Opening url [%s]"% (url))
            urlResp = urlopen(url, timeout=60)
            html = urlResp.read()
            urlResp.close()
            
            # instantiate the parser and fed it some HTML
            htmlparser = JPXReports()
            htmlparser.feed(str(html))
            batch = batch + htmlparser.data
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
    print  ("  \t-t,\t\ttype [batch|daily]")
    print  ("  \t-o,\t\toutput directory")
    print  ("  \t-h,\t\thelp")
                          
def main(argv):
    argType = "daily"
    argOutput = "/dfs/raw/live/jpx/"
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

    if len(argType) == 0:
        print_usage()
        sys.exit()

    if argType.lower() == "daily":
        download_daily(argOutput)
    elif argType.lower() == "batch":
        download_batch(argOutput)
    else:
        error("Unknown type [%s]" % (argType))

if __name__ == '__main__':
    main(sys.argv[1:])
