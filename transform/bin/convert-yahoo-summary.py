#!/opt/anaconda3/bin/python -u
import csv
import json
import sys
import getopt
import os.path
import pprint
from lxml import etree
from datetime import datetime, date
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.cal import business_days, calendar_days
from common.lib.log import debug, error, fatal, info, warn

supported_types = ['jpx','asx','hkex','sse','szse','krx']

def convert_summary(cdate, ctype, cinput, coutput):
    def handle_quote(quote, rootkey, quotekey, datatype='raw'):
        retval = ""
        if rootkey in quote:
            if quotekey in quote[rootkey]:
                if (type(quote[rootkey][quotekey]) is dict) and (datatype in quote[rootkey][quotekey]):
                    datastr = str(quote[rootkey][quotekey][datatype])
                    if "%" in datastr:
                        retval = datastr.rstrip("%")
                    else:
                        retval = quote[rootkey][quotekey][datatype]
        return (retval)
                        
    strdate = datetime.strftime(cdate,"%Y%m%d")
    jsonfile = cinput + "/" + strdate + "/" + strdate + "." + ctype + ".summary.json"
    if os.path.exists(jsonfile):
        quotdict = {}
        info("Reading json file [%s]" % (jsonfile))
        jsondata=open(jsonfile, 'r').read()

        jsonmap = json.loads(jsondata)
        #pp = pprint.PrettyPrinter(indent=4)
        #pp.pprint(jsonmap)        
        for jsonkey, jsonval in jsonmap.items():
            if type(jsonval) is dict:
                quotdict[jsonkey] = {}
                quotdict[jsonkey]['CLOSE'] = handle_quote(jsonval, 'price', 'regularMarketPrice')
                if len(str(quotdict[jsonkey]['CLOSE'])) == 0:
                    quotdict[jsonkey]['CLOSE'] = handle_quote(jsonval, 'summaryDetail', 'regularMarketPrice')
                if len(str(quotdict[jsonkey]['CLOSE'])) == 0:
                    quotdict[jsonkey]['CLOSE'] = handle_quote(jsonval, 'financialData', 'currentPrice')
                    
                quotdict[jsonkey]['OPEN'] = handle_quote(jsonval, 'price', 'regularMarketOpen')
                if len(str(quotdict[jsonkey]['OPEN'])) == 0:
                    quotdict[jsonkey]['OPEN'] = handle_quote(jsonval, 'summaryDetail', 'regularMarketOpen')   
                    
                quotdict[jsonkey]['HIGH'] = handle_quote(jsonval, 'price', 'regularMarketDayHigh')
                if len(str(quotdict[jsonkey]['HIGH'])) == 0:
                    quotdict[jsonkey]['HIGH'] = handle_quote(jsonval, 'summaryDetail', 'regularMarketDayHigh')  
                    
                quotdict[jsonkey]['LOW'] = handle_quote(jsonval, 'price', 'regularMarketDayLow')
                if len(str(quotdict[jsonkey]['LOW'])) == 0:
                    quotdict[jsonkey]['LOW'] = handle_quote(jsonval, 'summaryDetail', 'regularMarketDayLow')                        
                
                quotdict[jsonkey]['VOLUME'] = handle_quote(jsonval, 'price', 'regularMarketVolume')
                if len(str(quotdict[jsonkey]['VOLUME'])) == 0:
                    quotdict[jsonkey]['VOLUME'] = handle_quote(jsonval, 'summaryDetail', 'volume') 
            else:
                warn("Bad type found [%s][%s]" % (jsonkey, str(jsonval)))
        
        csvfile = coutput + "/" + strdate + "/" + strdate + "." + ctype + ".quotes.csv"
        write_quotes(csvfile, quotdict)
    else:
        error("File [%s] not found" % ( jsonfile))
        
def write_quotes(file, map):
    def get_key(dict, key):
        if key in dict:
            return dict[key]
        return ''

    if not os.path.exists(os.path.dirname(file)):
        info("Creating directory %s" % (os.path.dirname(file)))
        os.makedirs(os.path.dirname(file))
    info("Saving download to %s" % (file))
    with open(file, "w") as fh:
        fh.write("ticker,open,high,low,close,volume\n")
        for k,v in sorted(map.items()):
            fh.write("%s,%s,%s,%s,%s,%s\n" % (k, get_key(v, 'OPEN'), get_key(v, 'HIGH'), get_key(v, 'LOW'), get_key(v, 'CLOSE'), get_key(v, 'VOLUME')))
        fh.close()
        
def print_usage():
    print  ("  Usage: %s [options]" % (os.path.basename(__file__))) 
    print  ("  Options:")
    print  ("  \t-s, --start\tstart date")
    print  ("  \t-e, --end\tend date [optional]")
    print  ("  \t-t, --type\texchange types[%s]" % ("|".join(supported_types)))
    print  ("  \t-i, --input\tinput directory")
    print  ("  \t-o, --output\toutput directory")
    print  ("  \t-h,\t\thelp")
                          
def main(argv):
    argType = ""
    argInput = "/dfs/raw/live/yahoo/"
    argOutput = "/dfs/stage/yahoo/"
    argStart = datetime.strptime(datetime.strftime(date.today(), "%Y%m%d"), "%Y%m%d")
    argEnd = datetime.strptime(datetime.strftime(date.today(), "%Y%m%d"), "%Y%m%d")
    
    try:
        opts, args = getopt.getopt(argv,"hs:e:t:i:o:",["start=","end=","type=","input=","output="])
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
        elif opt in ("-i", "--input"):
            argInput = arg               
        elif opt in ("-s", "--start"):
            argStart = datetime.strptime(arg, "%Y%m%d")  
        elif opt in ("-e", "--end"):
            argEnd = datetime.strptime(arg, "%Y%m%d")     
            
    if len(argType) == 0 or argType not in supported_types:
        print_usage()
        sys.exit()

    caldates = calendar_days(argStart, argEnd)
    for caldate in caldates:
        convert_summary(caldate, argType, argInput,  argOutput)

if __name__ == '__main__':
    main(sys.argv[1:])
