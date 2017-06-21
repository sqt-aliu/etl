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

supported_types = ['jpx','asx','hkex','sse', 'szse']

def convert_summary(cdate, ctype, cinput, coutput):
                       
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
                if 'QuoteSummaryStore' in jsonval:
                    quotemap = jsonval['QuoteSummaryStore']
                    quotdict[jsonkey] = quotemap
        jsonfile2 = coutput + "/" + strdate + "/" + strdate + "." + ctype + ".summary.json.new"
        write_json(jsonfile2, quotdict)
    else:
        error("File [%s] not found" % ( jsonfile))
        
def write_json(file, map):
    if not os.path.exists(os.path.dirname(file)):
        info("Creating directory %s" % (os.path.dirname(file)))
        os.makedirs(os.path.dirname(file))       
    info("Saving download to %s" % (file))
    with open(file, "w") as fh:
        j = json.dumps(map, indent=4, sort_keys=True)
        fh.write(j)
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
    argOutput = "/dfs/raw/live/yahoo/"
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
