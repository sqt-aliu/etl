#!/opt/anaconda3/bin/python -u
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

def convert_dividends(cdate, cinput, coutput):             
    strdate = datetime.strftime(cdate,"%Y%m%d")
    jsonfile = cinput + "/" + strdate + "/" + strdate + ".dividends.json"
    if os.path.exists(jsonfile):
        divdict = {}
        info("Reading json file [%s]" % (jsonfile))
        jsondata=open(jsonfile, 'r').read()
        jsonmap = json.loads(jsondata)
        
        #pp = pprint.PrettyPrinter(indent=4)
        #pp.pprint(jsonmap)
        for divitem in jsonmap:
            code = divitem[0]
            if code != "Code":
                exdate = datetime.strftime(datetime.strptime(divitem[3], "%d %b %Y"), "%Y%m%d")
                amount = float(divitem[4].replace('$',''))
                if not code in divdict:
                    divdict[code] = {}
                if not exdate in divdict[code]:
                    divdict[code][exdate] = amount
                else:
                    divdict[code][exdate] += amount
     
        csvfile = coutput + "/" + strdate + "/" + strdate + ".divs.csv"
        write_dividends(csvfile, divdict)     
    else:
        error("File [%s] not found" % ( jsonfile))
        
def write_dividends(file, map):
    def get_key(dict, key):
        if key in dict:
            return dict[key]
        return ''

    if not os.path.exists(os.path.dirname(file)):
        info("Creating directory %s" % (os.path.dirname(file)))
        os.makedirs(os.path.dirname(file))
    info("Saving download to %s" % (file))
    with open(file, "w") as fh:
        fh.write("code,date,dividend\n")
        for k1,v1 in sorted(map.items()):
            for k2,v2 in sorted(v1.items()):
                fh.write("%s,%s,%f\n" % (k1, k2, v2))
        fh.close()
        
def print_usage():
    print  ("  Usage: %s [options]" % (os.path.basename(__file__))) 
    print  ("  Options:")
    print  ("  \t-s, --start\tstart date")
    print  ("  \t-e, --end\tend date [optional]")
    print  ("  \t-i, --input\tinput directory")
    print  ("  \t-o, --output\toutput directory")
    print  ("  \t-h,\t\thelp")
                          
def main(argv):
    argInput = "/dfs/raw/live/marketindex/"
    argOutput = "/dfs/stage/marketindex/"
    argStart = datetime.strptime(datetime.strftime(date.today(), "%Y%m%d"), "%Y%m%d")
    argEnd = datetime.strptime(datetime.strftime(date.today(), "%Y%m%d"), "%Y%m%d")
    
    try:
        opts, args = getopt.getopt(argv,"hs:e:i:o:",["start=","end=","input=","output="])
    except getopt.GetoptError:
        print_usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print_usage()
            sys.exit()                  
        elif opt in ("-o", "--output"):
            argOutput = arg     
        elif opt in ("-i", "--input"):
            argInput = arg               
        elif opt in ("-s", "--start"):
            argStart = datetime.strptime(arg, "%Y%m%d")  
        elif opt in ("-e", "--end"):
            argEnd = datetime.strptime(arg, "%Y%m%d")     
            
    caldates = calendar_days(argStart, argEnd)
    for caldate in caldates:
        convert_dividends(caldate, argInput,  argOutput)

if __name__ == '__main__':
    main(sys.argv[1:])
