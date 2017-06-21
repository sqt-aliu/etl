#!/opt/anaconda3/bin/python -u
import json
import sys
import getopt
import os.path
import pprint
from lxml import etree
from re import findall
from datetime import datetime, date
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.cal import business_days, calendar_days
from common.lib.log import debug, error, fatal, info, warn

supported_types = ['hkex','sse', 'szse']

def convert_dividends(cdate, ctype, cinput, coutput):
    def capture_dividend(capdict, symbol, exdate, dividend):
        if not symbol in capdict:
            capdict[symbol] = {}
        if not exdate in capdict[symbol]:
            capdict[symbol][exdate] = {}
        if not 'DIVIDEND' in capdict[symbol][exdate]:
            capdict[symbol][exdate]['DIVIDEND'] = float(dividend)
        else:
            capdict[symbol][exdate]['DIVIDEND'] += float(dividend)
            
    def capture_split(capdict, symbol, exdate, split):
        if not symbol in capdict:
            capdict[symbol] = {}
        if not exdate in capdict[symbol]:
            capdict[symbol][exdate] = {}
        if not 'SPLIT' in capdict[symbol][exdate]:
            capdict[symbol][exdate]['SPLIT'] = split
        else:
            warn("Shouldn't get to this point [%s][%s][%f].  Possible duplicate entry" % (symbol, exdate,split))
                        
    # for china use only
    def capture_bonus(capdict, symbol, exdate, split):
        if not symbol in capdict:
            capdict[symbol] = {}
        if not exdate in capdict[symbol]:
            capdict[symbol][exdate] = {}
        if not 'SPLIT' in capdict[symbol][exdate]:
            capdict[symbol][exdate]['SPLIT'] = split
        else:
            origsplit = (1. / capdict[symbol][exdate]['SPLIT']) - 1.
            newsplit = (1. / split) - 1.
            capdict[symbol][exdate]['SPLIT'] = 1. / (1. + (origsplit + newsplit))
            warn("Bonus adjustments [%s][%s][First=%f][Second=%f][Final=%f]" % (symbol, exdate, origsplit, newsplit, capdict[symbol][exdate]['SPLIT']))
                        
    strdate = datetime.strftime(cdate,"%Y%m%d")
    jsonfile = cinput + "/" + strdate + "/" + strdate + "." + ctype + ".dividends.json"
    if os.path.exists(jsonfile):
        adjdict = {}
        info("Reading json file [%s]" % (jsonfile))
        jsondata=open(jsonfile, 'r').read()
        jsonmap = json.loads(jsondata)
        for jsonkey, jsonval in jsonmap.items():
            if type(jsonval) is list:
                for jsonitem in jsonval:
                    if len(jsonitem) > 1:
                        if not 'DATE' in jsonitem[0].upper():
                            if not jsonitem[5].strip() == '-':
                                exdate = datetime.strftime(datetime.strptime(jsonitem[5], '%Y/%m/%d'), '%Y%m%d')
                                if ctype == 'hkex':
                                    #print (exdate)
                                    particular = jsonitem[3]
                                    if particular.startswith('D:'):
                                        particular = particular.replace('D:','').strip()
                                        if '(' in particular:
                                            findings = findall(r'HKD\s+\d+\.*\d*', particular)
                                            if len(findings) > 0:
                                                findings2 = []
                                                for finding in findings:
                                                    findings2 = findings2 + findall(r'\d+\.*\d*', finding)
                                                    
                                                capture_dividend(adjdict, jsonkey, exdate, findings2[-1])
                                            else:
                                                findings = findall(r'\d+\.*\d*', particular)
                                                if len(findings) == 1:
                                                    capture_dividend(adjdict, jsonkey, exdate, findings[-1])
                                                else:
                                                    warn("Found more than 1 dividend [%s][%s][%s]" % (jsonkey, exdate, particular))                                            
                                        else:
                                            findings = findall(r'\d+\.*\d*', particular)
                                            if len(findings) == 1:
                                                capture_dividend(adjdict, jsonkey, exdate, findings[0])
                                            else:
                                                warn("Found more than 1 dividend [%s][%s][%s]" % (jsonkey, exdate, particular)) 
                                    elif particular.startswith('SD:'):
                                        particular = particular.replace('SD:','').strip()
                                        if '(' in particular:
                                            findings = findall(r'HKD\s+\d+\.*\d*', particular)
                                            if len(findings) > 0:
                                                findings2 = []
                                                for finding in findings:
                                                    findings2 = findings2 + findall(r'\d+\.*\d*', finding)
                                                    
                                                capture_dividend(adjdict, jsonkey, exdate, findings2[-1])
                                            else:
                                                findings = findall(r'\d+\.*\d*', particular)
                                                if len(findings) == 1:
                                                    capture_dividend(adjdict, jsonkey, exdate, findings[-1])
                                                else:
                                                    warn("Found more than 1 dividend [%s][%s][%s]" % (jsonkey, exdate, particular))                                          
                                        else:
                                            findings = findall(r'\d+\.*\d*', particular)
                                            if len(findings) == 1:
                                                capture_dividend(adjdict, jsonkey, exdate, findings[0])
                                            else:
                                                warn("Found more than 1 dividend [%s][%s][%s]" % (jsonkey, exdate, particular))                                 
                                    elif particular.startswith('S:'):
                                        particular = particular.replace('S:','').strip()
                                        findings = findall(r'\d+\.*\d*', particular)
                                        if len(findings) == 2:
                                            split = float(findings[1]) / float(findings[0])
                                            capture_split(adjdict, jsonkey, exdate, split)
                                        else:
                                            warn("Badly formed stock-split [%s][%s][%s]" % (jsonkey, exdate, particular))                                        
                                    elif particular.startswith('B:'):
                                        particular = particular.replace('B:','').strip()
                                        findings = findall(r'\d+\.*\d*', particular)
                                        if len(findings) == 2:
                                            bonus = float(findings[1]) / (float(findings[0]) + float(findings[1]))
                                            capture_split(adjdict, jsonkey, exdate, bonus)
                                        else:
                                            warn("Badly formed bonus-issue [%s][%s][%s]" % (jsonkey, exdate, particular))                                    
                                    elif particular.startswith('R:'):
                                        # skipping rights issuse
                                        #debug("Rights-issue [%s][%s][%s]" % (jsonkey, exdate, particular))                                 
                                        pass
                                    elif particular.startswith('C:'):
                                        particular = particular.replace('C:','').strip()
                                        findings = findall(r'\d+\.*\d*', particular)
                                        if len(findings) == 2:
                                            consolidation = float(findings[1]) / float(findings[0])
                                            capture_split(adjdict, jsonkey, exdate, consolidation)
                                        else:
                                            warn("Badly formed consolidation [%s][%s][%s]" % (jsonkey, exdate, particular))
                                    elif particular.startswith('BW:'):
                                        #debug("Bonus-options [%s][%s][%s]" % (jsonkey, exdate, particular))                                 
                                        pass
                                    elif particular.startswith('O:'):
                                        #debug("Open-offer [%s][%s][%s]" % (jsonkey, exdate, particular))                                 
                                        pass
                                    else:
                                        #debug("Unknown [%s][%s][%s]" % (jsonkey, exdate, particular))                                 
                                        pass
                                else: #szse and sse
                                    # compute china dividends
                                    dividend = jsonitem[2].strip()
                                    if not dividend == '-':
                                        findings = findall(r'\d+\.*\d*', dividend)
                                        if len(findings) == 1:
                                            capture_dividend(adjdict, jsonkey, exdate, float(findings[0])/10.) # need to divide by 10
                                        else:
                                            warn("Found more than 1 dividend [%s][%s][%s]" % (jsonkey, exdate, particular)) 
                                    # compute china bonus issues        
                                    bonus = jsonitem[3].strip()
                                    if not bonus == '-/-':
                                        parts = bonus.split('/')
                                        if not parts[0].strip() == '-':
                                            capture_bonus(adjdict, jsonkey, exdate, 1./(1.+(float(parts[0])/10.))) # need to divide by 10    
                                        if not parts[1].strip() == '-':
                                            capture_bonus(adjdict, jsonkey, exdate, 1./(1.+(float(parts[1])/10.))) # need to divide by 10                               
                                    
                            #else:
                                #warn("No Ex-Date found %s=> " % (str(jsonitem)))
                    else:
                        pass
                        #warn("No items found %s => " % (str(jsonitem)))
                    
            else:
                warn("Bad type found [%s][%s]" % (jsonkey, str(jsonval)))
        #pp = pprint.PrettyPrinter(indent=4)
        #pp.pprint(quotdict)        
    
        csvfile = coutput + "/" + strdate + "/" + strdate + "." + ctype + ".divs.csv"
        write_dividends(csvfile, adjdict)     
    else:
        error("File [%s] not found" % ( jsonfile))
        
def write_dividends(file, map):
    def get_key(dict, key):
        if key in dict:
            return round(dict[key], 4)
        return ''

    if not os.path.exists(os.path.dirname(file)):
        info("Creating directory %s" % (os.path.dirname(file)))
        os.makedirs(os.path.dirname(file))
    info("Saving download to %s" % (file))
    with open(file, "w") as fh:
        fh.write("code,date,dividend,split\n")
        for k1,v1 in sorted(map.items()):
            for k2,v2 in sorted(v1.items()):
                fh.write("%s,%s,%s,%s\n" % (k1, k2, get_key(v2, 'DIVIDEND'), get_key(v2, 'SPLIT')))
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
    argInput = "/dfs/raw/live/aastocks/"
    argOutput = "/dfs/stage/aastocks/"
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
        convert_dividends(caldate, argType, argInput,  argOutput)

if __name__ == '__main__':
    main(sys.argv[1:])
