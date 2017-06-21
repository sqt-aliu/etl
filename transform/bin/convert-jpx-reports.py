#!/opt/anaconda3/bin/python -u
import glob
import re
import sys
import getopt
import os.path
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn

def isvalid(item):
    if item.strip() == "-":
        return True
    try:
        item = item.replace(',','')
        item = item.replace('ｶ','')
        item = item.replace('ｳ','')
        float(item)
        return True
    except ValueError:
        return False
        
def tofloat(item):
    if item.strip() == "-":
        return 0.
    try:
        return float(item.replace(',',''))
    except ValueError:
        return 0.   

def toint(item):
    if item.strip() == "-":
        return 0
    try:
        return int(item.replace(',',''))
    except ValueError:
        return 0           

def write_csv(file, map):
    def find(dict, key):
        if key in dict:
            return dict[key]
        return ''

    if not os.path.exists(os.path.dirname(file)):
        info("Creating directory %s" % (os.path.dirname(file)))
        os.makedirs(os.path.dirname(file))
    info("Saving download to %s" % (file))
    with open(file, "w") as fh:
        fh.write("code,lotsize,open,high,low,close,vwap,volume\n")
        for k,v in sorted(map.items()):
            fh.write("%s,%s,%s,%s,%s,%s,%s,%s\n" % (k, find(v,'LOTSZ'), find(v,'OPEN'), find(v,'HIGH'), find(v,'LOW'), find(v,'CLOSE'), find(v,'VWAP'), find(v,'VOLUME')))
        fh.close()
        
def convert_pdf(pdffile, csvfile):
    info("Converting pdf [%s] to csv [%s]" %(pdffile, csvfile))
    basename = os.path.basename(pdffile)
    tmpfile = "/var/tmp/" + basename.replace('pdf', 'txt')
    os.system("/usr/bin/pdftotext -layout %s %s" % (pdffile, tmpfile))
    if os.path.exists(tmpfile):
        quotes = {}
        with open(tmpfile, 'r') as fh:
            reserve = None
            for line in fh:
                line = line.strip()
                if "Regular Way" in line and "ToSTNeT" in line:
                    break
                
                parts = line.split()
                if len(parts) > 0 and isvalid(parts[0]):
                    parts[:] = [x for x in parts if isvalid(x)]
                    if len(parts) == 1:
                        reserve = parts
                    elif len(parts) < 14:
                        if reserve is not None:
                            parts = reserve + parts
                            if len(parts) > 15:
                                warn("Too many parts found (>15)")
                        reserve = parts    
                        
                    elif len(parts) == 14:
                        if reserve is not None:
                            parts = reserve + parts
                            reserve = None
                        else:
                            warn("Missing reserve item (=14)")
                        
                    if len(parts) == 15:
                        code = parts[0]
                        lotsz = toint(parts[1])
                        amop = tofloat(parts[2])
                        amhi = tofloat(parts[3])
                        amlo = tofloat(parts[4])
                        amcl = tofloat(parts[5])
                        pmop = tofloat(parts[6])
                        pmhi = tofloat(parts[7])
                        pmlo = tofloat(parts[8])
                        pmcl = tofloat(parts[9])
                        vwap = tofloat(parts[12])
                        vol = int(tofloat(parts[13])*1000)
                        
                        quotes[code] = {}
                        quotes[code]['LOTSZ'] = lotsz
                        quotes[code]['OPEN'] = amop if amop > 0. else (pmop if pmop > 0. else '')
                        quotes[code]['HIGH'] = amhi if (amhi > 0. and pmhi > 0. and amhi >= pmhi) or (amhi > 0. and pmhi == 0.) else (pmhi if (amhi > 0. and pmhi > 0. and pmhi > amhi) or (pmhi > 0. and amhi == 0.) else '')
                        quotes[code]['LOW'] = amlo if (amlo > 0. and pmlo > 0. and amlo <= pmlo) or (amlo > 0. and pmlo == 0.) else (pmlo if (amlo > 0. and pmlo > 0. and pmlo < amlo) or (pmlo > 0. and amlo == 0.) else '')
                        quotes[code]['CLOSE'] = pmcl if pmcl > 0. else (amcl if amcl > 0. else '')
                        quotes[code]['VOLUME'] = vol if vol > 0. else ''
                        quotes[code]['VWAP'] = vwap if vwap > 0. else ''
                        reserve = None
           
                else:
                    pass
            fh.close()
        info("Removing tmp file [%s]" % (tmpfile))
        os.system("rm %s" % (tmpfile))     
        
        write_csv(csvfile, quotes)
    else:
        error("Error attempting to convert pdf to text [%s]" % (pdffile))
    

def convert_reports(inputdir, outputdir, force=False):
    pdfs = sorted(glob.glob(inputdir + "/*/stq_*.pdf"))
    for pdf in pdfs:
        basename = os.path.basename(pdf)
        match = re.search("(20[0-2][0-9]{5})", basename)
        if match:
            convert_output = outputdir + "/" + match.group() + "/" + match.group() + ".stockquotes.csv"
            if not os.path.exists(convert_output) or force:
                convert_pdf(pdf, convert_output)
            else:
                warn("File [%s] already exists" % (convert_output))
                
def print_usage():
    print  ("  Usage: %s [options]" % (os.path.basename(__file__))) 
    print  ("  Options:")
    print  ("  \t-i, --input\tinput directory")
    print  ("  \t-o, --output\toutput directory")
    print  ("  \t-f, --force\tforce")
    print  ("  \t-h,\t\thelp")
                          
def main(argv):
    argInput = "/dfs/raw/live/jpx/"
    argOutput = "/dfs/stage/jpx/"
    argForce = False
    try:
        opts, args = getopt.getopt(argv,"hi:o:f:",["input=","output=","force"])
    except getopt.GetoptError:
        print_usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print_usage()
            sys.exit()           
        elif opt in ("-i", "--input"):
            argInput = arg           
        elif opt in ("-o", "--output"):
            argOutput = arg     
        elif opt in ("-f", "--force"):
            argForce = True                 

    convert_reports(argInput, argOutput, argForce)
    
if __name__ == '__main__':
    main(sys.argv[1:])
