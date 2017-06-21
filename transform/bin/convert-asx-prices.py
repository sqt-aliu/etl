#!/opt/anaconda3/bin/python -u
import glob
import sys
import getopt
import os.path
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn

def convert_prices(inputdir, outputdir, force=False, batch=False):
    if batch:
        zipfiles = sorted(glob.glob(inputdir + "/*.zip"))
    else:
        zipfiles = sorted(glob.glob(inputdir + "/*/*.zip"))
        
    for zipfile in zipfiles:
        tmpdir = "/var/tmp/" + os.path.basename(zipfile).replace('.zip','')
        os.system("/usr/bin/unzip -o %s -d %s" % (zipfile, tmpdir))
        txtfiles = glob.glob(tmpdir + "/*/*.*")
        for txtfile in txtfiles:
            if os.path.basename(txtfile.lower()).endswith('.txt'):
                info("Reading file [%s]" % (txtfile))
                csvfile = outputdir + "/" + os.path.basename(txtfile)[:8] + "/" + os.path.basename(txtfile)[:8] + ".stockquotes.csv"
                if not os.path.exists(csvfile) or force:
                    if not os.path.exists(os.path.dirname(csvfile)):
                        info("Creating directory %s" % (os.path.dirname(csvfile)))
                        os.makedirs(os.path.dirname(csvfile))              
                    with open(txtfile, 'r') as fr:
                        data = fr.read()
                        data = "code,date,open,high,low,close,volume\n" + data
                        with open(csvfile, 'w') as fw:
                            fw.write(data)
                            fw.close()
                        fr.close()
                else:
                    warn("File [%s] already exists" % (csvfile))
            else:
                error("File [%s] does not end with 'txt'" % (txtfile))
        info("Removing tmp directory [%s]" % (tmpdir))
        os.system("rm -rf %s" % (tmpdir))             
                
def print_usage():
    print  ("  Usage: %s [options]" % (os.path.basename(__file__))) 
    print  ("  Options:")
    print  ("  \t-i, --input\tinput directory")
    print  ("  \t-o, --output\toutput directory")
    print  ("  \t-f, --force\tforce")
    print  ("  \t-b, --batch\tbatch")
    print  ("  \t-h,\t\thelp")
                          
def main(argv):
    argInput = "/dfs/raw/live/asx/"
    argOutput = "/dfs/stage/asx/"
    argForce = False
    argBatch = False
    try:
        opts, args = getopt.getopt(argv,"hi:o:fb",["input=","output=","force","batch"])
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
        elif opt in ("-b", "--batch"):
            argBatch = True
            
    info("Batch = %s" % (str(argBatch)))            
    info("Force = %s" % (str(argForce)))        
    info("Input = %s" % (str(argInput)))   
    info("Output = %s" % (str(argOutput)))   
            
    convert_prices(argInput, argOutput, argForce, argBatch)
    
if __name__ == '__main__':
    main(sys.argv[1:])
