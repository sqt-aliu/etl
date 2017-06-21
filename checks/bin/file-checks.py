#!/opt/anaconda3/bin/python -u

import csv
import getopt
import glob
import gzip
import os
import sys
from datetime import datetime, timedelta
from time import sleep
from xml.dom import minidom
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn
from common.lib.cal import exchange_holidays
from common.lib.mail import mail_html
WARN = 1
ERROR = 2
SUCCESS = 3

config_map = {}

def count_check(filefmt, dates):
    rowcntsum = 0
    rowcnt = 0
    summary = []
    for date in dates:
        file = format_file(filefmt, date)
        files = glob.glob(file)
        if len(files) > 0:
            foundfile = max(files, key=os.path.getctime)
            linecnt = file_linecount(foundfile)    
            rowcntsum += linecnt
            rowcnt += 1
            details = "File '%s', %i lines" % (foundfile, linecnt)
            summary.append(details)
            info(details)
        else:
            details = "File missing '%s'" % (file)
            summary.append(details)
            info(details)
    
    return (rowcntsum / rowcnt, summary)
            
def file_linecount(file):
    cnt = 0
    if (file.endswith(".gz")):
        cnt = sum(1 for line in gzip.open(file))
    else:
        cnt = sum(1 for line in open(file, 'r', encoding="ISO-8859-1"))
    return cnt
    
def file_check(argv):
    filefmt = argv[0]
    tolerance = float(argv[1])
    exchcode = argv[2] if len(argv) >= 3 else None
    holidays = exchange_holidays(exchcode)
    if datetime.strftime(datetime.now(), "%Y%m%d") in holidays:
        return (SUCCESS, "Exchange Holiday '%s'. Skipping..." % (exchcode))
        
    file = format_file(filefmt, datetime.now(), holidays)
    files = glob.glob(file)
    dates = business_days(datetime.now(), 5, holidays)
    if len(files) > 0:
        foundfile = max(files, key=os.path.getctime)
        tdycnt = file_linecount(foundfile)
        avgcnt, info = count_check(filefmt, dates)
        info.insert(0, "File '%s', %i lines" % (foundfile, tdycnt))
        pctchg = ((tdycnt - avgcnt) / avgcnt) if avgcnt > 0. else 0.
        if abs(pctchg) > tolerance:
            return (WARN, "File '%s' exceeds tolerance levels<br>Stats: pctchg=%0.2f, tolerance=%0.2f, today=%i, avg=%0.2f<br>%s" % (file, pctchg * 100., tolerance * 100., tdycnt, avgcnt, "<br>".join(info)))
        else:
            return (SUCCESS, "Stats: pctchg=%0.2f, tolerance=%0.2f, today=%i, avg=%0.2f<br>%s" % (pctchg * 100., tolerance * 100., tdycnt, avgcnt, "<br>".join(info)))         
    else:
        return (ERROR, "File '%s' does not exist" % (file))
           
           
def format_file(file, time, holidays=[]):
    pdate = business_days(time, 1, holidays)[0]
    file = file.replace('PYYYYMMDD', datetime.strftime(pdate, "%Y%m%d"))
    file = file.replace('PYYYYMM', datetime.strftime(pdate, "%Y%m"))
    file = file.replace('PYYYY', datetime.strftime(pdate, "%Y"))   
    
    file = file.replace('YYYYMMDD', datetime.strftime(time, "%Y%m%d"))
    file = file.replace('YYYYMM', datetime.strftime(time, "%Y%m"))
    file = file.replace('YYYY', datetime.strftime(time, "%Y"))    
    return file
                      
def format_time(time):
    today = datetime.strftime(datetime.now(), "%Y%m%d")
    return datetime.strptime(today + "T" + time, "%Y%m%dT%H:%M:%S")
    
def business_days(date, daysBack=10, holidays=[]):
    dates = []
    count = 1
    while len(dates) < daysBack:
        delta = timedelta(days=(-1 * count))
        checkdate = date + delta
        formatdate = datetime.strftime(checkdate, "%Y%m%d")
        if checkdate.weekday() not in set([5,6]) and formatdate not in holidays:
            dates.append(checkdate)
        count += 1
    return (dates)
    
def load_config(config):
    info("Reading file " + config)
    xmldoc = minidom.parse(config)
    for groupNode in xmldoc.getElementsByTagName('checkgroup'):
        groupName = groupNode.getAttribute('name')
        groupTime = groupNode.getAttribute('time')
        config_map[groupName] = {}
        config_map[groupName]['TIME'] = format_time(groupTime)
        config_map[groupName]['COMPLETED'] = False
        config_map[groupName]['CHECKS'] = {}
        
        for checkNode in groupNode.getElementsByTagName('check'):
            checkName = checkNode.getAttribute('name')
            checkType = checkNode.getAttribute('type')
            checkArgs = checkNode.getAttribute('args')
            config_map[groupName]['CHECKS'][checkName] = {}
            config_map[groupName]['CHECKS'][checkName]['TYPE'] = checkType
            config_map[groupName]['CHECKS'][checkName]['ARGS'] = checkArgs

def process_checks(group, checks):
    alarm = False
   
    html = "<html>"
    html = apply_style(html)
    html += "<body>\n<table>\n<tr><th>alert</th><th>name</th><th>message</th></tr>\n"
    
    for check_key, check_vals in checks.items():
        check_type = check_vals['TYPE']
        check_args = check_vals['ARGS']
        info("Processing '%s', type='%s', args='%s'" % (check_key, check_type, check_args))
        if check_type == 'filecheck':
            check_result = file_check(check_args.split(','))
            if check_result[0] == SUCCESS:
                html += "<tr class='info'><td>INFO</td><td>%s</td><td>%s</td></tr>" % (check_key, check_result[1])
            elif check_result[0] == WARN:
                html += "<tr class='warn'><td>WARN</td><td>%s</td><td>%s</td></tr>" % (check_key, check_result[1])
                alarm = True
            else:
                html += "<tr class='error'><td>ERROR</td><td>%s</td><td>%s</td></tr>" % (check_key, check_result[1])
                alarm = True          
        else:
            error("Unknown check type '%s'" % (check_type))    

    html += "</body>\n</table>\n</html>"
    info(html)

    subj = "[%s] [%s] Monitor" % ("ALARM" if alarm else "OK", group)
    dist = "allenliu@htsc.com"
    mail_html(html, subj, dist, "sqt@htsc.com")
    #mail_html(html, subj, "allenliu@htsc.com", "sqt@htsc.com")
    
def apply_style(html):
    html += """<style>
    table {
      border-collapse: collapse;
      border: 1px solid #666666;
      font: normal 11px verdana, arial, helvetica, sans-serif;
      color: #363636;
      background: #f6f6f6;
      text-align:left;
      }
    caption {
      text-align: center;
      font: bold 16px arial, helvetica, sans-serif;
      background: transparent;
      padding:6px 4px 8px 0px;
      color: #CC00FF;
      text-transform: uppercase;
    }
    thead, tfoot {
      text-align:left;
      height:30px;
    }
    thead th, tfoot th {
      padding:5px;
    }
    table a {
      color: #333333;
      text-decoration:none;
    }
    table a:hover {
      text-decoration:underline;
    }
    tr.error {
      background: #FF2400;
    }
    tr.warn {
      background: #FFF8C6;
    }
    tr.info {
      background: #FFFFFF;
    }       
    tbody th, tbody td {
      padding:5px;
    }
    </style>
    """
    return(html)    
    
def print_usage():
    print  ("  Usage: %s [options]" % (os.path.basename(__file__))) 
    print  ("  Options:")
    print  ("  \t-c, --config\tconfig file")
    print  ("  \t-i, --interval\tinterval (default=60)")
    print  ("  \t-e, --end\tend time (default=23:59:00)")
    print  ("  \t-r, --runonly\trun item")
    print  ("  \t-h,\t\thelp")

def main(argv):
    config = ""
    end = "23:59:00"
    interval = 60
    runonly = None
    try:
        opts, args = getopt.getopt(argv,"hc:p:i:e:r:",["config=", "interval=","end=","runonly="])
    except getopt.GetoptError:
        print_usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print_usage()
            sys.exit()
        elif opt in ("-c", "--config"):
            config = arg
        elif opt in ("-i", "--interval"):
            interval = int(arg)
        elif opt in ("-e", "--end"):
            end = arg   
        elif opt in ("-r", "--runonly"):
            runonly = arg                

    if len(config) == 0:
        print_usage()
        sys.exit(-1)

    if not os.path.exists(config):
        error("File not found: %s" % (config))
        sys.exit(-1)
        
    endtime = format_time(end)
    info("Process check will stop at [%s]" %(str(endtime)))
    
    load_config(config)
    
    if runonly is not None:
        if runonly in config_map:
            process_checks(runonly, config_map[runonly]['CHECKS'])
        else:
            warn("Run only group '%s' not found" % (runonly))
        exit(0)

    while datetime.now() <= endtime:
        for config_key, config_vals in sorted(config_map.items()):
            if datetime.now() > config_vals['TIME'] and not config_vals['COMPLETED']:
                info("Processing '%s'" % (config_key))
                process_checks(config_key, config_vals['CHECKS'])
                config_vals['COMPLETED'] = True # mark complete
        sleep(interval)
    
if __name__ == '__main__':
    main(sys.argv[1:])

