# -*- coding: utf-8 -*-
from html.parser import HTMLParser
from re import findall, sub
import json
import os
import sys
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn

class YahooSummary(HTMLParser):
    def __init__(self, ticker):
        HTMLParser.__init__(self)
        self.ticker = ticker
        self.data = {}
        self.data[self.ticker] = {}
        self.record = 0

    def handle_starttag(self, tag, attrs):
        if tag == 'script':
           self.record = 1
       
    def handle_endtag(self, tag):
        if tag == 'script' :
            self.record = 1
            
    def handle_data(self, data):
        data = data.strip().replace(',','')
        if self.record == 1 and 'root.App.main' in data:
            lines = data.split('\n')
            for line in lines:
                if 'root.App.main' in line:
                    (before, after) = line.split('=', 1)
                    after = after.strip().strip(';')
                   
                    
                    after = after.replace('}\"', '},\"')
                    after = after.replace('}{','},{')
                    after = after.replace(']\"', '],\"')
                    after = after.replace('\"{', '\",{')
                    after = after.replace('\"\"\"','\"\",\"')
                    after = after.replace('\"\"','\",\"')
                    after = after.replace('true\"','true,\"')
                    after = after.replace('false\"','false,\"')
                    after = after.replace('null\"','null,\"')
                    findings = findall(r':\s*\-*[0-9]+"', after)
                    findings = set(findings)
                    for finding in findings:
                        after = after.replace(finding, finding.replace('\"',',\"'))
                    findings = findall(r':\s*\-*[0-9]+\.[0-9]+"', after)                    
                    findings = set(findings)
                    for finding in findings:
                        after = after.replace(finding, finding.replace('\"',',\"'))   
                    
                    after = after.replace('null','')
                    after = after.replace(':,',':\"\",')
                    after = after.replace(':}',':\"\"}')
                    try:
                        stores = json.loads(after)
                        if 'context' in stores:
                            if 'dispatcher' in stores['context']:
                                if 'stores' in stores['context']['dispatcher']:
                                    if 'QuoteSummaryStore' in stores['context']['dispatcher']['stores']:
                                        self.data[self.ticker] = stores['context']['dispatcher']['stores']['QuoteSummaryStore']
                                    else:
                                        error("No 'QuoteSummaryStore' found for %s" % (self.ticker))
                                else:
                                    error("No 'stores' found for %s" % (self.ticker))
                            else: 
                                error("No 'dispatcher' found for %s" % (self.ticker))
                        else:
                            error("No 'context' found for %s" % (self.ticker))
                    except ValueError as e:
                        error("ValueError => " + e.reason)
                        raise     

                    break
                    #stores = json.loads(after)
                    #print (after)
                    #stores =  findall(r'\"QuoteSummaryStore\".*\"FinanceConfigStore\"', after)  
                    #if len(stores) > 0:
                    #    store = '{' + stores[0].replace(',\"FinanceConfigStore\"','') + '}'
                    #    try:
                    #        self.data[self.ticker] = json.loads(store)
                    #    
                    #    except ValueError as e:
                    #        error("ValueError => " + e.reason)
                    #        raise                    
                    #else:
                    #    error("No QuoteSummaryStore found")
                    #break
                
                
                
                
