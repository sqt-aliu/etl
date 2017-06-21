# -*- coding: utf-8 -*-
from html.parser import HTMLParser
from datetime import datetime
import os
import sys
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn

# create a subclass and override the handler methods
class GoogleHistorical(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.table = 0
        self.tabledata = 0
        self.datactr = 0
        self.datefmt = 0
        self.data = {}

    def handle_starttag(self, tag, attrs):
        if tag == 'table':
            for name, value in attrs:
                if name == 'class' and 'historical_price' in value:
                    self.table = 1 
        if tag == 'tr' and self.table > 0:
            self.datactr = 0
        if tag == 'td' and self.table > 0:
            self.tabledata = 1   
           
    def handle_endtag(self, tag):
        if tag == 'td' and self.table > 0:
            self.tabledata = 0           
        if tag == 'table' and self.table > 0:
            self.table = 0
            
    def handle_data(self, data):
        data = data.strip()
        if self.table > 0 and self.tabledata > 0 and len(data) > 0:
            self.datactr += 1
            data = data.replace(',','').replace('-','').strip()
            if self.datactr == 1:
                mon,day,year = data.split(" ")
                datestr = mon + '-' + format(int(day), "02d") + '-' + year
                date = datetime.strptime(datestr, "%b-%d-%Y")
                self.datefmt = date
                
                self.data[self.datefmt] = {}
            if self.datactr == 2: #Open
                self.data[self.datefmt]['OP'] = float(data) if len(data)>0 else 'NULL'
            if self.datactr == 3: #High
                self.data[self.datefmt]['HI'] = float(data) if len(data)>0 else 'NULL'
            if self.datactr == 4: #Low
                self.data[self.datefmt]['LO'] = float(data) if len(data)>0 else 'NULL'
            if self.datactr == 5: #Close
                self.data[self.datefmt]['CL'] = float(data) if len(data)>0 else 'NULL'
            if self.datactr == 6: #Volume
                self.data[self.datefmt]['VOL'] = int(data) if len(data)>0 else 'NULL'