# -*- coding: utf-8 -*-
from html.parser import HTMLParser
from datetime import datetime
import os
import sys
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn

class InvestingHistorical(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.data = {}
        self.table = 0
        self.tr = 0
        self.td = 0
        self.ctr = 0
        self.date = None
        
    def handle_starttag(self, tag, attrs):
        if tag == 'table':
            for name, value in attrs:
                if name == 'class' and 'Footer' not in value:
                    self.table = 1
                    
        if tag == 'tr' and self.table > 0:
            self.tr = 1
            self.date = None
            self.ctr = 0
        if tag == 'td' and self.tr > 0:
            self.td = 1
    
    def handle_endtag(self, tag):
        if tag == 'td' and self.tr > 0:
            self.td = 0
        if tag == 'tr' and self.table > 0:
            self.tr = 0
        if tag == 'table':
            self.table = 0
            
    def handle_data(self, data):
        data = data.replace('%', '').strip()
        if data == 'No results found':
            return
        if len(data) > 0 and self.td > 0:
            if data.endswith('K'):
                data = str(float(data.replace('K', '') * 1000))
            if data.endswith('M'):
                data = str(float(data.replace('M', '') * 1000000))
            self.ctr += 1
            if self.ctr == 1:
                self.date = datetime.strptime(data, '%b %d, %Y')
                self.data[self.date] = {}
            elif self.ctr == 2:
                self.data[self.date]['CL'] = float(data) if len(data) > 0 else 'NULL'
            elif self.ctr == 3:
                self.data[self.date]['OP'] = float(data) if len(data) > 0 else 'NULL'
            elif self.ctr == 4:
                self.data[self.date]['HI'] = float(data) if len(data) > 0 else 'NULL'
            elif self.ctr == 5:
                self.data[self.date]['LO'] = float(data) if len(data) > 0 else 'NULL'
            elif self.ctr == 6:
                self.data[self.date]['CHG'] = float(data) if len(data) > 0 else 'NULL'