# -*- coding: utf-8 -*-

from html.parser import HTMLParser

# create a subclass and override the handler methods
class XEHistorical(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.table = 0
        self.tr = 0
        self.td = 0
        self.ctr = 0
        self.ccy = None
        self.data = {}

    def handle_starttag(self, tag, attrs):
        if tag == 'table':
            for name, value in attrs:
                if name == 'id' and 'historicalRateTbl' in value:
                    self.table = 1 

        if tag == 'tr' and self.table > 0:
            self.tr = 1
            self.ctr = 0
            self.ccy = None
        if tag == 'td' and self.tr > 0:
            self.td = 1   
                
    def handle_endtag(self, tag):
        if tag == 'tr' and self.table > 0:
            self.tr = 0
        if tag == 'td' and self.tr > 0:
            self.td = 0
        if tag == 'table' and self.table > 0:
            self.table = 0
            
    def handle_data(self, data):
        data = data.strip()
       
        if self.table > 0 and self.td > 0 and len(data) > 0:
            self.ctr += 1
            if self.ctr == 1:
                self.ccy = data
            elif self.ctr == 4:
                self.data[self.ccy] = float(data)
                