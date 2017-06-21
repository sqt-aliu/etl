# -*- coding: utf-8 -*-
from html.parser import HTMLParser
from re import findall, sub

class MarketIndexDividends(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.data = []
        self.table = 0
        self.tr = 0
        self.td = 0
        self.celldata = ""
        self.rowdata = []


    def handle_starttag(self, tag, attrs):
        if tag == 'table':
            for name, value in attrs:
                if name == 'class' and 'div' in value:
                    self.table = 1                          
        if self.table > 0 and tag == 'tr':
            self.tr = 1
        if self.tr > 0 and tag in ['td','th']:
            self.td = 1

       
    def handle_endtag(self, tag):
        if self.tr > 0 and tag in ['td','th']:
            self.td = 0
            self.rowdata.append(self.celldata)
            self.celldata = ""
        if self.table > 0 and tag == 'tr':
            self.tr = 0
            self.data.append(self.rowdata)
            self.rowdata = []
        if tag == 'table':
            self.table = 0
     
            
    def handle_data(self, data):
        data = data.replace("\\n", "").strip()
        if self.table > 0 and self.td > 0:
            self.celldata += data
                
                