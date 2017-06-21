# -*- coding: utf-8 -*-
from html.parser import HTMLParser
from re import findall, sub


class AAStocksDividends(HTMLParser):
    def __init__(self, ticker):
        HTMLParser.__init__(self)
        self.ticker = ticker
        self.data = {}
        self.data[self.ticker] = []
        self.table = 0
        self.span = 0
        self.record = 0
        self.recordtr = 0
        self.recordtd = 0
        self.recorditems = []
        self.recorddata = ""
        

    def handle_starttag(self, tag, attrs):
        if tag == 'table':
           self.table = 1
        if self.table > 0 and tag == 'span':
            self.span = 1
        if self.record > 0 and tag == 'tr':
            self.recordtr = 1
        if self.recordtr > 0 and tag == 'td':
            self.recordtd = 1
       
    def handle_endtag(self, tag):
        if self.recordtr > 0 and tag == 'td':
            self.recorditems.append(self.recorddata)   
            self.recorddata = ""
        if self.record > 0 and tag == 'tr':
            self.recordtr = 0
            self.recordtd = 0
            if len(self.recorditems) > 0:
                self.data[self.ticker].append(self.recorditems)
            self.recorditems = []
        if self.table > 0 and tag == 'span':
            self.span = 0
        if tag == 'table' :
            self.table = 0
            self.record = 0 
            
    def handle_data(self, data):
        if self.table > 0 and self.span > 0:
            if 'Dividend History' in data:
                self.record = 1

        if self.recordtd > 0:
            self.recorddata += data.strip()
            
                
class AAStocksDividendsCNHK(HTMLParser):
    def __init__(self, ticker):
        HTMLParser.__init__(self)
        self.ticker = ticker
        self.data = {}
        self.data[self.ticker] = []
        self.table = 0
        self.record = 0
        self.recordtr = 0
        self.recordtd = 0
        self.recorditems = []
        self.recorddata = ""
        

    def handle_starttag(self, tag, attrs):
        if tag == 'table':
           self.table = 1
        if self.record > 0 and tag == 'tr':
            self.recordtr = 1
        if self.recordtr > 0 and tag == 'td':
            self.recordtd = 1
       
    def handle_endtag(self, tag):
        if self.recordtr > 0 and tag == 'td':
            self.recorditems.append(self.recorddata)   
            self.recorddata = ""
        if self.record > 0 and tag == 'tr':
            self.recordtr = 0
            self.recordtd = 0
            if len(self.recorditems) > 0:
                self.data[self.ticker].append(self.recorditems)
            self.recorditems = []
        if tag == 'table' :
            self.table = 0
            self.record = 0 
            
    def handle_data(self, data):
        if self.table > 0:
            if 'Dividend History' in data:
                self.record = 1

        if self.recordtd > 0:
            self.recorddata += data.strip()
            
                
                                