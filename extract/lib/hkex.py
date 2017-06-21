# -*- coding: utf-8 -*-
from html.parser import HTMLParser
from re import sub, match, findall
from datetime import datetime
# create a subclass and override the handler methods
class HKEXSecurityList(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.table = 0
        self.tabledata = 0
        self.stockcode = None
        self.cellctr = 0
        self.data = {}

    def handle_starttag(self, tag, attrs):
        if tag == 'table':
            for name, value in attrs:
                if name == 'class' and value == 'table_grey_border':
                    self.table = 1             

        if tag == 'tr' and self.table > 0:
            self.cellctr = 0
        if tag == 'td' and self.table > 0:
            self.tabledata = 1   
                
    def handle_endtag(self, tag):
        if tag == 'table' and self.table > 0:
            self.table = 0
            
    def handle_data(self, data):
        data = data.replace("\\r\\n", "").strip()
        if self.table > 0 and self.tabledata > 0 and len(data) > 0:
            if self.cellctr == 0:
                if 'STOCK' not in data:
                    self.stockcode = data
                    self.data[self.stockcode] = {}
                    self.data[self.stockcode]["CCASS"] = 'F'
                    self.data[self.stockcode]["SHORT"] = 'F'
                    self.data[self.stockcode]["SSO"] = 'F'
                    self.data[self.stockcode]["SSF"] = 'F'
                    
            elif self.cellctr == 1 and self.stockcode is not None:
                self.data[self.stockcode]['NAME'] = sub('\s+', ' ', data.replace(',', ''))
            elif self.cellctr == 2 and self.stockcode is not None:
                self.data[self.stockcode]["LOTSIZE"] = data.replace(',','')
            elif self.cellctr < 7 and self.stockcode is not None:
                if data == "#":
                    self.data[self.stockcode]["CCASS"] = 'T'
                if data == "H":
                    self.data[self.stockcode]["SHORT"] = 'T'
                if data == "O":
                    self.data[self.stockcode]["SSO"] = 'T'                
                if data == "F":
                    self.data[self.stockcode]["SSF"] = 'T'
            self.cellctr += 1