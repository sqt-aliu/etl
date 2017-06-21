# -*- coding: utf-8 -*-
from html.parser import HTMLParser

# create a subclass and override the handler methods
class JPXReports(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.table = 0
        self.data = []

    def handle_starttag(self, tag, attrs):
        if tag == 'table':
            self.table = 1 
        if tag == 'a' and self.table > 0:
            for attr in attrs:
                if attr[1].endswith('.pdf'):
                    self.data.append('http://www.jpx.co.jp/' + attr[1])
                
    def handle_endtag(self, tag):
        if tag == 'table' and self.table > 0:
            self.table = 0
            
                                               