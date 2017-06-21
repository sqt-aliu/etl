# -*- coding: utf-8 -*-
from html.parser import HTMLParser

# create a subclass and override the handler methods
class ASXHistorical(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.data = []

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            for attr in attrs:
                if attr[1].endswith('.zip'):
                    self.data.append(attr[1])
                

            
                                               