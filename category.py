import re

class Category:
    def __init__(self,name, tyid, tylink, parent_category_id = None, last_category = 0):

        self.name = (name.replace(" &amp; "," & ")).replace("&amp;"," & ")
        self.tyid = tyid
        self.parent_category_id = parent_category_id
        self.last_category = last_category
        self.tylink = tylink
    
    def setLastCategory(self):
        self.last_category = 1
