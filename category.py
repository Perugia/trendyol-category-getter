
class Category:
    def __init__(self,name = None, tyid = None, tylink = None, parent_category_id = None, last_category = 0):
        if name is None:
            self.name = None
        else:
            self.name = (name.replace(" &amp; "," & ")).replace("&amp;"," & ")

        self.tyid = tyid
        self.parent_category_id = parent_category_id
        self.last_category = last_category
        self.tylink = tylink


    def setLastCategory(self):
        self.last_category = 1

    def setName(self,name):
        self.name = name

    def setParentId(self,id):
        self.parent_category_id = id
