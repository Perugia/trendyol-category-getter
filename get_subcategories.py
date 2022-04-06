import category

def GetSubCategories(parentCat,red,cur,categoryList):
    if parentCat["subCategories"] != None:
        for child in parentCat["subCategories"]:
            exist = red.hsetnx('categories',child["id"],'1')
            if exist == 1:
                if len(child["subCategories"]) == 0:
                    lastCategory = 1
                else:
                    lastCategory = 0

                categoryList.append(category.Category(child["name"],child["id"],child["parentId"],lastCategory))
                cur.execute("INSERT INTO categories(name, tyid, parent_category_id, last_category) VALUES (?, ?, ?, ?)",
                    (child["name"],child["id"],child["parentId"],lastCategory))
                GetSubCategories(child,red,cur,categoryList)

            else:
                print(child["name"], "Already added!")