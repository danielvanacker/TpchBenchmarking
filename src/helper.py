import random
from datetime import date, timedelta
from dateutil.relativedelta import *

def getRegions():
    return ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]

def getType1():
    type = ["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"]
    return type[rand(0, 5)]

def getType2():
    type = ["ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"]
    return type[rand(0, 4)]

def getType3():
    type = ["TIN", "NICKEL", "BRASS", "STEEL", "COPPER"]
    return type[rand(0, 4)]

def getTypeString():
    return getType1() + " " + getType2() + " " + getType3() 

def getNationRegions():
    return [("ALGERIA", 0), ("ARGENTINA", 1), ("BRAZIL", 1), ("CANADA", 1), ("EGYPT", 4), ("ETHIOPIA", 0), ( "FRANCE", 3), ("GERMANY", 3), ("INDIA", 2), ("INDONESIA", 2), ("IRAN", 4), ("IRAQ", 4), ("JAPAN", 2), ("JORDAN", 4), ("KENYA", 0), ("MOROCCO", 0), ("MOZAMBIQUE", 0), ("PERU", 1), ("CHINA", 2), ("ROMANIA", 3), ("SAUDI ARABIA", 4), ("VIETNAM", 2), ("RUSSIA", 3), ("UNITED KINGDOM", 3), ("UNITED STATES", 1)]

def getColor():
    color = ["almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue", "blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral", "cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger", "drab", "firebrick", "floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew", "hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon", "light", "lime", "linen", "magenta", "maroon", "medium", "metallic", "midnight", "mint", "misty", "moccasin", "navajo", "navy", "olive", "orange", "orchid", "pale", "papaya", "peach", "peru", "pink", "plum", "powder", "puff", "purple", "red", "rose", "rosy", "royal", "saddle", "salmon", "sandy", "seashell", "sienna", "sky", "slate", "smoke", "snow", "spring", "steel", "tan", "thistle", "tomato", "turquoise", "violet", "wheat", "white", "yellow"]
    return color[rand(0, 91)]

def rand(low, high):
    return random.randint(low, high)

def getRName():
    rName = getRegions()
    return rName[rand(0, 4)]

def getSegment():
    segment = ["AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"]
    return segment[rand(0, 4)]

def getRandDate(low, high):
    diff = (high - low).days
    addDays = rand(0, diff)
    return str(low + timedelta(days=addDays))

# Gets a randome first day of the month between [start, end]
def getRandMonth(start, end):
    numMonths = (end.year - start.year) * 12 + (end.month - start.month)
    addMonths = rand(0, numMonths)
    return start + relativedelta(months=+addMonths)

def monthsToDays(startDate, months):
    newDate = startDate + relativedelta(months=+months)
    return (newDate - startDate).days

def yearsToDays(startDate, years):
    newDate = startDate + relativedelta(years=+years)
    return (newDate - startDate).days

def getNNames():
    nations = getNationRegions()
    index = rand(0, 24)
    (nation1, r1) = nations[index]
    del nations[index]
    (nation2, r2) = nations[rand(0, 23)]
    return (nation1, nation2)

def getNationAndRegion():
    nations = getNationRegions()
    regions = getRegions()
    (nation, regionInd) = nations[rand(0, 24)]
    region = regions[regionInd]
    return (nation, region)

def getModes():
    modes = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"]
    index = rand(0, 6)
    mode1 = modes[index]
    del modes[index]
    mode2 = modes[rand(0, 5)]
    return (mode1, mode2)


print(rand(0, 1))
print(getType3())
print(getRName())
print(getSegment())
print(getRandDate(date(1995, 3, 1), date(1995, 3, 31)))
print(getRandMonth(date(1995, 3, 1), date(1996, 3, 31)))
print(monthsToDays(date(2019, 5, 7), 12))
print(yearsToDays(date(20, 5, 7), 1))
print(getNNames())
print(getNationAndRegion())
print(getTypeString())
print(getColor())
print(getModes())
