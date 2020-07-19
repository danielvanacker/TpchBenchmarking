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

def getTypeString2():
    return getType1() + " " + getType2()

def getTableColumns(table):
    tables = {"region":["r_regionkey", "r_name", "r_comment"], "nation":["n_nationkey", "n_name", "n_regionkey", "n_comment"], "part":["p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container", "p_retailprice", "p_comment"], "supplier":["s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"], "partsupp":["ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment"], "customer":["c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"], "orders":["o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"], "lineitem":["l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"]}
    return tables[table]

# Returns list of (N_NationKey, N_Name, N_RegionKey)
def getNationRegions():
    return [(0, "ALGERIA", 0), (1, "ARGENTINA", 1), (2, "BRAZIL", 1), (3, "CANADA", 1), (4, "EGYPT", 4), (5, "ETHIOPIA", 0), (6, "FRANCE", 3), (7, "GERMANY", 3), (8, "INDIA", 2), (9, "INDONESIA", 2), (10, "IRAN", 4), (11, "IRAQ", 4), (12, "JAPAN", 2), (13, "JORDAN", 4), (14, "KENYA", 0), (15, "MOROCCO", 0), (16, "MOZAMBIQUE", 0), (17, "PERU", 1), (18, "CHINA", 2), (19, "ROMANIA", 3), (20, "SAUDI ARABIA", 4), (21, "VIETNAM", 2), (22, "RUSSIA", 3), (23, "UNITED KINGDOM", 3), (24, "UNITED STATES", 1)]

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
    (tmp, nation1, r1) = nations[index]
    del nations[index]
    (tmp, nation2, r2) = nations[rand(0, 23)]
    return (nation1, nation2)

def getNationAndRegion():
    nations = getNationRegions()
    regions = getRegions()
    (tmp, nation, regionInd) = nations[rand(0, 24)]
    region = regions[regionInd]
    return (nation, region)

def getModes():
    modes = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"]
    index = rand(0, 6)
    mode1 = modes[index]
    del modes[index]
    mode2 = modes[rand(0, 5)]
    return (mode1, mode2)

def getWords():
    words1 = ["special", "pending", "unusual", "express"]
    words2 = ["packages", "requests", "accounts", "deposits"]
    return (words1[rand(0, 3)], words2[rand(0, 3)])

def getBrand():
    num1 = str(rand(1, 5))
    num2 = str(rand(1, 5))
    return f"Brand#{num1}{num2}"

def getContainer():
    syllable1 = ["SM", "MED", "JUMBO", "WRAP"]
    syllable2 = ["CASE", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"]
    return f"{syllable1[rand(0, 3)]} {syllable2[rand(0, 6)]}"

def getCountryCodes():
    nations = getNationRegions()
    codes = []
    for i in range(0, 7):
        ind = rand(0, len(nations) - 1)
        (num, tmp1, tmp2) = nations[ind]
        del nations[ind]
        codes.append(str(num+10))

    return codes
