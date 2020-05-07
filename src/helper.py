import random
from datetime import date, timedelta
from dateutil.relativedelta import *

def rand(low, high):
    return random.randint(low, high)

def getType():
    type = ["TIN", "NICKEL", "BRASS", "STEEL", "COPPER"]
    return type[rand(0, 4)]

def getRName():
    rName = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]
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

print(rand(0, 1))
print(getType())
print(getRName())
print(getSegment())
print(getRandDate(date(1995, 3, 1), date(1995, 3, 31)))
print(getRandMonth(date(1995, 3, 1), date(1996, 3, 31)))
print(monthsToDays(date(2020, 5, 7), 12))
