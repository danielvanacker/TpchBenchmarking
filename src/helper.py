import random
from datetime import date, timedelta

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


print(rand(0, 1))
print(getType())
print(getRName())
print(getSegment())
print(getRandDate(date(1995, 3, 1), date(1995, 3, 31)))
