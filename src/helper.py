import random

def rand(low, high):
    return random.randint(low, high)

def getType():
    type = ["TIN", "NICKEL", "BRASS", "STEEL", "COPPER"]
    return type[rand(0, 4)]

def getRName():
    rName = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]
    return rName[rand(0, 4)]

print(rand(0, 1))
print(getType())
print(getRName())
