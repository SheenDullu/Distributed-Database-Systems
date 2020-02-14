#!/usr/bin/python2.7
#
# Assignment5 Interface
# Name: Sheen Dullu
#

from pymongo import MongoClient
from math import sqrt, sin, cos, atan2, radians
import os
import sys
import json

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    try:
        businessBasedOnCity = open(saveLocation1, "w")
        cityToSearch = cityToSearch.title()
        for business in collection.find({"city": cityToSearch}):
            name = business['name'].upper()
            fullAddress = str(business['full_address']).upper()
            city = business['city'].upper()
            state = business['state'].upper()
            businessBasedOnCity.write("{}${}${}${}\n".format(name, fullAddress, city, state))
        businessBasedOnCity.close()
    except Exception as e:
        print('Error: {0}'.format(e))
        sys.exit(1)

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    try:
        business_docs = collection.find({'categories': {'$in': categoriesToSearch}})
        lat1 = float(myLocation[0])
        lon1 = float(myLocation[1])
        with open(saveLocation2, "w") as file:
            for business in business_docs:
                name = business['name']
                lat2 = float(business['latitude'])
                lon2 = float(business['longitude'])
                d = DistanceFunction(lat2, lon2, lat1, lon1)
                if d <= maxDistance:
                    file.write(name.upper() + "\n")
        file.close()
    except Exception as e:
        print('Error: {0}'.format(e))
        sys.exit(1)


def DistanceFunction(lat2, lon2, lat1, lon1):
    R = 3959
    phi1 = radians(lat1)
    phi2 = radians(lat2)
    delta_phi = radians(lat2 - lat1)
    delta_lambda = radians(lon2 - lon1)
    a = (sin(delta_phi/2) * sin(delta_phi/2)) + (cos(phi1) * cos(phi2) * sin(delta_lambda/2) * sin(delta_lambda/2))
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    d = R*c
    return d



