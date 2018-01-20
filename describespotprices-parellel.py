#! /usr/bin/env python

import argparse
import collections
import sqlite3
import urllib2
from datetime import datetime, timedelta

import json
from pprint import pprint
import pandas as pd

try:
    import boto
    import boto.ec2
    import boto3
    import boto3.ec2
except ImportError:
    raise Exception('Please make boto available first.')



VERSION = 0.1

EC2_INSTANCE_TYPES = [
    "t1.micro",

    "t2.nano",
    "t2.micro",
    "t2.small",
    "t2.medium",
    "t2.large",

    "m1.small",
    "m1.medium",
    "m1.large",
    "m1.xlarge",

    "m2.xlarge",
    "m2.2xlarge",
    "m2.4xlarge",

    "m3.medium",
    "m3.large",
    "m3.xlarge",
    "m3.2xlarge",

    "m4.large",
    "m4.xlarge",
    "m4.2xlarge",
    "m4.4xlarge",
    "m4.10xlarge",

    "c1.medium",
    "c1.xlarge",

    "c3.large",
    "c3.xlarge",
    "c3.2xlarge",
    "c3.4xlarge",
    "c3.8xlarge",

    "c4.large",
    "c4.xlarge",
    "c4.2xlarge",
    "c4.4xlarge",
    "c4.8xlarge"

    "cc2.8xlarge",
    "cg1.4xlarge",
    "cr1.8xlarge",
    "hi1.4xlarge",
    "hs1.8xlarge",

    "g2.2xlarge",
    "g2.8xlarge",

    "r3.large",
    "r3.xlarge",
    "r3.2xlarge",
    "r3.4xlarge",
    "r3.8xlarge",

    "i2.xlarge",
    "i2.2xlarge",
    "i2.4xlarge",
    "i2.8xlarge",

    "i3.large",
    "i3.xlarge",
    "i3.2xlarge",
    "i3.4xlarge",
    "i3.8xlarge",
    "i3.16xlarge",

    "d2.xlarge",
    "d2.2xlarge",
    "d2.4xlarge",
    "d2.8xlarge",

    "x1.32xlarge",
]   


def downloadData(regionsNames, args):
    '''Downloads data from EC2 itself.'''
    from multiprocessing import Pool, cpu_count
    pool = Pool(cpu_count())
    print '\nTotal number of regions: ' + str(len(regionsNames)) 

    for num, name in enumerate(regionsNames):

        ec2 = boto3.client('ec2', region_name=name)

        for zone in ec2.describe_availability_zones( )['AvailabilityZones']:
            for instanceTypeName in EC2_INSTANCE_TYPES:

                f = pool.apply_async(getSpotPricesFromRegion, [args, num, name,zone['ZoneName'],instanceTypeName], callback=cbLogPrices)
                # This ensures that the error is propagated to stdout should the function throw.
                # Note that this fails if the failure happens if num == 0, 1, 2, 3, ... (except the last)
                # For our purposes here, using multiple threads works though.
                if num == len(regionsNames) - 1:  # Synchronously block on return of only the last iteration
                    f.get()
    pool.close()
    pool.join()


def cbLogPrices(pr):
    '''Callback function for apply_async function to append results to a list.'''
    #all_prices.append(pr)


def getSpotPricesFromRegion(args, regionNum, regionName, availability_zone,instanceTypeName):
    '''Gets spot prices of the specified region.'''
    now = datetime.now()
    start = now - timedelta(days=90)  # Use a 6 month range

    print ( regionName)

 

    try:
                ec2 = boto3.client('ec2', region_name=regionName)

            #for zone in ec2.describe_availability_zones( )['AvailabilityZones']:
                   #availZones.append(zone['ZoneName'])

              
              #for instanceTypeName in EC2_INSTANCE_TYPES:
                all_prices = []
                
                print  "Instance type name is: "  +  instanceTypeName
                print "Getting Spot Price details for: " + availability_zone
                paginator = ec2.get_paginator('describe_spot_price_history')

                  
                #ec2.describe_spot_price_history() 
                response_iterator = paginator.paginate(
                  EndTime=now.isoformat(),
                    InstanceTypes=[
                        instanceTypeName ],

                    ProductDescriptions=[
                        args.os,
                        #'Linux/UNIX'
                        ],
                    StartTime=start.isoformat(),
                    AvailabilityZone= availability_zone,
                    
                    #MaxResults=1000,
                    )
                

                for page in response_iterator:
                     print "Adding records :  " + str(len(page['SpotPriceHistory'])) + " for AZ :" + availability_zone + " instanceType :" + instanceTypeName
                     price_dict = page['SpotPriceHistory']
                     all_prices += price_dict
                     print "Total record for now :" + str(len(all_prices)) + " for AZ :" + availability_zone + " instanceType :" + instanceTypeName

                #done with all instance types


                print 'Final records for the AZ for instanceType ' + instanceTypeName + str(len(all_prices)) 
                print "Writing Spot Price History Data for :" + availability_zone  + "instance_type" + instanceTypeName
                df =  pd.DataFrame(all_prices)
      
                filename = availability_zone + '-' + instanceTypeName +   '-jantest.csv'
                df.to_csv(filename)
             
    except Exception as e:
            print ' exception during processing' 
            print(e)
    '''
    try:
        ec2 = boto3.client('ec2', region_name=regionName)

        InstanceTypes=[
                    't1.micro','t2.nano','t2.micro','t2.small','t2.medium','t2.large','t2.xlarge','t2.2xlarge','m1.small','m1.medium','m1.large','m1.xlarge','m3.medium','m3.large','m3.xlarge','m3.2xlarge','m4.large','m4.xlarge','m4.2xlarge','m4.4xlarge','m4.10xlarge','m4.16xlarge','m2.xlarge','m2.2xlarge','m2.4xlarge','cr1.8xlarge','r3.large','r3.xlarge','r3.2xlarge','r3.4xlarge','r3.8xlarge','r4.large','r4.xlarge','r4.2xlarge','r4.4xlarge','r4.8xlarge','r4.16xlarge','x1.16xlarge','x1.32xlarge','x1e.xlarge','x1e.2xlarge','x1e.4xlarge','x1e.8xlarge','x1e.16xlarge','x1e.32xlarge','i2.xlarge','i2.2xlarge','i2.4xlarge','i2.8xlarge','i3.large','i3.xlarge','i3.2xlarge','i3.4xlarge','i3.8xlarge','i3.16xlarge','hi1.4xlarge','hs1.8xlarge','c1.medium','c1.xlarge','c3.large','c3.xlarge','c3.2xlarge','c3.4xlarge','c3.8xlarge','c4.large','c4.xlarge','c4.2xlarge','c4.4xlarge','c4.8xlarge','c5.large','c5.xlarge','c5.2xlarge','c5.4xlarge','c5.9xlarge','c5.18xlarge','cc1.4xlarge','cc2.8xlarge','g2.2xlarge','g2.8xlarge','g3.4xlarge','g3.8xlarge','g3.16xlarge','cg1.4xlarge','p2.xlarge','p2.8xlarge','p2.16xlarge','p3.2xlarge','p3.8xlarge','p3.16xlarge','d2.xlarge','d2.2xlarge','d2.4xlarge','d2.8xlarge','f1.2xlarge','f1.16xlarge','m5.large','m5.xlarge','m5.2xlarge','m5.4xlarge','m5.12xlarge','m5.24xlarge',],

        
    
        for zone in ec2.describe_availability_zones( )['AvailabilityZones']:
               #availZones.append(zone['ZoneName'])

          for instanceTypeName in EC2_INSTANCE_TYPES:
            print  "Instance type name is "  +  instanceTypeName
            print "Getting Spot Price details for: " + zone['ZoneName']
            #ec2.describe_spot_price_history() 
            pr =  ec2.describe_spot_price_history(
                EndTime=now.isoformat(),
                InstanceTypes=[
                    instanceTypeName ],

                ProductDescriptions=[
                    args.os,
                    ],
                StartTime=start.isoformat(),
                AvailabilityZone= zone['ZoneName'],
                #DryRun=False,
                #MaxResults=1000000,
                )

            print "Writing Spot Price History Data for :" + regionName + instanceTypeName
            df = pd.DataFrame(pr["SpotPriceHistory"])
  
            filename = regionName + '-' + instanceTypeName + '.csv'
            df.to_csv(filename)




              #pprint(data)
            
            #print (all_prices)
            #prices = all_prices[1]

           # print pr
            

            #print pr
            #all_prices.append(pr)
                
        pr = ec2.connect_to_region(regionName,
                                    profile_name = args.profile
                                    ).get_spot_price_history(start_time=start.isoformat(),
                                                             end_time=now.isoformat(),
                                                             instance_type=args.instanceType,
                                                             product_description=args.os)
        
        print 'Finished getting the prices from: ' + regionName

        #print all_prices
        return None
    except Exception as e :
        print 'exception while fetching data from ' + regionName + ' Instance type' + args.instanceType 
        print e

        return None
    '''

def isInternetOn():
    '''Tests if internet connection is working, adapted from http://stackoverflow.com/a/3764660'''
    try:
        response = urllib2.urlopen('http://www.amazon.com/', timeout=1)
        return True
    except urllib2.URLError:
        pass
    return False


def parseArgs():
    '''Parse arguments as specified.'''
    osChoices = ['linux', 'suselinux', 'windows']

    desc = 'Uses boto to get spot instance prices and displays zones with the lowest latest price.'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-instance-type', dest='instanceType', default='r3.large',
                        help='Sets the EC2 instance type. Defaults to "%(default)s".')
    parser.add_argument('-os', default='linux', choices=osChoices,
                        help='Sets the operating system. Choose from [' + ','.join(osChoices) +
                                ']. Defaults to "%(default)s".')
    parser.add_argument('-profile', default='default',
                       help='AWS profile name in ".boto". Defaults to "%(default)s".')
    parser.add_argument('-spawn-num', dest='spawnNum', default=1, type=int,
                       help='Sets the hypothetical number of instances to be spawned. ' +
                             'Defaults to "%(default)s".')
    parser.add_argument('-version', action='version', version='%(prog)s {}'.format(VERSION),
                       help=argparse.SUPPRESS)


    args = parser.parse_args()

    if args.os == 'linux':
        args.os = 'Linux/UNIX'
    elif args.os == 'suselinux':
        args.os = 'SUSE Linux'
    elif args.os == 'windows':
        args.os = 'Windows'

    return args



def main(all_prices):
    args = parseArgs()

    
    ec2 = boto3.client('ec2')

    regions = ec2.describe_regions()['Regions']

    regionNames = []

    for region in regions:
        regionNames.append(region['RegionName'])
    #for region in regions
    # print region.name

    allRegionNames = []

    for regionName in allRegionNames:
        allRegionNames.append(region['RegionName'])

    # TO-DO See https://github.com/boto/boto/issues/1951 as to why we reject the following regions.
    #regionNames = [x for x in allRegionNames if x not in ['cn-north-1', 'us-gov-west-1']]

    downloadData(regionNames, args)
    import pandas as pd 
   
all_price=[]
    
if __name__ == '__main__':
    main(all_price)