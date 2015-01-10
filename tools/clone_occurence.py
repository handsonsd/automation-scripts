import sys
import os
import argparse
import yaml
import json
from pytz import timezone, utc
from datetime import datetime
from dateutil import parser, relativedelta
from simple_salesforce import Salesforce, SalesforceLogin, SFType


# define arguments
# will read default from ~/.hosd.yml
argp = argparse.ArgumentParser(description='Clone Occurrence.')
argp.add_argument('config', nargs='+', help='yaml file that contain occurrences config')
argp.add_argument('--username', help='username USERNAME')
argp.add_argument('--password', help='password PASSWORD')
argp.add_argument('--token', help='token SECURITY_TOKEN')
argp.add_argument('--occurrence', help='occurrence OCCURRENCE_NAME')
argp.add_argument('--date', help='date OCCURRENCE_DATE(yyyy-mm-dd)')
argp.add_argument('--timezone', help='timezone TIMEZONE(eg.US/Pacific)')
argp.add_argument('--dry', help='dry run', action='store_true')
argp.add_argument('--debug', help='debug', action='store_true')


#sf = Salesforce(username='myemail@example.com', password='password', security_token='token')

def clone_occurrence(sf, oc_name, new_date, tz, dry_run=False, debug=False):
    # query for occurrence_name
    qres = sf.query("SELECT Id FROM HOC__Occurrence__c where Name = '%s'" % (oc_name))
    if ('records' not in qres) or (len(qres['records']) < 1) or ('Id' not in qres['records'][0]):
        print "Occurence %s not found !" % (oc_name)
        return -1

    # create Occurrence data type
    Occurrence = SFType('HOC__Occurrence__c', sf.session_id, sf.sf_instance)
    oc = Occurrence.get(qres['records'][0]['Id'])
    if not oc:
        print "Failed to retrieve Occurrence %s" % (qres['records'][0]['Id'])
        return

    # get Opportunity
    VolunteerOpportunity = SFType('HOC__Volunteer_Opportunity__c', sf.session_id, sf.sf_instance)
    op = VolunteerOpportunity.get(oc['HOC__Volunteer_Opportunity__c'])
    if not op:
        print "Failed to retrieve Volunteer Opportunity%s" % (oc['HOC__Volunteer_Opportunity__c'])
        return

    # do date calculation
    old_start_datetime = parser.parse(oc['HOC__Start_Date_Time__c'])
    old_end_datetime = parser.parse(oc['HOC__End_Date_Time__c'])
    # need to make sure we calculate delta date in the right timezone,
    # otherwise it can mess up the calculation
    delta = new_date - old_start_datetime.astimezone(tz).date()

    # this weird formula is to add delta while maintaining the correct timezone
    # first add delta, then remove timezone (to maintain the same hour)
    # then add back the timezone, so we can calculate utc timezone correctly afterward
    new_start_datetime = tz.localize((old_start_datetime.astimezone(tz) + delta).replace(tzinfo=None)).astimezone(utc)
    new_start_datetime_str = new_start_datetime.isoformat()
    new_end_datetime = tz.localize((old_end_datetime.astimezone(tz) + delta).replace(tzinfo=None)).astimezone(utc)
    new_end_datetime_str = new_end_datetime.isoformat()
    new_start_tz = new_start_datetime.astimezone(tz)

    print "========================================="
    print "Occurrence Id: " + oc_name
    print "Project Name: " + op['Name']
    print "Volunteer Coordinator Name: " + oc['HOC__Volunteer_Coordinator_Name__c']
    print "Volunteer Coordinator Email: " + oc['HOC__Volunteer_Coordinator_Email__c']
    print "Days Time Needed: " + oc['HOC__Days_Times_Needed__c']
    print "Clone to date (UTC): " + str(new_start_datetime)

    if debug:
        print "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
        print "Original Data"
        print "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
        print json.dumps(oc, sort_keys=True, indent=4, separators=(',',':'))
        print "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"


    # process properties
    new_oc = {}

    # we will go through the oc, and calling it one by one, for every key in the oc we will
    # check the modifier.
    # If the value exists:
    #   accept lambda to do some processing to it, will be passed the oc and new_oc as parameter
    #      lambda (key, old_oc, new_oc)
    #   otherwise if it is None, it will be removed
    # if the value doesn't exist, it will be copied as is
    oc_modifier = {
        u'attributes': None,
        u'Id': None,
        # u'OwnerId': None,
        u'IsDeleted': None,
        u'Name': None,
        u'CreatedDate': None,
        u'CreatedById': None,
        u'LastModifiedDate': None,
        u'LastModifiedById': None,
        u'SystemModstamp': None,
        u'LastActivityDate': None,
        u'ConnectionReceivedId': None,
        u'ConnectionSentId': None,
        u'HOC__City__c': None,
        u'HOC__Country__c': None,
        #u'HOC__Days_Times_Needed__c': None,
        u'HOC__End_Date_Time__c': lambda k,ooc,noc:new_end_datetime_str,
        u'HOC__Google_Map_URL__c': None,
        u'HOC__HOC_Domain_Name__c': None,
        u'HOC__HOC_ID__c': None,
        u'HOC__Import_ID__c': None,
        #u'HOC__Location__c': None,
        u'HOC__Managing_Organization_Name__c': None,
        #u'HOC__Maximum_Attendance__c': None,
        #u'HOC__Minimum_Attendance__c': None,
        u'HOC__Occurrence_URL__c': None,
        #u'HOC__Opportunity_Approval_Manager_Email__c': None,
        #u'HOC__Partner_Staff_Email__c': None,
        u'HOC__Posting_Status__c': None,
        #u'HOC__Recurrence__c': None,
        u'HOC__Registration_Deadline__c': None,
        u'HOC__Registration_Start_Date__c': None,
        u'HOC__Schedule_Type__c': None,
        u'HOC__Serial_Number__c': None,
        u'HOC__Start_Date_Time__c': lambda k,ooc,noc:new_start_datetime_str,
        u'HOC__State_Province__c': None,
        #u'HOC__Status__c': None,
        u'HOC__Street__c': None,
        u'HOC__Total_Attended__c': None,
        u'HOC__Total_Hours_Served__c': None,
        #u'HOC__Volunteer_Coordinator_Email__c': None,
        #u'HOC__Volunteer_Coordinator_Name__c': None,
        #u'HOC__Volunteer_Leader_Needed__c': None,
        u'HOC__Volunteer_Opportunity_Type__c': None,
        #u'HOC__Volunteer_Opportunity__c': None,
        u'HOC__Volunteers_Still_Needed__c': None,
        u'HOC__Zip_Postal_Code__c': None,
        u'HOC__Guest_Volunteer_Hours_Served__c': None,
        u'HOC__Guest_Volunteers_Attended__c': None,
        u'HOC__Total_Confirmed__c': None,
        u'HOC__Total_Connections__c': None,
        u'HOC__Total_Declined__c': None,
        u'HOC__Total_Not_Attended__c': None,
        u'HOC__Total_Pending__c': None,
        u'HOC__Total_Unreported__c': None,
        u'HOC__Volunteer_Hours_Served__c': None,
        u'HOC__Volunteers_Attended__c': None,
        u'HOC__Guest_Volunteer_Number_Hours_Served__c': None,
        #u'HOC__Opportunity_Coordinator__c': None,
        u'HOC__Total_Number_Hours_Served__c': None,
        u'HOC__Update_Connections_Status__c': None,
        u'HOC__Volunteer_Number_Hours_Served__c': None,
        u'HOC__CreationSource__c': None,
        u'HOC__Number_of_Occurrences__c': None,
        u'HOC__HOC_Backend_Domain_Name__c': None,
        u'HOC__LastModifiedByV2__c': None,
        u'HOC__OwnerIdV2__c': None,
        u'HOC__Grouped_Occurrences__c': None,
        #u'HOC__Include_Pending_for_Max_Attendance__c': None,
        u'HOC__Locations_Details_Page__c': None,
        #u'HOC__Maximum_Waitlist__c': None,
        #u'HOC__Turn_off_teams__c': None,
        #u'HOC__Turn_off_waitlist__c'

        # IMPACT
        "Additional_Impact__c": None,
        "Animals_Served_Cared_For__c": None,
        "ConnectionReceivedId": None,
        "ConnectionSentId": None,
        "Craft_Items_Created_Constructed__c": None,
        "Facilities_Maintained_Revitalized__c": None,
        "For_Follow_Up__c": None,
        "Gardens_Maintained_Created__c": None,
        "Individuals_Received_Donations__c": None,
        "Individuals_Served_Engaged__c": None,
        "Mi_Trail_Beach_Park_Maintained_Created__c": None,
        "Potential_Volunteer_Leaders__c": None,
        "Pounds_of_Trash_Debris_Collected__c": None,
        "Share_a_Story__c": None,
    }

    for k in oc.keys():
        if k in oc_modifier:
            if oc_modifier[k] is None:
                # skip the data
                pass
            else:
                # assume this is lambda
                new_oc[k] = oc_modifier[k](k, oc, new_oc)
        else:
            new_oc[k] = oc[k]

    if debug:
        print "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
        print "Modified Data"
        print "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
        print json.dumps(new_oc, sort_keys=True, indent=4, separators=(',',':'))
        print "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

    # double check the time, there should be no two occurence within the same date - to make this Idempotent
    check = sf.query("SELECT Id FROM HOC__Occurrence__c where HOC__Volunteer_Opportunity__c = '%s' and HOC__Start_Date_Time__c = %s" % (
        oc['HOC__Volunteer_Opportunity__c'], new_start_datetime_str))
    if check['totalSize'] > 0:
        print "Skipping - duplicate record found for %s, "%(new_start_tz.strftime('%A')) + str(new_start_tz)
        print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
        return
    else:
        print "Clone to date: %s, "%(new_start_tz.strftime('%A')) + str(new_start_tz)
        print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"

    if dry_run:
        print("DRY RUN ..")
        print "========================================="
    else:
        print("CREATING OCCURRENCE ..")
        result = Occurrence.create(new_oc)
        print result
        print "========================================="

    return 0


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    config = {}


    args = argp.parse_args(argv)

    try:
        for cfg in args.config:
            with open(cfg) as yml:
                config.update(yaml.load(yml))
    except IOError as e:
        pass
    config['schedule']=config.get('schedule',[]) # ensure schedule exists

    username = args.username or config.get('username', 'UNKNOWN')
    password = args.password or config.get('password', 'UNKNOWN')
    token = args.token or config.get('token', 'UNKNOWN')
    mytz = timezone(args.timezone or config.get('timezone', 'US/Pacific'))
    dry_run = args.dry
    debug = args.debug

    if args.occurrence is not None and args.date is not None:
        config['schedule'].append({
            'occurence':args.occurence,
            'date':datetime.strptime(args.date, '%Y-%m-%d')
            })

    if len(config['schedule'])==0:
        print 'No occurence scheduled ..'
        return

    try:
        print 'Logging in as %s'%(username)
        session_id, instance = SalesforceLogin(username=username, password=password, security_token=token)
    except Exception, e:
        print 'Failed to login : %s' % (str(e))
        return 1

    sf = Salesforce(instance=instance, session_id=session_id)

    for sched in config['schedule']:
        new_date = datetime.strptime(str(sched['date']), '%Y-%m-%d').date()
        clone_occurrence(sf, sched['occurence'], new_date, mytz, dry_run, debug)

    return 0

if __name__ == "__main__":
    sys.exit(main())

