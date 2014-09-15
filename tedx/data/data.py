import random
import pymongo

person_count = 1

def get_random_person():
    global person_count 
    person_count = person_count+1
    return "tedx_%s_attendee@gmail.com"%(str(person_count))

def get_random_profession(input_csv = 'professions.csv'):
    random_profession = random.randrange(1,20, 2)
    line_number = 1

    with open(input_csv) as fp:
        for line in fp:
            if line_number == random_profession:
                return line.strip()
            line_number = line_number + 1

def generate_sample( type = 'i_am' , output_csv ='data.csv' , input_csv='professions.csv' ):
    DATA_SIZE = 50
    output = open(output_csv , "w+")
    for person in range(0,DATA_SIZE):
        person_details = get_random_person()
        profession_details = person_details 
        my_professions = set()
        while len(my_professions)!=10:
            my_professions.add(get_random_profession(input_csv))
        
        for profession in my_professions:
            profession_details = profession_details  + "," + get_random_profession(input_csv)

        output.write(profession_details+"\n")

    output.flush()
    output.close()

DB_CONNECTION_STRING = "mongodb://sbose:teamtedxblr@kahana.mongohq.com:10036/tedx"

from pymongo import MongoClient
connection = None
def getConnection():
    global connection

    if not connection or connection.alive() == False:
        connection = MongoClient(DB_CONNECTION_STRING)
    db = connection.tedx
    return db

def getCollection(name):
    db =  getConnection()
    return db[name]    
     
def insert_raw_data_into_db( input_csv = "data.csv" ):
    collection = getCollection('raw')
    with open(input_csv) as fp:
       for line in fp:
           name = line.split(",")[0]
           am = line.split(",")[1:5]
           want = line.split(",")[6:10]

           
           raw_attendee = { "name" : name , "am" : am , "want" : want }
           print str(raw_attendee)
           collection.insert(raw_attendee)

       	


def insert_wufoo_data_into_db( input_scv = "wufoo.csv" ):

    collection = getCollection('wufoo')
    count = 0
    with open(input_scv) as fp:
        for line in fp:

            count = count + 1
            print "\n\n Line number : %s"%(count)

            attendee_data_raw = line.split(",")
            attendee_data  = []

            for key in attendee_data_raw:
                value = key.translate(None,"\"")
                attendee_data.append(value)




            '''
            "Entry Id","My Name","Facebook profile link","My Age","My Display Picture","My Email Address","My Short Biography",(8)"I am ","I am ","I am ","I am ","I am ","I am ","I am ","I am ",(16)"I need ","I need ","I need ","I need ","I need ","I need ","I need ",(23)"I need ","Untitled","Date Created","Created By","Last Updated","Updated By"
            '''

            attendee = {
                "email": attendee_data[5].strip(),
                "name": attendee_data[1].strip() ,
                "facebook": attendee_data[2].strip(),
                "bio":attendee_data[6].strip(),
                "age": attendee_data[3].strip() ,
                "dp": attendee_data[4].strip(),
                "am" : "",
                "need":"",
            }

            print str(attendee)
            #collection.insert(raw_attendee)


def encodeUserData(user, password):
        return "Basic " + (user + ":" + password).encode("base64").rstrip()


def talk_to_wufoo_api():
    import requests

    r = requests.get("https://tedxbangalore.wufoo.com/api/v3/forms/ztlv7yx1xwmbo4/entries.xml?pageStart=100&pageSize=120",auth=("CVG3-GAZF-A6LW-NQWU","nothing"))
    f = open("wufoo_api_output.xml","w+")
    full_text = r.text

    for c in full_text:
        f.write(c.encode('utf-8','ignore'))
        f.flush()

    f.close()


import xml.dom.minidom

EMAIL_ADDRESS = "Field211"
DP = "Field210"
NAME = "Field215"
FACEBOOK = "Field218"
AGE = "Field216"
BIOGRAPHY = "Field212"

I_AM = [ "Field2","Field3","Field4","Field5","Field6","Field7","Field8","Field9" ]
I_NEED = [ "Field102","Field103","Field104","Field105","Field106","Field107","Field108","Field109" ]



def parse_wufoo_xml(xml_file = "wufoo_api_output.xml"):
    DOMTree = xml.dom.minidom.parse(xml_file)
    tree = DOMTree.documentElement

    info = tree.getElementsByTagName("Entry")

    db_table = getCollection("wufoo_data")
    for attendee_info in info:
        email_address = attendee_info.getElementsByTagName(EMAIL_ADDRESS)[0].childNodes[0].data
        dp = attendee_info.getElementsByTagName(DP)[0].childNodes[0].data
        biography = attendee_info.getElementsByTagName(BIOGRAPHY)[0].childNodes[0].data

        name = ""
        facebook = ""
        age = ""
        try:
            name = attendee_info.getElementsByTagName(NAME)[0].childNodes[0].data
        except:
            pass

        try:
            facebook = attendee_info.getElementsByTagName(FACEBOOK)[0].childNodes[0].data
        except:
            pass

        try:
            age = attendee_info.getElementsByTagName(AGE)[0].childNodes[0].data
        except:
            pass

        i_am = []
        i_need = []

        for characteristic in I_AM :
            try:
                c = attendee_info.getElementsByTagName(characteristic)[0].childNodes[0].data
                if c and len(c)>0:
                    i_am.append(c)
            except:
                pass

        for characteristic in I_NEED:
            try:
                c = attendee_info.getElementsByTagName(characteristic)[0].childNodes[0].data
                if c and len(c)>0:
                    i_need.append(c)
            except:
                pass

        attendee = {
                "email": email_address,
                "name": name ,
                "facebook": facebook,
                "bio": biography,
                "age": age ,
                "dp": dp,
                "am" : i_am,
                "need":i_need,
                "matches" : [],
        }
        #print str(attendee) + "\n"

        a = db_table.find_one({"email":email_address})

        if not a:
            db_table.save(attendee)
        else:

            for key in attendee.keys():
                a[key] = attendee[key]

            db_table.save(a)
            #print str(a)



def update_i_am_collection():
    wuffoo_data_collection = getCollection("wufoo_data")
    i_am_collection = getCollection("i_am")

    all_attendee_data = wuffoo_data_collection.find()
    for attendee in all_attendee_data:
        email = attendee["email"]
        i_am = attendee["am"]

        for characteristic in i_am:
            row = i_am_collection.find_one({"characteristic":characteristic})

            attendees = []
            if row:
                attendees = list(row['attendees'])

            else:
                row = {
                    "characteristic":characteristic,
                    "attendees":attendees,
                }

            attendees.append(email)
            row["attendees"] = list(set(attendees))
            i_am_collection.save(row)


'''
Mappings:
Support ---> Kind,
'''




def has_bandwith_to_meet(attendee_email,max_bandwidth):

    # Get attendee object
    attendee = get_attendee_object_from_email(attendee_email)
    #print("%s is being tested for bandwidth"%(str(attendee)))

    matched = None
    try:
        matched = attendee['matches']
        if len(matched) < max_bandwidth:
                return True
        else:
            return False

        return  True
    except Exception as e:
        print "Error : %s  ; Email:  %s"%(str(e),attendee_email)
    return False

def get_age(attendee_email):
    print("testing age for %s"%(attendee_email))
    # Get attendee object
    attendee = get_attendee_object_from_email(attendee_email)

    age = None
    try:
        age = int(attendee['age'])
    except:
        age = 100 # Age not mentioned

    print age
    return age


COMMON_TITLES = { "Entrepreneur" , "Designer" , "Investor" }

def get_attendee_object_from_email(attendee_email,remove=False):

    print "getting attendee object for %s"%(attendee_email)
    attendee = None
    for i in range(0,len(base_attendee_data)-1):
        a = base_attendee_data[i]
        #print "comparing %s and %s "%(a['email'],attendee_email)
        if str(a['email']) == attendee_email:

            attendee = a
            if remove == True:
                base_attendee_data.remove(a)
            break
    print "Object : %s"%(str(attendee))
    return attendee

base_attendee_data = None
base_attendee_collection = None
i_am_collection= None
i_am_data = None
def match_people_with_similar_title():

    global  i_am_collection , i_am_data , base_attendee_data, base_attendee_collection


    if not i_am_collection:
        i_am_collection = getCollection("i_am")
        print "fetched i_am"
        #i_am_data = i_am_collection.find_all()


    if not base_attendee_data:
        base_attendee_collection = getCollection("wufoo_data")
        base_attendee_data = list(base_attendee_collection.find())
        print "fetched base attendee data"

    max_bandwidth = 4
    for title in COMMON_TITLES:
        print "Working with %s "%title

        # Get all people with the specific occupation
        occupation = i_am_collection.find_one({"characteristic":title})

        # Get another person of similar profession
        similar_attendee = occupation["attendees"]

        for i in range(0,len(similar_attendee)-1):
            for j in range(0,len(similar_attendee)-1):
                print("%s and %s "%(str(i),str(j)))
                if i != j and has_bandwith_to_meet(similar_attendee[i],max_bandwidth) and has_bandwith_to_meet(similar_attendee[j],max_bandwidth):

                    print "###################################################Matched"

                    attendee_1 = get_attendee_object_from_email(similar_attendee[i],remove=True)
                    attendee_2 = get_attendee_object_from_email(similar_attendee[j],remove=True)
                    print("%s  and   %s"%(str(attendee_1['email']), str(attendee_2['email'])))

                    # remove them
                    #base_attendee_data.remove(attendee_1)
                    #base_attendee_data.remove(attendee_2)

                    matched_list  = attendee_1['matches']
                    matched_list.append(similar_attendee[j])
                    attendee_1['matches'] = list(set(matched_list))
                    print("Matches of %s : %s"%(similar_attendee[i],attendee_1['matches']))

                    matched_list  = attendee_2['matches']
                    matched_list.append(similar_attendee[i])
                    attendee_2['matches'] = list(set(matched_list))
                    print("Matches of %s : %s"%(similar_attendee[j],attendee_2['matches']))

                    # Add them back
                    base_attendee_data.append(attendee_1)
                    base_attendee_data.append(attendee_2)

                    print "%s and %s have been matched "%(attendee_1,attendee_2)

                    base_attendee_collection.save(attendee_1)
                    base_attendee_collection.save(attendee_2)
                    # Add them as a match

        max_bandwidth = max_bandwidth + 1
        # match {1,2,3,4,5} 0 vs 4  , 0 v/s n-1


def update_match(attendee1,attendee2):
    pass





if __name__ == "__main__":


    #talk_to_wufoo_api()


    #parse_wufoo_xml()


    #update_i_am_collection()

    match_people_with_similar_title()


