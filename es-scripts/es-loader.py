import os
import sys
import urllib2
import json as jsonlib
import datetime

def check(r, payload):
    if r.getcode() != 200:
        print "ERROR"
        print payload

endpoint = 'http://{0}:9200/_bulk'.format(sys.argv[2])
BATCH = 100000

"""
record = ('{ID:{0},CreatedAt:{1},Text:{2},UserName:{3},UserID:{4},UserDescription:{5},UserCreatedAt:{6},'
          'FavoriteCount:{7},GeoPoint:{8},InReplyToStatusId:{9},InReplyToUserId:{10},'
          'RetweetedID:{11},RetweetedUserID:{12},Source:{13},UserFavoritesCount:{14},UserFollowersCount:{15},'
          'UserFriendsCount:{16},UserLocation:{17},UserScreenName:{18},UserStatusesCount:{19},'
          'UserTimeZone:{20},UserUrl:{21},UserUtcOffset:{22},User.ProfileImageUrl:{23}}'
         )
"""

fields = ("ID", "CreatedAt", "Text", "UserName", "UserID", "UserDescription", "UserCreatedAt",
          "FavoriteCount", "GeoPoint", "InReplyToStatusId", "InReplyToUserId",
          "RetweetedID", "RetweetedUserID", "Source", "UserFavoritesCount", "UserFollowersCount",
          "UserFriendsCount", "UserLocation", "UserScreenName", "UserStatusesCount", "UserTimeZone", "UserUrl",
          "UserUtcOffset", "UserProfileImageUrl")
userDefined = ("Text", "UserName", "UserDescription", "UserLocation", "UserScreenName")

path = sys.argv[1]

for p in os.listdir(path):
    if not p.endswith(".csv"):
        continue
    print "{0} {1}".format(p, datetime.datetime.now())
    f = open(os.path.join(path, p), 'r')
    count = 0
    payload = []
    for line in f:
        split = line.strip().split('\t')
        if len(split) != 24:
            continue
        split[8] = split[8].replace(' ', ',')
        json = []
        for i, v in enumerate(split):
            if (v == "null" and fields[i] not in userDefined) or v == "":
                json.append(fields[i] + ':null')
            else:
                json.append(fields[i] + ':' + jsonlib.dumps(v))
        payload.append('{ "create": { "_index": "twitter", "_type": "tweet"}}')
        payload.append('{' + ','.join(json) + '}')
        count += 1
        if count == BATCH:
            check(urllib2.urlopen(endpoint, data='\n'.join(payload)), payload)
            count = 0
            payload = []
    if count > 0:
        check(urllib2.urlopen(endpoint, data='\n'.join(payload)), payload)



