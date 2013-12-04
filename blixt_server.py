import time
import json
import requests
import logging
from pprint import pprint

import boto
from boto import sns
from boto import dynamodb2
from boto.dynamodb2.table import Table

logger = logging.getLogger('requests')

# Here's where you type your configuration 
ADNAppTokenURL = 'https://account.app.net/oauth/access_token'
clientID = 'ADN_CLIENT_ID_GOES_HERE'
clientSecret = 'ADN_CLIENT_SECRET_GOES_HERE'
grantType = 'client_credentials'
ADN_HOSTNAME = 'https://alpha-api.app.net'
HEADER_JSON = {'Content-Type':'application/json'}
streamKey = 'ADN_STREAM_KEY_GOES_HERE'
AWS_REGION = 'us-west-2' #This is for AWS - if you're not using AWS, you don't need to worry about this.

# FIRST, GET THE APP TOKEN
appTokenRequestPayload = dict(client_id=clientID, client_secret=clientSecret, grant_type=grantType)
tokenResponseObject = requests.post(ADNAppTokenURL,appTokenRequestPayload)
tokenData = json.loads(tokenResponseObject.text)
appAccessToken = tokenData['access_token']

headerString = '?access_token='+appAccessToken

# NEXT, WE NEED TO NUKE ANY EXISTING STREAM ON ADN'S SIDE.
# warning - this is a bug, ideally I'd use a PUT request, but they aren't letting me, apparently.

# Grab the stream's ID
fetchStreamEndpoint = ADN_HOSTNAME + '/stream/0/streams' + headerString + '&key=' + streamKey + '&purge=1'
blixtStreamResponseObject = requests.get(fetchStreamEndpoint)
blixtStreamData = json.loads(blixtStreamResponseObject.text)
# pprint(blixtStreamData)

try:
    streamID = blixtStreamData.get('data')[0].get('id')
    deleteURL = ADN_HOSTNAME+'/stream/0/streams/'+streamID+headerString
    deleteResponse = requests.delete(deleteURL)
    # pprint(json.loads(deleteResponse.text))
except:
    logger.exception('Error deleting existing stream. Guess it was already deleted!')    


# CREATE FILTER CLAUSES
# What kind of filters do we want?
# A: Notify when a Blixt user's post is favorited
# B: Notify when a Blixt user gets a new follower
# C: Notify when a Blixt user's post is reposted
# D: Notify when a Blixt user gets a new @mention
# E: Notify when a Blixt user gets a new message
# ... there are more here: http://developers.app.net/docs/resources/app-stream/#standard-notifications
clauseA = {'object_type':'star', 'operator':'one_of', 'field':'/data/post/user/id', 'value':'$authorized_userids'}
clauseB = {'object_type':'user_follow', 'operator':'one_of', 'field':'/data/follows_user/id', 'value':'$authorized_userids'}
clauseC = {'object_type':'post', 'operator':'one_of', 'field':'/data/repost_of/user/id', 'value':'$authorized_userids'}
clauseD = {'object_type':'post', 'operator':'one_of', 'field':'/data/entities/mentions/*/id', 'value':'$authorized_userids'}
clauseE = {'object_type':'message', 'operator':'one_of', 'field':'/meta/subscribed_user_ids','value':'$authorized_userids'}



# ASSEMBLE THE FILTER FOR THE STREAM
# Creating filters requires a user token, for some odd reason. So, grab an access token from your app developer page, and drop it in here:
bryanAccessToken = 'MANUALLY_CREATE_A_USER_TOKEN_AND_PASTE_HERE'
bryanHeaderString = '?access_token='+bryanAccessToken
createFilterEndpoint = ADN_HOSTNAME + '/stream/0/filters' + bryanHeaderString

clauses = [clauseA, clauseB, clauseD]
# filterObject = {'clauses':clauses, 'name':'only-blixt-users', 'match_policy':'include_any'}
filterObject = {'clauses':clauses, 'match_policy':'include_any'}
filterResponse = requests.post(createFilterEndpoint,json.dumps(filterObject),headers=HEADER_JSON)

# pprint(json.loads(filterResponse.text))
filterID = json.loads(filterResponse.text).get('data').get('id')


# CREATE AN APP STREAM
# Alright, now that we've created the filter object, we're back to using our app token. We'll start by rolling that filter into a new stream object.
createStreamEndpoint = ADN_HOSTNAME + '/stream/0/streams'+headerString
objectTypes = ['post','star','user_follow','message'] # I'm building a "Standard Notification" engine, so these are all I care about.
streamObject = {'endpoint':createStreamEndpoint, 'object_types':objectTypes, 'type':'long_poll', 'key':streamKey, 'filter_id':filterID}
# streamObject = {'endpoint':createStreamEndpoint, 'object_types':objectTypes, 'type':'long_poll', 'key':streamKey} # Use this line of code instead if you want to see all global activity.
responseObject = requests.post(createStreamEndpoint,json.dumps(streamObject),headers=HEADER_JSON)



# LASTLY, GET THE STREAM ENDPOINT. JUST TO PROVE IT EXISTS AND WHATNOT
# Yes, the stream object is 'responseObject' in the above line of code, but once you've gotten all this stream-generation debugged, you don't have to do all of the above code. You can just run this chunk down here:
fetchStreamEndpoint = ADN_HOSTNAME + '/stream/0/streams' + headerString + '&key=' + streamKey + '&purge=1'
# pprint(fetchStreamEndpoint)
blixtStreamResponseObject = requests.get(fetchStreamEndpoint)
blixtStreamData = json.loads(blixtStreamResponseObject.text)



# NOW WE HAVE AN APP STREAM. LET'S MONITOR THAT SUMBITCH

# Fetch the ADN Stream Endpoint
try:
    streamData = blixtStreamData.get('data')
    stream = streamData[0]
    endpoint = stream.get('endpoint')
except:
    logger.exception('Error fetching stream endpoint')

# Fetch our Amazon Web Services SNS and DynamoDB endpoints
dynamo = boto.dynamodb.connect_to_region(AWS_REGION)
users_table = dynamo.get_table('users')
sns = boto.sns.connect_to_region(AWS_REGION)

while True:
    try:
        pprint('Checking Stream')
        r = requests.get(endpoint, stream=True)
        r.raise_for_status()
        


    except KeyboardInterrupt:
        raise
    except:
        logger.exception('Error fetching stream contents:')
    else:
        for line in r.iter_lines(chunk_size=1):
            if not line:
                continue

            blob = json.loads(line)

            if not blob.get('data') or blob.get('data').get('machine_only') or blob.get('meta').get('is_deleted'):
                continue

            try:
                meta = blob.get('meta')
                data = blob.get('data')
                
                metaType = meta.get('type').decode('utf-8')
                
                userIDsToNotify = []
                usernamesToNotify = []
                pushNotificationSummary = 'SUMMARY_TO_DISPLAY_FOR_NOTIFICATION' #This is the lockscreen notification
                
                # STARS
                if metaType == 'star':
                    # So - whose post was starred? and by whom?
                    userStarring = data.get('user')
                    post = data.get('post')
                    userStarred = post.get('user')
                    
                    userStarringName = userStarring.get('username')
                    userStarredName = userStarred.get('username')
                    pprint(userStarringName + ' starred ' + userStarredName + '\'s post')
                    
                    userIDsToNotify.append(userStarred.get('id'))
                    usernamesToNotify.append(userStarred.get('username'))
                    
                    pushNotificationSummary = '@' + userStarringName + ' starred: ' + post.get('text')
                    
                # MENTIONS AND REPOSTS
                elif metaType == 'post':
                    post = data
                    username = post.get('user').get('username')
                    
                    #Was the post created?
                    if (post.get('isDeleted')):
                        pprint('post deleted')
                    else:
                        pprint('post created by ' + username)
                        
                        #Was it a Repost or a Mention?
                        if post.get('repost_of'):
                            #If the post is a repost...
                            
                            userWhosePostWasReposted = post.get('repost_of').get('user')
                            userIDsToNotify.append(userWhosePostWasReposted.get('id'))
                            usernamesToNotify.append(userWhosePostWasReposted.get('username'))
                            
                            usernameWhoReposted = post.get('user').get('username')
                            originalPostText = post.get('repost_of').get('text')
                            
                            pushNotificationSummary = '@' + usernameWhoReposted + ' reposted: ' + originalPostText
                        
                        elif post.get('entities').get('mentions'):
                            # Then it must be a mention
                            
                            # Figure out who to notify: everybody *except* the author (because sometimes, that happens, ya?)
                            postAuthorUsername = post.get('user').get('username')
                            postAuthorID = post.get('user').get('id')
                            mentionEntitiesInPost = post.get('entities').get('mentions')
                            for mentionEntity in mentionEntitiesInPost:
                                if mentionEntity.get('id') != postAuthorID:
                                    usernamesToNotify.append(mentionEntity.get('name'))
                                    userIDsToNotify.append(mentionEntity.get('id'))
                                    
                            # What's the message summary?
                            pushNotificationSummary = '@' + postAuthorUsername + ' mentioned: ' + post.get('text')
                            
                        
                elif metaType == 'message':
                    message = data
                    username = message.get('user').get('username')
                    
                    # Remove the author of the message from the list of folks who'll get notified
                    messageAuthorID = message.get('user').get('id')
                    subscribedUsers = meta.get('subscribed_user_ids')
                    pprint(subscribedUsers)
                    for subscriberID in subscribedUsers:
                        if subscriberID != messageAuthorID:
                            userIDsToNotify.append(subscriberID)
                            usernamesToNotify.append('subscriber\'s name redacted for privacy')
                
                    pushNotificationSummary = '@' + username + ' messaged: ' + message.get('text')
                
                elif metaType == 'user_follow':
                    userFollowed = data.get('follows_user')
                    userFollowing = data.get('user')
                    
                    usernamesToNotify.append(userFollowed.get('username'))
                    userIDsToNotify.append(userFollowed.get('id'))
                    
                    pushNotificationSummary = '@' + userFollowing.get('username') + ' started following you.'
                
                else:
                    pprint('No notification engine built out yet for this type: '+metaType)
                
                
                
                # NOW THE FUN BEGINS
                # determine which users to notify
                # look up device ID by userid etc, format post summary, make your call to amazon here
                
                if len(userIDsToNotify) > 0:
                    
                    i = 0
                    pprint(str(len(userIDsToNotify))+' notifications to send')
                    
                    for userIDToNotify in userIDsToNotify:
                            
                        usernameToNotify = usernamesToNotify[i]
                        
                        # First, let's format the notification summary.
                        out = pushNotificationSummary
                        out = out.replace( u'\u2018', u"'")
                        out = out.replace( u'\u2019', u"'")
                        out = out.replace( u'\u201c', u'"')
                        out = out.replace( u'\u201d', u'"')
                        out.encode('ascii')
                        
                        # For debugging, print the output to your console. 
                        # Yes, this is creepy. No, there's not a great way to debug without doing this. 
                        # On the plus side, now you know how the NSA feels!
                        pprint('Action: ' + metaType)
                        pprint('UserID to notify:' + userIDToNotify)
                        pprint('Username to notify:' + usernameToNotify)
                        pprint('Summary: ' + out)
                        pprint('')
                        
                        # SO: We've got the userID. We now need to tell Amazon SNS to 
                        # notify that user with that message.
                        
                        # NOTE: THIS IS ALL COMMENTED OUT, 'CUZ YOU DON'T NEED IT UNLESS YOU'RE WORKING WITH AMAZON AWS.
                        # 
                        # # Fetch the user/arn objects from dynamoDB. 
                        # # If a user has signed into multiple devices, this will be multiple objects
                        # dynamoUsers = users_table.query(hash_key=userIDToNotify)
                        # 
                        # # Tell SNS to notify this userID with this message
                        # for dynamoUser in dynamoUsers:
                        #     device_arn = dynamoUser['device_arn'].encode('ascii')
                        #     ddbUserID = dynamoUser['adn_user_id'].encode('ascii')
                        #     
                        #     if (ddbUserID == userIDToNotify):
                        #         pprint('Pushing notification to ddbuserID: '+ddbUserID)
                        #         
                        #         apns_dict = {'aps':{'alert':out,'sound':'ctpn_dry.caf'}}
                        #         apns_string = json.dumps(apns_dict,ensure_ascii=False)
                        #         message = {'default':'default message','APNS_SANDBOX':apns_string}
                        #         messageJSON = json.dumps(message,ensure_ascii=False)
                        #         sns.publish(message=messageJSON,target_arn=device_arn,message_structure='json')
                        #     else:
                        #         pprint('UserID from DDB: ' + ddbUserID + ' Does not match userIDToNotify: ' + userIDToNotify)    
                            
                        
                        # Increment the index for the for loop
                        i += 1           
                else:
                    if metaType == 'message':
                        pprint('This channel does not have subscribed_ids. How do I sort these out? This one is from: ' + meta.get('channel_type'))
                    else:
                        pprint('You should write code to process metaType: ' + metaType)
            except:
                logger.exception('Error parsing app stream blob:')
                            
    print 'sleeping before reconnect'
    time.sleep(1)








