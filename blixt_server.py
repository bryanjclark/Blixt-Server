import time
import json
import requests
import logging
import traceback
import  exceptions
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
AWS_REGION = 'us-west-2' #This is for AWS - if you're not using AWS, you don't need to worry about this.
useDebug = True
# useDebug = False

if (useDebug):
	streamKey = 'ADN_STREAM_KEY_GOES_HERE_FOR_DEBUGGING'
else:
	streamKey = 'ADN_STREAM_KEY_GOES_HERE'

pprint(streamKey)

# FIRST, GET THE APP TOKEN
appTokenRequestPayload = dict(client_id=clientID, client_secret=clientSecret, grant_type=grantType)
tokenResponseObject = requests.post(ADNAppTokenURL,appTokenRequestPayload)
tokenData = json.loads(tokenResponseObject.text)
appAccessToken = tokenData['access_token']

headerString = '?access_token='+appAccessToken
pprint(appAccessToken)
'''
# NEXT, WE NEED TO NUKE ANY EXISTING STREAM ON ADN'S SIDE.
# warning - this is a bug, ideally I'd use a PUT request, but they aren't letting me, apparently.

# Grab the stream's ID
fetchStreamEndpoint = ADN_HOSTNAME + '/stream/0/streams' + headerString + '&key=' + streamKey + '&purge=1'
blixtStreamResponseObject = requests.get(fetchStreamEndpoint)
blixtStreamData = json.loads(blixtStreamResponseObject.text)
pprint(blixtStreamData)

try:
	streamID = blixtStreamData.get('data')[0].get('id')
	deleteURL = ADN_HOSTNAME+'/stream/0/streams/'+streamID+headerString
	deleteResponse = requests.delete(deleteURL)
	pprint(json.loads(deleteResponse.text))
except:
	logger.exception('Error deleting existing stream. Guess it was already deleted!')    


# CREATE FILTER CLAUSES
# What kind of filters do we want?
# A: Notify when a Blixt user's post is favorited
# B: Notify when a Blixt user gets a new follower
# C: Notify when a Blixt user's post is reposted
# D: Notify when a Blixt user gets a new @mention
# E: Notify when a Blixt user gets a new message (NOTE: Commenting this one out, because Blixt doesn't have messages in 1.0)
# ... there are more here: http://developers.app.net/docs/resources/app-stream/#standard-notifications
clauseA = {'object_type':'star', 'operator':'one_of', 'field':'/data/post/user/id', 'value':'$authorized_userids'}
clauseB = {'object_type':'user_follow', 'operator':'one_of', 'field':'/data/follows_user/id', 'value':'$authorized_userids'}
clauseC = {'object_type':'post', 'operator':'one_of', 'field':'/data/repost_of/user/id', 'value':'$authorized_userids'}
clauseD = {'object_type':'post', 'operator':'one_of', 'field':'/data/entities/mentions/*/id', 'value':'$authorized_userids'}
# clauseE = {'object_type':'message', 'operator':'one_of', 'field':'/meta/subscribed_user_ids','value':'$authorized_userids'}

# ASSEMBLE THE FILTER FOR THE STREAM
# Creating filters requires a user token, for some odd reason. So, grab an access token from your app developer page, and drop it in here:
bryanAccessToken = 'MANUALLY_CREATE_A_USER_TOKEN_AND_PASTE_HERE'
bryanHeaderString = '?access_token='+bryanAccessToken
headerString = '?access_token='+appAccessToken
createFilterEndpoint = ADN_HOSTNAME + '/stream/0/filters' + bryanHeaderString

clauses = [clauseA, clauseB, clauseC, clauseD]
filterObject = {'clauses':clauses, 'match_policy':'include_any'}
filterResponse = requests.post(createFilterEndpoint,json.dumps(filterObject),headers=HEADER_JSON)

# pprint(json.loads(filterResponse.text))
filterID = json.loads(filterResponse.text).get('data').get('id')

# CREATE AN APP STREAM
# Alright, now that we've created the filter object, we're back to using our app token. We'll start by rolling that filter into a new stream object.
createStreamEndpoint = ADN_HOSTNAME + '/stream/0/streams'+headerString
# objectTypes = ['post','star','user_follow','message'] # I'm building a "Standard Notification" engine, so these are all I care about.
objectTypes = ['post', 'star', 'user_follow'] #We don't have messages in Blixt 1.0
streamObject = {'endpoint':createStreamEndpoint, 'object_types':objectTypes, 'type':'long_poll', 'key':streamKey, 'filter_id':filterID}
# streamObject = {'endpoint':createStreamEndpoint, 'object_types':objectTypes, 'type':'long_poll', 'key':streamKey} # Use this line of code instead if you want to see all global activity.
responseObject = requests.post(createStreamEndpoint,json.dumps(streamObject),headers=HEADER_JSON)
pprint('a')
meta = json.loads(responseObject.text)
pprint(meta)
'''

# LASTLY, GET THE STREAM ENDPOINT. JUST TO PROVE IT EXISTS AND WHATNOT
# Yes, the stream object is 'responseObject' in the above line of code, but once you've gotten all this stream-generation debugged, you don't have to do all of the above code. You can just run this chunk down here:
fetchStreamEndpoint = ADN_HOSTNAME + '/stream/0/streams' + headerString + '&key=' + streamKey + '&purge=1'
# pprint(fetchStreamEndpoint)
blixtStreamResponseObject = requests.get(fetchStreamEndpoint)
blixtStreamData = json.loads(blixtStreamResponseObject.text)

# pprint('b')
# pprint(blixtStreamData)
# pprint(blixtStreamData.get('data'))


# NOW WE HAVE AN APP STREAM. LET'S MONITOR THAT SUMBITCH

# Fetch the ADN Stream Endpoint
try:
	# pprint('getting endpoint')
	streamData = blixtStreamData.get('data')
	stream = streamData[0]
	endpoint = stream.get('endpoint')
	# pprint('c')
	# pprint(endpoint)
except:
	logger.exception('Error fetching stream endpoint')

# Fetch our Amazon Web Services SNS and DynamoDB endpoints
dynamo = boto.dynamodb.connect_to_region(AWS_REGION)
if (useDebug):
	users_table = dynamo.get_table('debug_users')
	devices_table = dynamo.get_table('debug_devices')
else:
	users_table = dynamo.get_table('users')
	devices_table = dynamo.get_table('devices')

sns = boto.sns.connect_to_region(AWS_REGION)

while True:

	try:
		pprint('Checking Stream')
		# pprint('hereeeeee')
		r = requests.get(endpoint, stream=True)
		# pprint(r)
		# pprint('here')
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
				alert = 'SUMMARY_TO_DISPLAY_FOR_NOTIFICATION' #Lockscreen notification
				notificationType = ''
				soundName = 'bxt.aiff'
				badgeCount = 0

				# STARS
				if metaType == 'star':
					notificationType = 'star'
					# So - whose post was starred? and by whom?
					userStarring = data.get('user')
					postStarred = data.get('post')
					userStarred = postStarred.get('user')

					userStarringName = userStarring.get('username')
					userStarredName = userStarred.get('username')
					pprint(userStarringName + ' starred ' + userStarredName + '\'s post')

					userIDsToNotify.append(userStarred.get('id'))
					usernamesToNotify.append(userStarred.get('username'))

					alert = '@' + userStarringName + ' starred: ' + postStarred.get('text')
					titleSuffix = 'starred:'
					subtitleString = postStarred.get('text')
					titleUserID = userStarring.get('id')

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
							notificationType = 'repost'

							userWhosePostWasReposted = post.get('repost_of').get('user')
							userIDsToNotify.append(userWhosePostWasReposted.get('id'))
							usernamesToNotify.append(userWhosePostWasReposted.get('username'))

							repostPost = post
							usernameWhoReposted = post.get('user').get('username')
							postThatWasReposted = post.get('repost_of')
							originalPostText = postThatWasReposted.get('text')

							alert = '@' + usernameWhoReposted + ' reposted: ' + originalPostText
							titleUserID = post.get('user').get('id')
							titleSuffix = 'reposted:'
							subtitleString = originalPostText

						elif post.get('entities').get('mentions'):
							# Then it must be a mention
							notificationType = 'mention'
							mentionPost = post

							# Figure out who to notify: everybody *except* the author (because sometimes, that happens, ya?)
							postAuthorUsername = post.get('user').get('username')
							postAuthorID = post.get('user').get('id')
							mentionEntitiesInPost = post.get('entities').get('mentions')
							for mentionEntity in mentionEntitiesInPost:
								if mentionEntity.get('id') != postAuthorID:
									usernamesToNotify.append(mentionEntity.get('name'))
									userIDsToNotify.append(mentionEntity.get('id'))

							# What's the message summary?
							alert = '@' + postAuthorUsername + ' mentioned: ' + post.get('text')
							titleUserID = post.get('user').get('id')
							titleSuffix = 'mentioned:'
							subtitleString = post.get('text')
							badgeCount = 1

				elif metaType == 'message':
					message = data
					username = message.get('user').get('username')
					notificationType = 'message'

					# Remove the author of the message from the list of folks who'll get notified
					messageAuthorID = message.get('user').get('id')
					subscribedUsers = meta.get('subscribed_user_ids')
					pprint(subscribedUsers)
					for subscriberID in subscribedUsers:
						if subscriberID != messageAuthorID:
							userIDsToNotify.append(subscriberID)
							usernamesToNotify.append('subscriber\'s name redacted for privacy')

					alert = '@' + username + ' messaged: ' + message.get('text')
					titleUserID = message.get('user').get('id')
					titleSuffix = 'messaged:'
					subtitleString = message.get('text')
					badgeCount = 1

				elif metaType == 'user_follow':
					userFollowed = data.get('follows_user')
					userFollowing = data.get('user')
					notificationType = 'follow'

					usernamesToNotify.append(userFollowed.get('username'))
					userIDsToNotify.append(userFollowed.get('id'))

					alert = '@' + userFollowing.get('username') + ' started following @' + userFollowed.get('username') + '.'
					titleUserID = userFollowing.get('id')
					titleSuffix = 'followed @' + userFollowed.get('username')

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
						# pprint('what is thissssss')
						# whatisthis(alert)
						out = alert
						# out = out.replace( u'\u2018', u"'")
						# out = out.replace( u'\u2019', u"'")
						# out = out.replace( u'\u201c', u'"')
						# out = out.replace( u'\u201d', u'"')
						# out.encode('utf-8')

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

						# Fetch the user/arn objects from dynamoDB. 
						# If a user has signed into multiple devices, this will be multiple objects
						dynamoUsers = users_table.query(hash_key=userIDToNotify)

						# Tell SNS to notify this userID with this message
						for dynamoUser in dynamoUsers:
							device_arn = dynamoUser['device_arn']
							ddbUserID = dynamoUser['adn_user_id']

							# Figure out if we should send a notification:
							try:
								userNotificationsAll = dynamoUser['bxt_apns_all']
								userNotificationsFavorites = dynamoUser['bxt_apns_favorites']
								userNotificationsFollowers = dynamoUser['bxt_apns_followers']
								userNotificationsMentions = dynamoUser['bxt_apns_mentions']
								userNotificationsMessages = dynamoUser['bxt_apns_messages']
								userNotificationsReposts = dynamoUser['bxt_apns_reposts']
							except:
								userNotificationsAll = 'BLANK'
								userNotificationsFavorites = 'BLANK'
								userNotificationsFollowers = 'BLANK'
								userNotificationsMentions = 'BLANK'
								userNotificationsMessages = 'BLANK'
								userNotificationsReposts = 'BLANK'


							# pprint('ALL, FAVORITES, FOLLOW, MENTION, MSG, REPOST')
							# pprint(userNotificationsAll)
							# pprint(userNotificationsFavorites)
							# pprint(userNotificationsFollowers)
							# pprint(userNotificationsMentions)
							# pprint(userNotificationsMessages)
							# pprint(userNotificationsReposts)

							shouldNotify = 'YES'

							# Let's build the blixt:// url scheme, while we figure out whether a notification should appear.
							blixt_url = ''
							if (notificationType == 'star'):
								blixt_url = 'blixt://' + usernameToNotify + '/user/' + userStarring.get('id')
								if (userNotificationsFavorites == 'NO'):
									shouldNotify = 'NO'

							elif (notificationType == 'mention'):
								blixt_url = 'blixt://' + usernameToNotify + '/conversation/' + post.get('id')
								if (userNotificationsMentions == 'NO'):
									shouldNotify = 'NO'

							elif (notificationType == 'follow'):
								blixt_url = 'blixt://' + usernameToNotify + '/user/' + userFollowing.get('id')
								if (userNotificationsFollowers == 'NO'):
									shouldNotify = 'NO'

							elif (notificationType == 'message'):
								blixt_url = 'blixt://' + usernameToNotify + '/channel/' + message.get('channel_id')
								if (userNotificationsMessages == 'NO'):
									shouldNotify = 'NO'

							elif (notificationType == 'repost'):
								blixt_url = 'blixt://' + usernameToNotify + '/user/' + post.get('user').get('id')
								if (userNotificationsReposts == 'NO'):
									shouldNotify = 'NO'

							if (ddbUserID == userIDToNotify):
								if (shouldNotify == 'YES'):

									# pprint('about to send notification')

									# We need the badge count
									devicesWithArn = devices_table.query(hash_key=device_arn)
									for device in devicesWithArn:
										# pprint(device)
										if (device.has_key('bxt_badgeCount')):
											# pprint('badge count was in dynamodb as: ')
											# pprint(device['bxt_badgeCount'])
											badgeCount += device['bxt_badgeCount']

										else:
											badgeCount = badgeCount
											# pprint('No badge count in database')
									# pprint('badge count in the notification will be ' + str(badgeCount))

									#Encode and shorten the data before sending
									textLength = 95
									if (len(out)>textLength):
										out = out[:textLength]+'...'
										# pprint('truncated alert: ' + out)

									# Build the apns_dict, depending on what type we have
									if (notificationType == 'star'):
										# Stars get an alert/badge/sound/type + postID + userID
										apns_dict = {'aps':{'alert':out, 'sound':soundName, 'badge':badgeCount, 'b_t':notificationType, 'b_uID':userStarring.get('id'), 'b_pID':postStarred.get('id')}}

									elif (notificationType == 'repost'):
										# Reposts get an alert/badge/sound/type + postID
										apns_dict = {'aps':{'alert':out, 'sound':soundName, 'badge':badgeCount, 'b_t':notificationType, 'b_pID':repostPost.get('id')}}

									elif (notificationType == 'mention'):
										# Mentions get an alert/badge/sound/type + postID + URL
										apns_dict = {'aps':{'alert':out, 'sound':soundName, 'badge':badgeCount, 'b_t':notificationType, 'b_pID':mentionPost.get('id'),'b_url':blixt_url}}

									elif (notificationType == 'follow'):
										# Follows get an alert/badge/sound/type + userID + youName + URL
										apns_dict = {'aps':{'alert':out, 'sound':soundName, 'badge':badgeCount, 'b_t':notificationType, 'b_uID':userFollowing.get('id'),'b_you':userFollowed.get('username'),'b_url':blixt_url}}

									elif (notificationType == 'message'):
										# Messages get an alert/badge/sound/type + userID + URL
										apns_dict = {'aps':{'alert':out, 'sound':soundName, 'badge':badgeCount, 'b_t':notificationType, 'b_uID':messageAuthorID,'b_url':blixt_url}}



									# Send the push notification via SNS
									# pprint('Pushing notification to ddbuserID: '+ddbUserID)
									apns_string = json.dumps(apns_dict,ensure_ascii=True)
									if (useDebug):
										message = {'default':'default message','APNS_SANDBOX':apns_string}
									else:
										message = {'default':'default message','APNS':apns_string}

									messageJSON = json.dumps(message,ensure_ascii=False)
									pprint(messageJSON)
									pprint('device arn: ' + device_arn)
									results = sns.publish(message=messageJSON,target_arn=device_arn,message_structure='json')

									# Update the badge count in DynamoDB
									device['bxt_badgeCount'] = badgeCount
									device.save()
									pprint('push notification complete without crashing')

								else:
									pprint('shouldNotify was NO')
							else:
								pprint('UserID from DDB: ' + ddbUserID + ' Does not match userIDToNotify: ' + userIDToNotify)    


						# Increment the index for the for loop
						i += 1           
				else:
					if metaType == 'message':
						pprint('This channel does not have subscribed_ids. How do I sort these out? This one is from: ' + meta.get('channel_type'))
					else:
						pprint('You should write code to process metaType: ' + metaType)
			except Exception as e:
				traceback.print_exc()

	print 'sleeping before reconnect'
	time.sleep(1)
