import sys
import time
from datetime import datetime, timedelta, tzinfo
import rethinkdb as r

# arguments
# 1: horizon DB Name

HorizonDB = sys.argv[1]
SettingsTable = 'user_settings'
SubscriptionTable = 'subscription'
NotificationTable = 'notifications'

premiumCost = 150000000

if __name__ == '__main__':

  horizon_conn = r.connect(db=HorizonDB)

  print("Executing at %s" %  datetime.now())

  now = datetime.now(r.make_timezone('00:00'))

  subscriptions = list(r.table(SubscriptionTable).run(horizon_conn))

  for sub in subscriptions:
    
    if 'subscription_date' not in sub or 'premium' not in sub:
      continue

    if sub['subscription_date'] is None:
      continue

    if sub['premium'] == False:
      continue

    if now - sub['subscription_date'] > timedelta(days=30):

      settings = list(r.table(SettingsTable).filter(lambda doc: doc['userID'] == sub['userID']).limit(1).run(horizon_conn))[0]

      renew = True
      premium = True

      if 'general' in settings:
        if 'auto_renew' in settings['general']:
          renew = True if settings['general']['auto_renew'] == True else False

      if renew == True:

        if sub['balance'] < premiumCost:
          premium = False

      else:
        premium = False

      if premium == True:

        print("Renewing subscription for user %s " % sub['userName'])

        r.table(SubscriptionTable).get(sub['id']).update({
          'balance': r.row['balance'] - premiumCost,
          'subscription_date': now,
          'history': r.row['history'].append({
            'type': 1,
            'description': 'Subscription fee',
            'processed': True,
            'time': now,
            'amount': premiumCost
          })
        }).run(horizon_conn)

        r.table(NotificationTable).insert({
          'userID': sub['userID'],
          'read': False,
          'message': 'Your premium subscription has been automatically renewed',
          'time': now
        }).run(horizon_conn)

      else:

        print("Ending subscription for user %s " % sub['userName'])

        # Remove premium group
        active = list(r.table('users').filter({'user_id': sub['userID']}).limit(1).run(horizon_conn))

        if len(active) != 1:
          print("Failed to find user table entry for user %s" % sub['userID'])
          continue

        active = active[0]

        if 'premium' in active['groups']:
          active['groups'].remove('premium')

        r.table('users').filter({'user_id': sub['userID']}).limit(1).update({'groups': active['groups']}).run(horizon_conn)

        # Remove premium status
        r.table(SubscriptionTable).get(sub['id']).update({
          'premium': False,
          'subscription_date': None
        }).run(horizon_conn)

        # Notify
        r.table(NotificationTable).insert({
          'userID': sub['userID'],
          'read': False,
          'message': 'Your premium subscription has ended',
          'time': now
        }).run(horizon_conn)