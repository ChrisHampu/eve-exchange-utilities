import requests
import asyncio
import time
import functools
import traceback
import json
import locale
from datetime import timedelta
import os

from . import database, settings as _settings, mail_interface as _mail

# for comma seperating floats
locale.setlocale(locale.LC_ALL, 'en_US.utf8')

publish_url = 'localhost:4501'

mailer = _mail.MailInterface()
config = _settings.Settings()

db = database.DatabaseConnector()

with open(os.path.realpath('./sde/market_id_to_name.json'), 'r', encoding='utf-8') as f:
    market_id_to_name = json.loads(f.read())

class AlertInterface:
    def __init__(self):
        self.user_settings = None

    async def trigger_alert(self, alert, message):

        alert_settings = self.get_user_alert_settings(alert['user_id'])

        if alert_settings == None:
            print("Unable to trigger alert for user %s due to missing settings document" % alert['user_id'])
            return

        show_browser = alert_settings.get('canShowBrowserNotification', True)
        send_evemail = alert_settings.get('canSendMailNotification', True)

        if show_browser == True:
            msg_doc = {
                'user_id': alert['user_id'],
                'message': message
            }

            await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.post,
                                                                                    'http://' + publish_url + '/publish/alert',
                                                                                    timeout=5, json=msg_doc))

        if send_evemail == True:
            if alert['alertType'] == 0:
                subject = 'Price Alert - EVE Exchange'
            else:
                subject = 'Alert - EVE Exchange'

            mailer.queueMail(alert['user_id'], subject, message)

            db.alerts.find_and_modify({'user_id': alert['user_id'], '_id': alert['_id']}, {
                '$set': {
                    'nextTrigger': config.utcnow + timedelta(hours=alert['frequency']),
                    'lastTrigger': config.utcnow
                }
            })

        await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.post,
                                                           'http://' + publish_url + '/publish/alerts/' + alert['user_id'], timeout=5))

        print('Triggered alert %s for %s' % (alert['_id'], alert['user_id']))

    def get_user_region(self, user_id):
        user = self.user_settings.get(user_id, None)

        if user is None:
            return None

        market_settings = user.get('market', None)

        if market_settings is None:
            return None

        return int(market_settings.get('region', 10000002))

    def get_user_alert_settings(self, user_id):
        user = self.user_settings.get(user_id, None)

        if user is None:
            return None

        alert_settings = user.get('alerts', None)

        return alert_settings

    async def check_price_alerts(self, minute_docs):
        print("Checking price alerts")

        self.user_settings = await db.GetAllUserSettings()

        for alert in await db.alerts.find({
            'nextTrigger': {'$lt': config.utcnow},
            'alertType': 0,
            'paused': False
        }).to_list(length=None):

            indicator = alert['priceAlertPriceType']
            comparator = alert['priceAlertComparator']
            amount = round(alert['priceAlertAmount'], 2)
            item_id = alert['priceAlertItemID']

            region = self.get_user_region(alert['user_id'])

            if region is None:
                continue

            item_doc = next((x for x in minute_docs if x['type'] == item_id), None)

            if item_doc is None:
                continue

            region_doc = next((x for x in item_doc['regions'] if x.get('region', 0) == region), None)

            if region_doc is None:
                continue

            if indicator == 0:
                key = 'buyPercentile'
                key_name = 'buy price'
            elif indicator == 1:
                key = 'sellPercentile'
                key_name = 'sell price'
            elif indicator == 2:
                key = 'spread'
                key_name = 'spread'
            else:
                key = 'tradeVolume'
                key_name = 'trade volume'

            if comparator == 0:
                alert_passes = region_doc[key] > amount
                comparator_verb = 'exceeds'
            elif comparator == 1:
                alert_passes = region_doc[key] < amount
                comparator_verb = 'is below'
            else:
                alert_passes = region_doc[key] == amount
                comparator_verb = 'matches'

            if alert_passes == True:

                if isinstance(amount, int):
                    amount = format(amount, ",d")
                else:
                    amount = locale.format("%.2f", amount, grouping=True)

                if isinstance(region_doc[key], int):
                    regional = format(region_doc[key], ",d")
                else:
                    regional = locale.format("%.2f", region_doc[key], grouping=True)

                msg = "Price alert: %s has a %s of %s which %s your alert set at %s" % (market_id_to_name[str(item_id)], key_name, regional, comparator_verb, amount)

                await self.trigger_alert(alert, msg)
            