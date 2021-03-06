import asyncio
import requests
from typing import List
from pymongo import DESCENDING
from datetime import datetime
import sys
import os
import time
import functools
import traceback
from bson.objectid import ObjectId

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from aggregation import settings as _settings, database, redis_interface, alert_interface
from market import OrderInterface

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

publish_url = 'localhost:4501'

settings = _settings.Settings()
db = database.db
cache = redis_interface.CacheInterface(OrderInterface())
alerts = alert_interface.AlertInterface()

standard_headers = settings.standard_headers

# TODO: add error checking to every single request
class ProfitAggregator:
    def __init__(self):
        self._rowCount = 2000 # Number of rows to pull from api
        self._hour_offset = settings.hour_offset_utc.utctimetuple()
        self._sales = {} # char id -> [transactions]
        self._profits = {} # char id -> profit results

    def getCharacterTransactions(self, char_id, eve_key, eve_vcode, fromID=None) -> List:

        rows = []

        try:
            auth = (char_id, eve_key, eve_vcode, self._rowCount, "" if fromID is None else "&fromID=" + str(fromID))
            url = "https://api.eveonline.com/char/WalletTransactions.xml.aspx?characterID=%s&keyID=%s&vCode=%s&rowCount=%s%s" % auth
            req = requests.get(url, timeout=2, headers=standard_headers)

            tree = ET.fromstring(req.text)

            if tree.find('error') is not None:
                print("Error while pulling character transactions (%s, %s, %s): %s" % (char_id, eve_key, eve_vcode, tree.find('error').text))
                return []

            rows = [i for i in list(tree.find('result').find('rowset')) if i.attrib['transactionFor'] == 'personal']

        except:
            pass

        return rows

    def getCorporationTransactions(self, wallet_key, eve_key, eve_vcode, fromID=None) -> List:

        rows = []

        try:
            auth = (wallet_key, eve_key, eve_vcode, self._rowCount, "" if fromID is None else "&fromID=" + str(fromID))
            url = "https://api.eveonline.com/corp/WalletTransactions.xml.aspx?accountKey=%s&keyID=%s&vCode=%s&rowCount=%s%s" % auth
            req = requests.get(url, timeout=2, headers=standard_headers)

            tree = ET.fromstring(req.text)

            if tree.find('error') is not None:
                print("Error while pulling corporation transactions (%s, %s, %s): %s" % (wallet_key, eve_key, eve_vcode, tree.find('error').text))
                return []

            rows = [i for i in list(tree.find('result').find('rowset')) if i.attrib['transactionFor'] == 'corporation']

        except:
            pass

        return rows

    def getAllCharacterTransactions(self, char_id, eve_key, eve_vcode) -> List:

        fromID = None
        journal = self.getCharacterTransactions(char_id, eve_key, eve_vcode, fromID)

        if len(journal) == 0:
            return []

        '''
        fromID = journal[-1].attrib['transactionID']
        lastPull = len(journal)

        while lastPull >= self._rowCount-1:
            newJournal = self.getCharacterTransactions(char_id, eve_key, eve_vcode, fromID)
            lastPull = len(newJournal)
            if lastPull == 0:
                break

            journal.extend(newJournal)
            fromID = newJournal[-1].attrib['transactionID']

        if lastPull > 0:
            newJournal = self.getCharacterTransactions(char_id, eve_key, eve_vcode, fromID)
            journal.extend(newJournal)
        '''

        return journal

    def getAllCorporationTransactions(self, wallet_key, eve_key, eve_vcode) -> List:

        fromID = None
        journal = self.getCorporationTransactions(wallet_key, eve_key, eve_vcode, fromID)

        if len(journal) == 0:
            return []

        '''
        fromID = journal[-1].attrib['transactionID']
        lastPull = len(journal)

        while lastPull >= self._rowCount-1:
            newJournal = self.getCorporationTransactions(wallet_key, eve_key, eve_vcode, fromID)
            lastPull = len(newJournal)
            if lastPull == 0:
                break

            journal.extend(newJournal)
            fromID = newJournal[-1].attrib['transactionID']

        if lastPull > 0:
            newJournal = self.getCorporationTransactions(wallet_key, eve_key, eve_vcode, fromID)
            journal.extend(newJournal)
        '''

        return journal

    def getCharacterJournalEntries(self, char_id, eve_key, eve_vcode) -> List:

        rows = []

        try:
            auth = (char_id, eve_key, eve_vcode)
            url = "https://api.eveonline.com/char/WalletJournal.xml.aspx?characterID=%s&keyID=%s&vCode=%s&rowCount=2000" % auth
            req = requests.get(url, timeout=2, headers=standard_headers)

            tree = ET.fromstring(req.text)

            if tree.find('error') is not None:
                print("Error while pulling character journal (%s, %s, %s): %s" % (char_id, eve_key, eve_vcode, tree.find('error').text))
                return []

            rows = list(tree.find('result').find('rowset'))
        except:
            pass

        data = [{key:row.attrib[key] for key in ('amount', 'refTypeID', 'date')} for row in rows if time.strptime(row.attrib['date'], '%Y-%m-%d %H:%M:%S') > self._hour_offset]

        return data

    def getCorporationJournalEntries(self, wallet_key, eve_key, eve_vcode) -> List:

        rows = []

        try:
            auth = (wallet_key, eve_key, eve_vcode)
            url = "https://api.eveonline.com/corp/WalletJournal.xml.aspx?accountKey=%s&keyID=%s&vCode=%s&rowCount=2000" % auth
            req = requests.get(url, timeout=2, headers=standard_headers)

            tree = ET.fromstring(req.text)

            if tree.find('error') is not None:
                print("Error while pulling corporation journal (%s, %s, %s): %s" % (wallet_key, eve_key, eve_vcode, tree.find('error').text))
                return []

            rows = list(tree.find('result').find('rowset'))
        except:
            pass

        data = [{key:row.attrib[key] for key in ('amount', 'refTypeID', 'date')} for row in rows if time.strptime(row.attrib['date'], '%Y-%m-%d %H:%M:%S') > self._hour_offset]

        return data

    # user_id -> owner of the ETF account
    # char_id -> character to make API calls for
    # entity_name -> character name for transactions
    async def gatherCharacterProfitData(self, user_id, char_id, entity_name, eve_key, eve_vcode):

        rows = self.getAllCharacterTransactions(char_id, eve_key, eve_vcode)
        journal = self.getCharacterJournalEntries(char_id, eve_key, eve_vcode)

        await self.gatherProfitData(user_id, 1000, char_id, entity_name, rows, journal)

    # wallet_key -> wallet division account key
    async def gatherCorporationProfitData(self, user_id, corp_id, wallet_key, entity_name, eve_key, eve_vcode):

        rows = self.getAllCorporationTransactions(wallet_key, eve_key, eve_vcode)
        journal = self.getCorporationJournalEntries(wallet_key, eve_key, eve_vcode)

        await self.gatherProfitData(user_id, wallet_key, corp_id, entity_name, rows, journal)

    async def gatherProfitData(self, user_id, wallet_key, entity_id, entity_name, transactions, journal):

        data = [{key: row.attrib[key] for key in (
            'typeName', 'journalTransactionID', 'transactionType', 'price', 'quantity', 'typeID',
            'transactionDateTime')} for row in transactions]

        sells = [x for x in data if
                 time.strptime(x['transactionDateTime'], '%Y-%m-%d %H:%M:%S') > self._hour_offset and x[
                     'transactionType'] == 'sell']
        #buys = [x for x in data if
        #        time.strptime(x['transactionDateTime'], '%Y-%m-%d %H:%M:%S') > self._hour_offset and x[
        #            'transactionType'] == 'buy']

        groupedSells = dict()
        sales = list()

        for i in sells:
            if i['typeID'] in groupedSells:
                groupedSells[i['typeID']].append(i)
            else:
                groupedSells[i['typeID']] = [i]

        for typeID in groupedSells.keys():
            buy = next((x for x in data if x['typeID'] == typeID and x['transactionType'] == 'buy'), None)

            if buy is None:
                #print("No buy data for type ID %s" % typeID)
                continue

            salesCount = len(groupedSells[typeID])

            if salesCount == 0:
                continue

            totalProfit = 0
            quantity = 0

            for x in groupedSells[typeID]:
                price = float(x['price']) - float(buy['price'])
                _sales = int(x['quantity'])

                totalProfit += _sales * price
                quantity += _sales

            sales.append({
                'type': typeID,
                'sellPrice': float(groupedSells[typeID][0]['price']),
                'name': groupedSells[typeID][0]['typeName'],
                'totalProfit': totalProfit,
                'quantity': quantity,
                'avgPerUnit': totalProfit / quantity,
                'time': datetime.strptime(groupedSells[typeID][0]['transactionDateTime'],
                                          '%Y-%m-%d %H:%M:%S').replace(tzinfo=settings.utcnow.tzinfo),
                'who': entity_name,
                'whoID': entity_id,
                'walletKey': wallet_key
            })

        taxEntries = [x for x in journal if x['refTypeID'] == '54']
        brokerEntries = [x for x in journal if x['refTypeID'] == '46']

        tax = sum(float(x['amount']) for x in taxEntries)
        broker = sum(float(x['amount']) for x in brokerEntries)
        totalProfit = sum(float(x['totalProfit']) for x in sales)

        if user_id in self._sales:
            self._sales[user_id].extend(sales)
        else:
            self._sales[user_id] = sales

        if user_id in self._profits:
            self._profits[user_id]['profit'] += totalProfit
            self._profits[user_id]['taxes'] += tax
            self._profits[user_id]['broker'] += broker
        else:
            self._profits[user_id] = {'profit': totalProfit, 'taxes': tax, 'broker': broker}

    async def updateTopItems(self, user_id) -> List:
        top_items = await db.profit_top_items.find_one({'user_id': user_id})

        sales = self._sales[user_id] if user_id in self._sales else None

        if sales is None:
            return []

        data = [{key: row[key] for key in ('totalProfit', 'quantity', 'avgPerUnit', 'type', 'name', 'who', 'whoID')} for row in
                sales]

        # create new top list
        if top_items is None:
            print("User %s has no top items chart" % user_id)

        # modify/add to existing top list
        else:
            for item in data:

                # update a specifc item
                find = [(i, j) for i, j in enumerate(top_items['items']) if j['type'] == item['type']]

                if len(find) == 0:
                    top_items['items'].append({
                        'totalProfit': item['totalProfit'],
                        'quantity': item['quantity'],
                        'type': item['type'],
                        'name': item['name'],
                        'avgPerUnit': item['avgPerUnit']
                    })

                else:
                    index, top = find[0]

                    top['quantity'] += item['quantity']
                    top['totalProfit'] += item['totalProfit']
                    top['avgPerUnit'] = top['totalProfit'] / top['quantity']

                    top_items['items'][index] = top

                # update a char/corp profile

                if 'profiles' not in top_items:
                    top_items['profiles'] = [] # Upgrade a legacy document

                find = [(i, j) for i, j in enumerate(top_items['profiles']) if j['whoID'] == item['whoID']]

                if len(find) == 0:
                    top_items['profiles'].append({
                        'whoID': item['whoID'],
                        'who': item['who'],
                        'totalProfit': item['totalProfit'],
                        'salesCount': 1,
                        'avgProfit': item['totalProfit']
                    })

                else:
                    index, top = find[0]

                    top['totalProfit'] += item['totalProfit']
                    top['salesCount'] += 1
                    top['avgProfit'] = top['totalProfit'] / top['salesCount']

                    top_items['profiles'][index] = top

            await db.profit_top_items.find_and_modify({'user_id': user_id}, top_items)

        return [{**{key: row[key] for key in
                                 ('totalProfit', 'quantity', 'avgPerUnit', 'time', 'name', 'type', 'who', 'whoID')},
                              **{'user_id': user_id}} for row in sales]

    async def updateAllTime(self, user_id):

        profit = self._profits[user_id] if user_id in self._profits else None

        # If no profit data, just insert 0's for this hour
        this_hourly_result = {
            'user_id': user_id,
            'time': settings.utcnow,
            'frequency': 'hourly',
            'profit': profit['profit'] if profit is not None else 0,
            'taxes': profit['taxes'] if profit is not None else 0,
            'broker': profit['broker'] if profit is not None else 0
        }

        # First load all hourly results
        all_profit_hourly = await db.profit_chart.find({'frequency': 'hourly', 'user_id': user_id}).sort('time', DESCENDING).to_list(length=None)

        # Purge anything older than 48 hours
        if len(all_profit_hourly) >= 48:

            await db.profit_chart.remove({'user_id': user_id, 'frequency': 'hourly', 'time': { '$lte': all_profit_hourly[:48][-1]['time'] }})

        # Insert new result
        await db.profit_chart.insert(this_hourly_result)

        # Find alltime record for this user
        alltime = await db.profit_all_time.find_one({'user_id': user_id})

        if alltime is None:
            print("User %s has no all time doc" % user_id)

        else:
            # Aggregate past 24 hours of hourly docs
            hourly_chart = await db.profit_chart.find({'frequency': 'hourly', 'user_id': user_id}).sort('time', DESCENDING).limit(24).to_list(length=None)

            sumDocs = lambda docs: {'profit': sum([i['profit'] for i in docs]),
                                    'broker': sum([i['broker'] for i in docs]),
                                    'taxes': sum([i['taxes'] for i in docs])}

            pastDay = sumDocs(hourly_chart)

            # Update alltime statistics
            await db.profit_all_time.find_and_modify({'_id': ObjectId(oid=alltime['_id'])}, {
                '$set': {
                    'day': pastDay
                },
                '$inc': {
                    'alltime.profit': profit['profit'] if profit is not None else 0,
                    'alltime.broker': profit['broker'] if profit is not None else 0,
                    'alltime.taxes': profit['taxes'] if profit is not None else 0
                }
            })

            # Daily: Aggregate hourly docs into a single daily doc
            if settings.is_daily:

                await db.profit_chart.insert({
                    **pastDay,
                    **{'user_id': user_id, 'time': settings.utcnow, 'frequency': 'daily'}
                })

                daily_chart = await db.profit_chart.find({'frequency': 'daily', 'user_id': user_id}).sort('time', DESCENDING).limit(90).to_list(length=None)

                pastWeek = sumDocs(daily_chart[:7])
                pastMonth = sumDocs(daily_chart[:30])
                pastBiannual = sumDocs(daily_chart[:90])

                await db.profit_all_time.find_and_modify({'_id': ObjectId(oid=alltime['_id'])}, {
                    '$set': {
                        'week': pastWeek,
                        'month': pastMonth,
                        'biannual': pastBiannual
                    }
                })

    async def loadCharacterOrders(self, user_id, char_id, entity_name, eve_key, eve_vcode):

        auth = (char_id, eve_key, eve_vcode)
        url = "https://api.eveonline.com/char/MarketOrders.xml.aspx?characterID=%s&keyID=%s&vCode=%s" % auth

        rows = []

        try:
            req = await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.get, url, timeout=2, headers=standard_headers))

            tree = ET.fromstring(req.text)

            if tree.find('error') is not None:
                print(tree.find('error').text)
                print("Error while pulling character orders for user %s" % user_id)
                return []

            rows = [row for row in list(tree.find('result').find('rowset')) if row.attrib['orderState'] == '0']

        except:
            pass

        orders = [{**{k:row.attrib[k] for k in ('orderID', 'orderState', 'volRemaining', 'issued', 'minVolume', 'stationID', 'volEntered', 'typeID', 'bid', 'price')},
                   **{'user_id': user_id, 'who': entity_name, 'whoID': char_id}} for row in rows]

        if len(orders) == 0:
            return []

        await db.user_orders.insert(orders)

        return orders

    async def getCharacterOrderById(self, order_id, user_id, char_id, entity_name, eve_key, eve_vcode):

        auth = (char_id, eve_key, eve_vcode, order_id)
        url = "https://api.eveonline.com/char/MarketOrders.xml.aspx?characterID=%s&keyID=%s&vCode=%s&orderID=%s" % auth

        rows = []

        try:
            req = await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.get, url, timeout=2, headers=standard_headers))

            tree = ET.fromstring(req.text)

            if tree.find('error') is not None:
                print("Error while pulling character orders for user %s and order %s: %s" % (user_id, order_id, tree.find('error').text))
                return []

            rows = [row for row in list(tree.find('result').find('rowset'))]

        except:
            pass

        orders = [{**{k:row.attrib[k] for k in ('orderID', 'orderState', 'volRemaining', 'issued', 'minVolume', 'stationID', 'volEntered', 'typeID', 'bid', 'price')},
                   **{'user_id': user_id, 'who': entity_name, 'whoID': char_id}} for row in rows]

        if len(orders) == 0:
            return []

        return orders

    def findAssetChildren(self, asset, entity_info):

        obj = {
            **{k: int(asset.attrib[k]) for k in (
                'typeID', 'quantity', 'flag', 'singleton')},
            **entity_info,
            **{
                'children': [],
                'locationID': int(asset.attrib['locationID']) if 'locationID' in asset.attrib else 0,
                'value': cache.GetItemSellPrice(int(asset.attrib['typeID']))
            },
        }

        try:

            children = []

            for row in list(asset.find('rowset')):
                children.append(self.findAssetChildren(row, entity_info))

            obj['children'] = children

        except:
            pass

        return obj

    async def loadCharacterAssets(self, user_id, char_id, entity_name, eve_key, eve_vcode, assets_data):

        auth = (char_id, eve_key, eve_vcode)
        url = "https://api.eveonline.com/char/AssetList.xml.aspx?characterID=%s&keyID=%s&vCode=%s" % auth
        rows = []

        try:
            req = await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.get, url, timeout=2, headers=standard_headers))

            tree = ET.fromstring(req.text)

            if tree.find('error') is not None:
                print(tree.find('error').text)
                print("Error while pulling character assets for user %s" % user_id)
                return

            rows = [row for row in list(tree.find('result').find('rowset'))]

        except:
            pass

        assets = []
        entity_info = {'user_id': user_id, 'who': entity_name, 'whoID': char_id}

        for asset in rows:
            assets.append(self.findAssetChildren(asset, entity_info))

        assets_data['list'].extend(assets)

    async def loadCorporationAssets(self, user_id, wallet_key, entity_id, entity_name, eve_key, eve_vcode, assets_data):

        auth = (eve_key, eve_vcode)
        url = "https://api.eveonline.com/corp/AssetList.xml.aspx?keyID=%s&vCode=%s" % auth
        rows = []

        try:
            req = await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.get, url, timeout=2, headers=standard_headers))

            tree = ET.fromstring(req.text)

            if tree.find('error') is not None:
                print(tree.find('error').text)
                print("Error while pulling corporation assets for user %s" % user_id)
                return

            rows = [row for row in list(tree.find('result').find('rowset'))]

        except:
            pass

        assets = []
        entity_info = {'user_id': user_id, 'who': entity_name, 'whoID': entity_id}

        for asset in rows:
            assets.append(self.findAssetChildren(asset, entity_info))

        assets_data['list'].extend(assets)

    async def loadCorporationOrders(self, user_id, wallet_key, entity_id, entity_name, eve_key, eve_vcode):

        auth = (eve_key, eve_vcode)
        url = "https://api.eveonline.com/corp/MarketOrders.xml.aspx?keyID=%s&vCode=%s" % auth

        rows = []

        try:
            req = await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.get, url, timeout=2, headers=standard_headers))

            tree = ET.fromstring(req.text)

            if tree.find('error') is not None:
                print(tree.find('error').text)
                print("Error while pulling corporation orders for user %s" % user_id)
                return []

            rows = [row for row in list(tree.find('result').find('rowset')) if row.attrib['orderState'] == '0']

        except:
            pass

        orders = [{**{k:row.attrib[k] for k in ('orderID', 'orderState', 'volRemaining', 'issued', 'minVolume', 'stationID', 'volEntered', 'typeID', 'bid', 'price')},
                   **{'user_id': user_id, 'who': entity_name, 'whoID': entity_id}}
                  for row in rows if row.attrib['accountKey'] == str(wallet_key)]

        if len(orders) == 0:
            return []

        await db.user_orders.insert(orders)

        return orders

    async def getCorporationOrderById(self, order_id, user_id, wallet_key, entity_id, entity_name, eve_key, eve_vcode):

        auth = (eve_key, eve_vcode, order_id)
        url = "https://api.eveonline.com/corp/MarketOrders.xml.aspx?keyID=%s&vCode=%s&orderID=%s" % auth

        rows = []

        try:
            req = await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.get, url, timeout=2, headers=standard_headers))

            tree = ET.fromstring(req.text)

            if tree.find('error') is not None:
                print("Error while pulling corporation orders for user %s and order %s: %s" % (user_id, order_id, tree.find('error').text))
                return []

            rows = [row for row in list(tree.find('result').find('rowset'))]

        except:
            pass

        orders = [{**{k:row.attrib[k] for k in ('orderID', 'orderState', 'volRemaining', 'issued', 'minVolume', 'stationID', 'volEntered', 'typeID', 'bid', 'price')},
                   **{'user_id': user_id, 'who': entity_name, 'whoID': entity_id}}
                  for row in rows if row.attrib['accountKey'] == str(wallet_key)]

        if len(orders) == 0:
            return []

        return orders

    async def clearUserOrders(self, user_id):

        old_orders = await db.user_orders.find({'user_id': user_id}).to_list(length=None)

        await db.user_orders.remove({'user_id': user_id})

        return old_orders

    async def getCorporationBalance(self, user_id, wallet_key, eve_key, eve_vcode):

        auth = (eve_key, eve_vcode)
        url = "https://api.eveonline.com/corp/AccountBalance.xml.aspx?keyID=%s&vCode=%s" % auth

        try:
            req = await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.get, url, timeout=2, headers=standard_headers))

            tree = ET.fromstring(req.text)

            if tree.find('error') is not None:
                print(tree.find('error').text)
                print("Error while pulling corporation balance for user %s" % user_id)
                return 0

            rows = [row for row in list(tree.find('result').find('rowset'))]

            for row in rows:
                if row.attrib['accountKey'] == str(wallet_key):
                    return float(row.attrib['balance'])

        except:
            pass

        return 0

    async def getCharacterBalance(self, user_id, char_id, eve_key, eve_vcode):

        auth = (char_id, eve_key, eve_vcode)
        url = "https://api.eveonline.com/char/AccountBalance.xml.aspx?characterID=%s&keyID=%s&vCode=%s" % auth

        try:
            req = await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.get, url, timeout=2, headers=standard_headers))

            tree = ET.fromstring(req.text)

            if tree.find('error') is not None:
                print("Error while pulling character balance for user %s: %s" % (user_id, tree.find('error').text))
                return 0

            rows = [row for row in list(tree.find('result').find('rowset'))]

            for row in rows:
                if row.attrib['accountKey'] == "1000": # Characters have a single wallet of key '1000'
                    return float(row.attrib['balance'])

        except:
            pass

        return 0

    async def updateWalletBalance(self, user_id, profile_id, balance):

        await db.settings.find_and_modify(
            {
                'user_id': user_id,
                'profiles.id': profile_id
            },
            {
                '$set': {'profiles.$.wallet_balance': balance}
            }
        )

    async def verifyKey(self, user_id, char_id, corp_id, _type, eve_key, eve_vcode, error):

        verified = True
        entity = None
        message = "There was a problem with looking up your API key. Please verify that this key still exists"
        auth = (char_id, eve_key, eve_vcode)

        try:
            keyInfo = await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.get,
                                                                                             'https://api.eveonline.com/account/APIKeyInfo.xml.aspx?characterID=%s&keyID=%s&vCode=%s' % auth,
                                                                                             timeout=5, headers=standard_headers))

            tree = ET.fromstring(keyInfo.text)

            if tree.find('error') is not None:
                raise Exception()

            key = tree.find('result').find('key')

            if key is None:
                raise Exception()

            # verify access mask
            if _type == 0:

                if key.attrib['accessMask'] != "23072779":
                    message = "The access mask for this API key is not set to 23072779"
                    raise Exception()

                if key.attrib['type'] == "Corporation":
                    message = "This API key is not a valid account or character key"
                    raise Exception()

            else:
                if key.attrib['accessMask'] != "3149835":
                    message = "The access mask for this API key is not set to 3149835"
                    raise Exception()

                if key.attrib['type'] != "Corporation":
                    message = "This API key is not a valid corporation key"
                    raise Exception()

            # verify expiry
            if key.attrib['expires'] != "":
                expiry = datetime.strptime(key.attrib['expires'], "%Y-%m-%d %H:%M:%S")
                if settings.utcnow > expiry:
                    message = "This API key is expired. Please remove the expiry or replace this key"
                    raise Exception()

            # Verify entity exists
            found = False
            rows = [row for row in key.find('rowset')]

            for row in rows:
                if int(row.attrib['characterID']) == char_id:
                    found = True
                    entity = row

            if found == False:
                message = "There was a problem verifying the character attached to this key. If it has been changed, this key will need to be replaced"
                raise Exception()

            # verify entity info
            else:

                if int(entity.attrib['corporationID']) != corp_id:
                    print("Updating corporation for user %s (key: %s, vcode: %s, corp: %s) to: %s " % (char_id, eve_key, eve_vcode, corp_id, entity.attrib['corporationName']))

                    await db.settings.find_and_modify({
                        'user_id': user_id,
                        'profiles': {
                            '$elemMatch': {
                                'key_id': eve_key,
                                'vcode': eve_vcode
                            }
                        }
                    }, {
                        '$set': {
                            'profiles.$.corporation_id': int(entity.attrib['corporationID']),
                            'profiles.$.corporation_name': entity.attrib['corporationName']
                        }
                    })

        except:
            verified = False

        if verified == False:

            await db.settings.find_and_modify(
                {
                    'user_id': user_id,
                    'profiles': {
                        '$elemMatch': {
                            'key_id': eve_key,
                            'vcode': eve_vcode
                        }
                    }
                },
                {
                    '$set': {
                        'profiles.$.error': message
                    }
                })

        else:
            await db.settings.find_and_modify({
                'user_id': user_id,
                'profiles': {
                    '$elemMatch': {
                        'key_id': eve_key,
                        'vcode': eve_vcode
                    }
                }
            }, {
                '$set': {
                    'profiles.$.error': None
                }
            })

        return verified

    async def checkUserOrders(self, old_orders, _type, user_id, entity_id, entity_name, eve_key, vcode, wallet_key = 0):

        changed_orders = []

        try:
            if _type == 0:
                new_orders = await self.loadCharacterOrders(user_id, entity_id, entity_name, eve_key, vcode)

            else:
                new_orders = await self.loadCorporationOrders(user_id, wallet_key, entity_id, entity_name, eve_key, vcode)
        except:
            print("Failed to load orders during check for user %s (entity: %s, key: %s, vcode: %s)" % (user_id, entity_id, eve_key, vcode))
            return changed_orders

        old_ids = {x['orderID'] for x in old_orders if x['whoID'] == entity_id}
        new_ids = {x['orderID'] for x in new_orders if x['whoID'] == entity_id}

        intersected = list(old_ids.intersection(new_ids))
        difference = list(old_ids.difference(new_ids))

        for id in intersected:
            new_order = next((x for x in new_orders if x['orderID'] == id), None)
            old_order = next((x for x in old_orders if x['orderID'] == id), None)

            if new_order == None or old_order == None:
                continue

            oldVolRem = int(old_order['volRemaining'])
            newVolRem = int(new_order['volRemaining'])

            if newVolRem < oldVolRem:

                changed_orders.append({
                    'change': oldVolRem - newVolRem,
                    'orderID': id,
                    'whoID': entity_id,
                    'whoName': entity_name,
                    'previous': oldVolRem,
                    'remaining': newVolRem,
                    'bid': int(old_order['bid']) == 1,
                    'completed': False,
                    'typeID': new_order['typeID'],
                    'volEntered': int(new_order['volEntered']),
                    'price': float(old_order['price'])
                })

                #print('order partially fulfilled: %s; %s -> %s' % (id, oldVolRem, newVolRem))

        for id in difference:
            old_order = next((x for x in old_orders if x['orderID'] == id), None)

            oldVolRem = int(old_order['volRemaining'])

            if _type == 0:
                order = await self.getCharacterOrderById(old_order['orderID'], user_id, entity_id, entity_name, eve_key, vcode)
            else:
                order = await self.getCorporationOrderById(old_order['orderID'], user_id, wallet_key, entity_id, entity_name, eve_key, vcode)

            if len(order) != 0:
                order = order[0]
            else:
                order = None

            if order != None and order['orderState'] == '2':

                changed_orders.append({
                    'change': oldVolRem,
                    'orderID': id,
                    'whoID': entity_id,
                    'whoName': entity_name,
                    'previous': oldVolRem,
                    'remaining': 0,
                    'bid': int(old_order['bid']) == 1,
                    'completed': True,
                    'typeID': old_order['typeID'],
                    'volEntered': int(old_order['volEntered']),
                    'price': float(old_order['price'])
                })

        return changed_orders

    async def aggregateProfit(self):

        if not settings.is_hourly:
            return

        print("Aggregating profits")

        profit_start = time.perf_counter()
        transactions = []
        user_settings = await db.GetAllUserSettings()
        user_settings_arr = user_settings.values()
        user_settings_count = len(user_settings_arr)

        for user in user_settings_arr:

            user_id = user['user_id']
            profiles_calculated = 0
            assets_data = {
                'list': [],
                'total_value': 0,
                'user_id': user_id
            }

            old_orders = await self.clearUserOrders(user_id)
            changed_orders = []

            await db.user_assets.remove({'user_id': user_id})

            for profile in user.get('profiles', []):

                _type = profile['type']

                if _type == 1 and user['premium'] == False:
                    continue

                if profiles_calculated >= 5 and user['premium'] == False:
                    break

                char_id = profile['character_id']
                corp_id = profile['corporation_id']
                entity_name = profile['character_name'] if _type == 0 else profile['corporation_name']
                eve_key = profile['key_id']
                vcode = profile['vcode']
                wallet_key = profile['wallet_key']
                error = profile.get('error', None)
                profiles_calculated += 1

                verified = await self.verifyKey(user_id, char_id, corp_id, _type, eve_key, vcode, error)

                if verified == False:
                    continue

                if _type == 0:

                    await self.loadCharacterAssets(user_id, char_id, entity_name, eve_key, vcode, assets_data)

                    await self.gatherCharacterProfitData(user_id, char_id, entity_name, eve_key, vcode)
                    await self.loadCharacterOrders(user_id, char_id, entity_name, eve_key, vcode)

                    wallet_balance = await self.getCharacterBalance(user_id, char_id, eve_key, vcode)
                else:
                    await self.loadCorporationAssets(user_id, wallet_key, corp_id, entity_name, eve_key, vcode, assets_data)

                    await self.gatherCorporationProfitData(user_id, corp_id, wallet_key, entity_name, eve_key, vcode)
                    await self.loadCorporationOrders(user_id, wallet_key, corp_id, entity_name, eve_key, vcode)

                    wallet_balance = await self.getCorporationBalance(user_id, wallet_key, eve_key, vcode)

                await self.updateWalletBalance(user_id, profile['id'], wallet_balance)

                changed_orders.extend(await self.checkUserOrders(old_orders, _type, user_id, char_id if _type == 0 else corp_id, entity_name, eve_key, vcode))

            await alerts.check_sales_alerts(user_id, changed_orders)

            transactions.extend(await self.updateTopItems(user_id))

            await self.updateAllTime(user_id)

            try:
                await db.user_assets.insert(assets_data)
            except:
                print("There was a problem updating assets for user %s" % user_id)
                traceback.print_exc()

        if len(transactions) > 0:
            await db.profit_transactions.insert(transactions)

        await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.post,
                                                                               'http://' + publish_url + '/publish/profit', timeout=5))
        await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.post,
                                                                               'http://' + publish_url + '/publish/settings', timeout=5))

        print("Profits aggregated in %s seconds" % (time.perf_counter() - profit_start))
        print("Average time was %s seconds per user for %s total users" % ((time.perf_counter() - profit_start) / user_settings_count, user_settings_count))


if __name__ == "__main__":

    loop = asyncio.get_event_loop()

    print("Running at %s" % settings.utcnow)
    script_start = time.perf_counter()

    loop.run_until_complete(ProfitAggregator().aggregateProfit())

    print("Finished in %s seconds" % (time.perf_counter() - script_start))

    loop.close()
