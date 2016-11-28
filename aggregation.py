import asyncio
from typing import List, Dict
import requests
import json
import time
import functools
from datetime import datetime, timedelta
from motor import motor_asyncio
from pymongo import DESCENDING
from bson.objectid import ObjectId
import traceback
import redis
import math

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

# Use to override timed operations
development_mode = False
publish_url = 'localhost:4501'
premiumCost = 150000000
api_access_cost = 150000000

override_basePrice = {
    11567: 85000000000,
    3764: 85000000000,
    671: 85000000000,
    23773: 85000000000,
    42126: 350000000000,
    23919: 21000000000,
    23917: 24000000000,
    23913: 25000000000,
    22852: 25000000000,
    3514: 100000000000
}

# Load static data
with open('sde/blueprints.js', 'r', encoding='utf-8') as f:
    blueprints = json.loads(f.read())

with open('sde/market_ids.js', 'r', encoding='utf-8') as f:
    market_ids = json.loads(f.read())

with open('sde/blueprint_basePrice.js', 'r', encoding='utf-8') as f:
    blueprints_basePrice = json.loads(f.read())

    for i in blueprints_basePrice:
        override_basePrice[int(i)] = blueprints_basePrice[i]

class Settings:
    def __init__(self):
        self._now = datetime.now()
        self._now_utc = datetime.utcnow()
        self._do_hourly = self._now.timetuple().tm_min is 0 or development_mode
        self._do_daily = True if (self._now_utc.timetuple().tm_hour == 11 and self._do_hourly) or development_mode else False
        self._workers = 2 # Number of threads to use in multi processing

        if self._do_hourly:
            print("Settings: Performing hourly tasks")
        if self._do_daily:
            print("Settings: Performing daily tasks")

    @property
    def now(self):
        return self._now

    @property
    def utcnow(self):
        return self._now_utc

    @property
    def is_hourly(self):
        return self._do_hourly

    @property
    def is_daily(self):
        return self._do_daily

    @property
    def workers(self):
        return self._workers

    @property
    def hour_offset_utc(self):
        return self._now_utc - timedelta(hours=1)

    @property
    def minute_data_prune_time(self):
        return self._now_utc - timedelta(seconds=64800) # 24 hours

    @property
    def hourly_data_prune_time(self):
        return self._now_utc - timedelta(seconds=604800) # 1 week

settings = Settings()


class DatabaseConnector:
    def __init__(self):
        self.client = motor_asyncio.AsyncIOMotorClient()
        self.database = self.client.eveexchange
        self.market_orders = self.database.orders
        self.aggregates_minutes = self.database.aggregates_minutes
        self.aggregates_hourly = self.database.aggregates_hourly
        self.aggregates_daily = self.database.aggregates_daily
        self.portfolios = self.database.portfolios
        self.settings = self.database.settings
        self.profit_all_time = self.database.profit_alltime
        self.profit_top_items = self.database.profit_top_items
        self.profit_transactions = self.database.profit_transactions
        self.profit_chart = self.database.profit_chart
        self.user_orders = self.database.user_orders
        self.subscription = self.database.subscription
        self.notifications = self.database.notifications
        self.audit = self.database.audit_log

        self.settings_cache = None

    async def GetAllUserSettings(self) -> Dict:
        if self.settings_cache is not None:
            return self.settings_cache

        self.settings_cache = {}

        # Build a user id -> settings document map for easy access on demand
        async for user in self.settings.find():

            # Sanity check
            if 'user_id' not in user:
                continue

            self.settings_cache[user['user_id']] = user

        return self.settings_cache

db = DatabaseConnector()


class Utilities:
    def __init__(self):
        pass

    @staticmethod
    def SplitArray(_list, wanted_parts=1):
        length = len(_list)
        return [_list[i * length // wanted_parts: (i + 1) * length // wanted_parts]
                for i in range(wanted_parts)]

utilities = Utilities()


class CacheInterface:
    def __init__(self):
        self._redis = None

        try:
            self._redis = redis.StrictRedis(host='localhost', port=6379, db=0)
            print("Redis server is available")
        except:
            print("Redis server is unavailable")
            self._redis = None

    @property
    def redis(self):
        return self._redis

    def RedisAvailable(self):
        if self._redis == None:
            return False
        return True

    async def PurgeStaleMarketData(self):

        print("Purging stale market data")
        purgeTimer = time.perf_counter()

        await asyncio.gather(*[
            db.aggregates_minutes.remove({'time': {'$lte': settings.minute_data_prune_time}}),
            db.aggregates_hourly.remove({'time': {'$lte': settings.hourly_data_prune_time}})
        ])

        print("Stale market data purged in %s seconds" % (time.perf_counter() - purgeTimer))

    async def LoadDailyRedisCache(self):
        if not self.RedisAvailable():
            print("Skipping daily redis cache load since redis is unavailable")

        '''
        try:
            if self._redis.exists("dly:28668") == True:
                return
        except:
            print("Failed to load daily cache as Redis is unavailable")
            return
        '''

        print("Loading daily documents into redis cache")

        loadTimer = time.perf_counter()
        dailyDocs = {}

        async for doc in db.aggregates_daily.find():
            if doc['type'] in dailyDocs:
                if doc['time'] > dailyDocs[doc['type']]['time']:
                    dailyDocs[doc['type']] = doc

            else:
                dailyDocs[doc['type']] = doc

        for k in dailyDocs:
            for reg in dailyDocs[k]['regions']:
                self._redis.hmset('dly:' + str(k) + '-' + str(reg['region']), {**reg, **{'type': k}})

        print("Loaded %s daily documents into cache in %s seconds" % (len(dailyDocs), time.perf_counter() - loadTimer))

    async def LoadCurrentRedisCache(self, aggregates):
        if not self.RedisAvailable():
            print("Skipping current redis cache load since redis is unavailable")

        loadTimer = time.perf_counter()

        try:
            # Keys use both the type ID and region for efficiency in retrieval, and to avoid nesting
            for v in aggregates:
                for reg in v['regions']:
                    self._redis.hmset('cur:' + str(v['type']) + '-' + str(reg['region']), {**reg, **{'type': v['type']}})
        except:
            traceback.print_exc()
            print("Failed to update current redis cache")

        print("Loaded current aggregates into cache in %s seconds" % (time.perf_counter() - loadTimer))

cache = CacheInterface()


class DeepstreamPublisher():
    def __init__(self):
        pass

    async def PublishOrders(self):
        print("Publishing market orders")

        try:
            await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.post,
                                                           'http://' + publish_url + '/publish/market/orders', timeout=5))
        except:
            print("Error while publishing orders")
        print("Market orders published")

    async def PublishMinuteAggregates(self):
        print("Publishing minute aggregates")

        try:
            await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.post,
                                                           'http://' + publish_url + '/publish/market/minutes', timeout=5))
        except:
            print("Error while publishing minute aggregates")
        print("Minute aggregates published")


    async def PublishHourlyAggregates(self):
        print("Publishing hourly aggregates")

        try:
            await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.post,
                                                           'http://' + publish_url + '/publish/market/hourly', timeout=5))
        except:
            print("Error while publishing hourly aggregates")
        print("Hourly aggregates published")

    async def PublishDailyAggregates(self):
        print("Publishing daily aggregates")

        try:
            await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.post,
                                                           'http://' + publish_url + '/publish/market/daily', timeout=5))
        except:
            print("Error while publishing daily aggregates")
        print("Daily aggregates published")


class OrderInterface:
    def __init__(self) -> None:

        self._old_volume = {}
        self._new_volume = {}
        self._volume_changes = None  # Region -> Type -> Trade Volume
        self._order_ids = []
        self._orders = []
        self._page_count = None
        self._existing_orders = None  # Orders in the database at the time this script started
        self._persisted_orders = None  # Orders in the database after new/changed orders are persisted
        self._deleted_orders = None  # Order ids that are purged from database
        self._failures = 0 # Count failed market pulls and skip certain tasks if not 0

    @property
    def regions(self):
        return [10000002, 10000043, 10000032, 10000042, 10000030]  # Forge (Jita), Domain (Amarr), Sinq (Dodixie), Hek, Rens

    @property
    def regionToStationHub(self, region):
        return {
            10000002: 60003760,
            10000043: 60008494,
            10000032: 60011794,
            10000042: 60005686,
            10000030: 60004588
        }.get(region, 0)

    @property
    def hubs(self):
        return [60003760, 60008494, 60011794, 60005686, 60004588]

    async def LoadAllOrders(self) -> None:

        print("Loading all market orders")
        loadTimer = time.perf_counter()

        for region in self.regions:

            # Reset page count for this batch of orders
            self._page_count = None

            # First page load should set the page count for this order batch
            await self.LoadPage(region, 1)

            if self._page_count is None:
                print("Failed to begin loading orders for region %s" % region)
                continue

            if self._page_count == 0:
                continue

            # Initiate all the page loads async and wait for them
            await asyncio.gather(*[self.LoadPage(region, i) for i in range(2, self._page_count+1)])

            print("Loaded region %s" % region)

        print("%s Market orders loaded in %s seconds" % (len(self._orders), time.perf_counter() - loadTimer))

    async def LoadPage(self, region: int = 10000002, page: int = 1) -> None:

        # The executor will run the request in a dedicated thread
        try:
            req = await asyncio.get_event_loop().run_in_executor(None, requests.get, "https://crest-tq.eveonline.com/market/%s/orders/all/?page=%s" % (region, page))

            js = req.json()
        except:
            print("Failed to load page %s for region %s" % (page, region))
            self._failures += 1
            traceback.print_exc()
            return

        if 'items' not in js:
            if self._page_count is None:
                print("Failed initial load for region %s" % region)
                self._page_count = 0
            print("Failed to load page %s" % page)
            self._failures += 1
            return
            
        orders = [{'price': k['price'], 'region': region, 'type': k['type'], 'volume': k['volume'], 'buy': k['buy'],
                   'time': k['issued'], 'id': k['id'], 'stationID': k['stationID']}
                  for k in js['items'] if (k['stationID'] in self.hubs or k['stationID'] >= 1000000000000)]

        if self._page_count is None:
            self._page_count = js['pageCount']

        self._orders.extend(orders)
        self._order_ids.extend([k['id'] for k in orders])

    async def PersistOrders(self) -> None:

        if len(self._orders) == 0:
            print("No orders to persist")
            return

        print("Persisting market orders to database")
        persistTimer = time.perf_counter()

        # Load up market orders currently in the database
        self._existing_orders = await self.GetPersistedOrders()

        # Hold the futures for the jobs to be executed
        ops = []

        # Split the orders that need to be inserted into the databases evenly between
        # the number of workers that can be used
        for job in utilities.SplitArray(self._orders, settings.workers):

            bulk_op = db.market_orders.initialize_unordered_bulk_op()

            for order in job:
                bulk_op.find({'id': order['id']}).upsert().replace_one(order)

            # execute() will return a future to wait on
            ops.append(bulk_op.execute())

        # Wait for all the bulk operations to complete
        await asyncio.gather(*ops)

        # Clean up temporary orders
        self._orders = []

        # Load up all the new order data
        self._persisted_orders = await self.GetPersistedOrders()

        # Diff order ids in the database vs the orders returned from the API to detect stale orders
        persisted_ids = {v['id'] for v in self._persisted_orders}
        new_ids = set(self._order_ids)

        self._deleted_orders = list(persisted_ids.difference(new_ids))

        # Finally, delete all the stale orders
        if self._failures is 0:
            if len(self._deleted_orders) > 0:
                print("Deleting %s stale orders" % len(self._deleted_orders))
                db.market_orders.remove({"id": {"$in": self._deleted_orders}})
        else:
            print("Skipping stale order deletion due to previous page load failures")

        print("Persisted market orders in %s seconds" % (time.perf_counter() - persistTimer))

    # Loads market orders from the database and returns as a list
    @staticmethod
    async def GetPersistedOrders() -> List[Dict]:

        return await db.market_orders.find({}).to_list(length=None)

    def GetVolumeChanges(self) -> Dict:

        if self._existing_orders is None or self._persisted_orders is None:
            return {}

        if self._volume_changes is not None:
            return self._volume_changes

        vol_timer = time.perf_counter()

        self._volume_changes = {}

        # Helper function
        filter_by_region = lambda _list, _region: filter(lambda doc: doc['region'] == _region, _list)

        for region in self.regions:

            self._volume_changes[region] = {}

            # Create a map for order id -> volume lookup
            exist_orders_volume = {}
            exist_orders_type = {}
            exist_orders_buy = {}

            for i in filter_by_region(self._existing_orders, region):
                exist_orders_volume[i['id']] = i['volume']
                exist_orders_type[i['id']] = i['type']
                exist_orders_buy[i['id']] = i['buy']

            # Calculate change in volume
            for i in filter_by_region(self._persisted_orders, region):

                if i['id'] not in exist_orders_volume:
                    continue

                # Compare new data volume to previous dataset
                diff = i['volume'] - exist_orders_volume[i['id']]

                if diff <= 0:
                    continue

                self._volume_changes[region][i['type']] = diff

            # Consider volume from deleted orders
            if len(self._deleted_orders) > 0:

                type_to_avg_volume = {}
                type_to_total_buy_volume = {}
                type_to_total_sell_volume = {}

                # Pre-compute some basic data
                for i in filter_by_region(self._existing_orders, region):

                    _type = i['type']

                    if _type not in type_to_avg_volume:
                        type_to_avg_volume[_type] = [i['volume']]
                    else:
                        type_to_avg_volume[_type].append(i['volume'])

                    if i['buy']:
                        if _type not in type_to_total_buy_volume:
                            type_to_total_buy_volume[_type] = i['volume']
                        else:
                            type_to_total_buy_volume[_type] += i['volume']
                    else:
                        if _type not in type_to_total_sell_volume:
                            type_to_total_sell_volume[_type] = i['volume']
                        else:
                            type_to_total_sell_volume[_type] += i['volume']

                # Average out the volumes
                for i in type_to_avg_volume:
                    type_to_avg_volume[i] = sum(type_to_avg_volume[i]) / len(type_to_avg_volume[i])

                # For each deleted order, check for anomalous volumes and add to volume changes
                for i in self._deleted_orders:

                    # If this order id is not in the map, then its not part of the current region
                    if i not in exist_orders_type:
                        continue

                    _type = exist_orders_type[i]

                    # Crude but should be effective enough for some items
                    if _type == 29668 and exist_orders_volume[i] > 25:
                        continue
                    if _type == 40520 and exist_orders_volume[i] > 50:
                        continue

                    # If the volume of this order exceeds the entirety of the rest of the order volume for this item,
                    # then its an outcast
                    if exist_orders_volume[i] > 10: # Ignore insignificant amounts
                        if exist_orders_buy[i]:
                            _check = type_to_total_buy_volume[_type] - exist_orders_volume[i]
                            if _check > 1:
                                if exist_orders_volume[i] > _check:
                                    print("Order % of item %s exceeds realistic buy volume %s,%s" % (
                                        i, _type, exist_orders_volume[i],
                                        type_to_total_buy_volume[_type] - exist_orders_volume[i]))
                                    continue
                        else:
                            _check = type_to_total_sell_volume[_type] - exist_orders_volume[i]
                            if _check > 1:
                                if exist_orders_volume[i] > _check:
                                    print("Order % of item %s exceeds realistic sell volume %s,%s" % (
                                        i, _type, exist_orders_volume[i],
                                        type_to_total_sell_volume[_type] - exist_orders_volume[i]))
                                    continue

                    if exist_orders_volume[i] > type_to_avg_volume[_type] * 100:
                        print("Order % of item %s exceeds realistic avg %s,%s" % (
                            i, _type, exist_orders_volume[i], type_to_avg_volume[_type]))
                        continue

                    if _type not in self._volume_changes[region]:
                        self._volume_changes[region][_type] = exist_orders_volume[i]
                    else:
                        self._volume_changes[region][_type] += exist_orders_volume[i]

            print("%s volume changes for region %s" % (len(self._volume_changes[region]), region))

        # Clean up orders that were pulled from DB for this task
        self._deleted_orders = []
        self._existing_orders = []

        # Persisted orders will be used in portfolio aggregation to get top order prices
        #self._persisted_orders = []

        print("Computed volume changes in %s seconds" % (time.perf_counter() - vol_timer))

        return self._volume_changes

    @property
    def orders(self) -> List[Dict]:
        return self._orders

    @orders.setter
    def orders(self, value: List = list()) -> None:
        self._orders = value

    @property
    def order_ids(self) -> List[int]:
        return self._order_ids

    @order_ids.setter
    def order_ids(self) -> None:
        pass

    @property
    def volume_changes(self) -> List[Dict]:
        return self.GetVolumeChanges()

    @volume_changes.setter
    def volume_changes(self) -> None:
        pass


class OrderAggregator:
    def __init__(self, interface, deepstream):

        self._interface = interface
        self._deepstream = deepstream
        self._aggregates_minutes = None
        self._aggregates_hourly = None
        self._aggregates_daily = None
        self._aggregates_daily_sma = None

    async def AggregateAll(self) -> None:

        await self.AggregateMinutes()

        if settings.is_hourly:
            await self.AggregateHourly()
            await self._deepstream.PublishHourlyAggregates()

        if settings.is_daily:
            await self.AggregateDaily()
            await self._deepstream.PublishDailyAggregates()
            await cache.LoadDailyRedisCache()


        await asyncio.gather(*[
            cache.LoadCurrentRedisCache(self._aggregates_minutes),
            self._deepstream.PublishMinuteAggregates()
        ])

    async def DoThreadedInsert(self, collection, data) -> None:

        ops = []

        for _data in utilities.SplitArray(data, settings.workers):

            ops.append(collection.insert(_data))

        await asyncio.gather(*ops)

    async def AggregateMinutes(self) -> None:

        agg_timer = time.perf_counter()

        print("Aggregating minute data")

        pipeline = [
            {
                '$project': {
                    'region': 1,
                    'buy': 1,
                    'volume': 1,
                    'price': 1,
                    'type': 1
                }
            },
            {
                '$sort': {
                    'price': -1
                }
            },
            {
                '$group': {
                    '_id': { "region": "$region", "type": "$type" },
                    'buyVolume': {'$sum': {'$cond': [{"$eq": [ "$buy", True ]}, '$volume', 0 ]} },
                    'sellVolume': {'$sum': {'$cond': [{"$eq": [ "$buy", False ]}, '$volume', 0 ]} },
                    'sellAvg': {'$sum': {'$cond': [{"$eq": [ "$buy", False ]}, '$price', 0 ]} },
                    'buyAvg': {'$sum': {'$cond': [{"$eq": [ "$buy", True ]}, '$price', 0 ]} },
                    'buyCount': {'$sum': {'$cond': [{"$eq": ["$buy", True]}, 1, 0]}},
                    'sellCount': {'$sum': {'$cond': [{"$eq": ["$buy", False]}, 1, 0]}},
                    'sellMin': {'$min': {'$cond': [{"$eq": ["$buy", False]}, '$price', None]}},
                    'sellMax': {'$max': {'$cond': [{"$eq": ["$buy", False]}, '$price', None]}},
                    'buyMin': {'$min': {'$cond': [{"$eq": ["$buy", True]}, '$price', None]}},
                    'buyMax': {'$max': {'$cond': [{"$eq": ["$buy", True]}, '$price', None]}},
                    'buyPercentile': { '$push': {'$cond': [{'$and':[{"$eq": ["$buy", True]}]}, '$price', None]} },
                    'sellPercentile': {'$push': {'$cond': [{"$eq": ["$buy", False]}, '$price', None]}}
                }
            },
            {
                '$project': {
                    'buyMin': { '$ifNull': ['$buyMin', 0]},
                    'sellMin': { '$ifNull': ['$sellMin', 0]},
                    'buyMax': { '$ifNull': ['$buyMax', 0]},
                    'sellMax': { '$ifNull': ['$sellMax', 0]},
                    'buyVolume': 1,
                    'sellVolume': 1,
                    'buyAvg': { '$cond': [{'$gt': ['$buyCount', 1]}, {'$divide': ['$buyAvg','$buyCount']}, 0 ] },
                    'sellAvg': {'$cond': [{'$gt': ['$sellCount', 1]}, {'$divide': ['$sellAvg', '$sellCount']}, 0]},
                    'buyPercentile': {'$avg': {'$slice': [{'$filter':{'input':'$buyPercentile', 'as':'arr','cond':{'$gt':['$$arr', 0]}}}, {'$trunc':{'$max': [1, {'$multiply':[0.05, '$buyCount']}]}}]}},
                    'sellPercentile': {'$avg': {'$slice': [{'$filter':{'input':'$sellPercentile', 'as':'arr','cond':{'$gt':['$$arr', 0]}}}, {'$cond':[{'$eq': ['$sellCount', 0]}, 0, {'$floor': {'$max': [-1, {'$multiply': [-0.05, '$sellCount']}]}}]}]}},
                }
            },
            {
                '$project': {
                    'buyMin': 1,
                    'sellMin': 1,
                    'buyMax': 1,
                    'sellMax': 1,
                    'buyVolume': 1,
                    'sellVolume': 1,
                    'buyAvg': 1,
                    'sellAvg': 1,
                    'buyPercentile': { '$ifNull': ['$buyPercentile', 0]},
                    'sellPercentile': { '$ifNull': ['$sellPercentile', 0]},
                    'spread': { '$cond': [{'$and':[{'$gt': ['$sellPercentile',0]},{'$gt': ['$buyPercentile',0]}]}, { '$subtract': [100, {'$multiply': [{'$divide': ['$buyPercentile', '$sellPercentile']}, 100 ]}] }, 0 ]}
                }
            },
            {
                '$project': {
                    'buyMin': 1,
                    'sellMin': 1,
                    'buyMax': 1,
                    'sellMax': 1,
                    'buyVolume': 1,
                    'sellVolume': 1,
                    'buyAvg': 1,
                    'sellAvg': 1,
                    'buyPercentile': 1,
                    'sellPercentile': 1,
                    'spread': 1,
                    'spreadValue': { '$multiply': [{'$divide': ['$spread', 100]}, '$sellPercentile']}
                }
            }
        ]

        accumulator = {}

        async for i in db.market_orders.aggregate(pipeline, allowDiskUse=True):
            _type = i['_id']['type']
            region = i['_id']['region']
            tradeVolume = 0

            if region in self._interface.GetVolumeChanges():
                if _type in self._interface.GetVolumeChanges()[region]:
                    tradeVolume = self._interface.GetVolumeChanges()[region][_type]

            override = {'buyPercentile': override_basePrice[_type], 'sellPercentile': override_basePrice[_type], 'spread': 0} if _type in override_basePrice else {}

            if _type not in accumulator:
                accumulator[_type] = {
                    'time': settings.utcnow,
                    'type': _type,
                    'regions': [
                        {
                            'region': region,
                            'tradeVolume': tradeVolume,
                            'tradeValue': tradeVolume * i['spreadValue'],
                            **{key:value for key, value in i.items() if key not in {'_id'}},
                            **override
                        }
                    ]
                }
            else:
                accumulator[i['_id']['type']]['regions'].append(
                    {
                        'region': i['_id']['region'],
                        'tradeVolume': tradeVolume,
                        'tradeValue': tradeVolume * i['spreadValue'],
                        **{key: value for key, value in i.items() if key not in {'_id'}},
                        **override
                    }
                )

        self._aggregates_minutes = list(accumulator.values())

        await self.DoThreadedInsert(db.aggregates_minutes, self._aggregates_minutes)

        print("Minute data finished in %s seconds" % (time.perf_counter() - agg_timer))

    async def AggregateHourly(self) -> None:

        agg_timer = time.perf_counter()

        print("Aggregating hourly data")

        pipeline = [
            {
                '$match': {
                    'time': {
                        '$gte': settings.utcnow - timedelta(hours=1, minutes=2)  # 1 day + slight buffer
                    }
                }
            },
            {
                '$unwind': "$regions"
            },
            {
                '$group': {
                    '_id': {"region": "$regions.region", "type": "$type"},
                    'buyPercentile': { '$avg': '$regions.buyPercentile' },
                    'sellPercentile': {'$avg': '$regions.sellPercentile'},
                    'buyAvg': {'$avg': '$regions.buyAvg'},
                    'sellAvg': {'$avg': '$regions.sellAvg'},
                    'buyMax': {'$max': '$regions.buyMax'},
                    'sellMax': {'$max': '$regions.sellMax'},
                    'buyMin': {'$min': '$regions.buyMin'},
                    'sellMin': {'$min': '$regions.sellMin'},
                    'spread': {'$avg': '$regions.spread'},
                    'spreadValue': {'$avg': '$regions.spreadValue'},
                    'tradeValue': {'$avg': '$regions.tradeValue'},
                    'tradeVolume': {'$sum': '$regions.tradeVolume'}
                }
            }
        ]

        accumulator = {}

        async for i in db.aggregates_minutes.aggregate(pipeline, allowDiskUse=True):
            _type = i['_id']['type']
            region = i['_id']['region']

            if _type not in accumulator:
                accumulator[_type] = {
                    'time': settings.utcnow,
                    'type': _type,
                    'regions': [
                        {
                            'region': region,
                            **{key:value for key, value in i.items() if key not in {'_id'}}

                        }
                    ]
                }
            else:
                accumulator[i['_id']['type']]['regions'].append(
                    {
                        'region': i['_id']['region'],
                        **{key: value for key, value in i.items() if key not in {'_id'}}
                    }
                )

        self._aggregates_hourly = list(accumulator.values())

        await self.DoThreadedInsert(db.aggregates_hourly, self._aggregates_hourly)

        print("Hourly data finished in %s seconds" % (time.perf_counter() - agg_timer))

    async def AggregateDaily(self) -> None:

        agg_timer = time.perf_counter()

        print("Aggregating daily data")

        # Pre-compute daily SMA values to use in daily aggregation that follows
        pipeline = [
            {
                '$match': {
                    'time': {
                        '$gte': settings.utcnow - timedelta(days=7, minutes=2) # 1 day + slight buffer
                    }
                }
            },
            {
                '$project': {
                    'type': 1,
                    'regions.region': 1,
                    'regions.spread': 1,
                    'regions.tradeVolume': 1
                },
            },
            {
                '$unwind': "$regions"
            },
            {
                '$group': {
                    '_id': {"region": "$regions.region", "type": "$type"},
                    'spread': {'$avg': '$regions.spread'},
                    'tradeVolume': {'$avg': '$regions.tradeVolume'}
                }
            }
        ]

        self._aggregates_daily_sma = {}

        async for i in db.aggregates_daily.aggregate(pipeline, allowDiskUse=True):
            _type = i['_id']['type']
            region = i['_id']['region']

            if _type not in self._aggregates_daily_sma:
                self._aggregates_daily_sma[_type] = {}

            self._aggregates_daily_sma[_type][region] = {key: value for key, value in i.items() if key not in {'_id'}}

        pipeline = [
            {
                '$match': {
                    'time': {
                        '$gte': settings.utcnow - timedelta(hours=24, minutes=2)  # 1 day + slight buffer
                    }
                }
            },
            {
                '$unwind': "$regions"
            },
            {
                '$group': {
                    '_id': {"region": "$regions.region", "type": "$type"},
                    'buyPercentile': {'$avg': '$regions.buyPercentile'},
                    'sellPercentile': {'$avg': '$regions.sellPercentile'},
                    'buyAvg': {'$avg': '$regions.buyAvg'},
                    'sellAvg': {'$avg': '$regions.sellAvg'},
                    'buyMax': {'$max': '$regions.buyMax'},
                    'sellMax': {'$max': '$regions.sellMax'},
                    'buyMin': {'$min': '$regions.buyMin'},
                    'sellMin': {'$min': '$regions.sellMin'},
                    'spread': {'$avg': '$regions.spread'},
                    'spreadValue': {'$avg': '$regions.spreadValue'},
                    'tradeValue': {'$avg': '$regions.tradeValue'},
                    'tradeVolume': {'$sum': '$regions.tradeVolume'}
                }
            }
        ]

        accumulator = {}

        async for i in db.aggregates_hourly.aggregate(pipeline, allowDiskUse=True):
            _type = i['_id']['type']
            region = i['_id']['region']
            spread_sma = 0
            volume_sma = 0

            if _type in self._aggregates_daily_sma:
                if region in self._aggregates_daily_sma[_type]:
                    spread_sma = self._aggregates_daily_sma[_type][region]['spread']
                    volume_sma = self._aggregates_daily_sma[_type][region]['tradeVolume']

            if _type not in accumulator:
                accumulator[_type] = {
                    'time': settings.utcnow,
                    'type': _type,
                    'regions': [
                        {
                            'region': region,
                            'spread_sma': spread_sma,
                            'volume_sma': volume_sma,
                            **{key: value for key, value in i.items() if key not in {'_id'}}

                        }
                    ]
                }
            else:
                accumulator[_type]['regions'].append(
                    {
                        'region': i['_id']['region'],
                        'spread_sma': spread_sma,
                        'volume_sma': volume_sma,
                        **{key: value for key, value in i.items() if key not in {'_id'}}
                    }
                )

        self._aggregates_daily = list(accumulator.values())

        await self.DoThreadedInsert(db.aggregates_daily, self._aggregates_daily)

        print("Daily data finished in %s seconds" % (time.perf_counter() - agg_timer))

    @property
    def aggregates_minutes(self) -> List[Dict]:
        return self._aggregates_minutes

    @property
    def aggregates_hourly(self) -> List[Dict]:
        return self._aggregates_hourly

    @property
    def aggregates_daily(self) -> List[Dict]:
        return self._aggregates_daily

orderInterface = OrderInterface()

class MarketAggregator:
    def __init__(self):
        self._order_interface = orderInterface
        self._deepstream = DeepstreamPublisher()
        self._order_aggregator = OrderAggregator(self._order_interface, self._deepstream)
        self._timer = time.perf_counter()

    async def StartAggregation(self) -> None:

        # Start low priority long running tasks
        marketPurgeTask = cache.PurgeStaleMarketData()

        # Core tasks that need to be run sequentially
        await self._order_interface.LoadAllOrders()

        publishOrdersTask = self._deepstream.PublishOrders()

        await self._order_interface.PersistOrders()

        await self._order_aggregator.AggregateAll()

        # Wait on side tasks to complete
        await asyncio.gather(*[
            marketPurgeTask,
            publishOrdersTask
        ])

        print("Aggregation finished in %s seconds" % (time.perf_counter() - self._timer))


class PortfolioAggregator:
    def __init__(self):
        self.adjusted_prices = None
        self.simulation_cache = {}
        pass

    def getAdjustedPrices(self):
        if self.adjusted_prices is None:
            self.adjusted_prices = {}

            try:
                req = requests.get('https://esi.tech.ccp.is/latest/markets/prices/?datasource=tranquility')

                prices = req.json()

                for item in prices:
                    self.adjusted_prices[item['type_id']] = item['adjusted_price']
            except:
                print("Failed to load adjusted prices from ESI API")

        return self.adjusted_prices

    def _getMaterialsFromComponent(self, component):
        _materials = []

        if str(component['typeID']) in blueprints:

            mats = blueprints[str(component['typeID'])]['materials']

            for mat in mats:
                _materials.extend(self._getMaterialsFromComponent(mat))

        else:
            _materials.append(component)

        return _materials


    def getMaterialsFromComponent(self, component):
        if str(component['typeID']) not in blueprints:
            return []

        return self._getMaterialsFromComponent(component)

    async def PublishPortfolios(self):
        print("Publishing portfolios")

        try:
            await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.post,
                                                                                   'http://' + publish_url + '/publish/portfolios', timeout=5))
        except:
            print("Error while publishing portfolios")
        print("Portfolios published")

    def doSimulateTrade(self, _type, quantity, _buy_price, _sell_price, simulation_settings, region):

        buy_price = _buy_price
        sell_price = _sell_price

        if simulation_settings['strategy'] == 0:

            if _type in self.simulation_cache:
                cached = self.simulation_cache[_type]

                buy_price = cached['buy_price']
                sell_price = cached['sell_price']
            else:

                orders = [order for order in orderInterface._persisted_orders if order['type'] == _type and order['region'] == region]

                buy_orders = [order['price'] for order in orders if order['buy'] == True]
                sell_orders = [order['price'] for order in orders if order['buy'] == False]

                buy_price = max(buy_orders) if len(buy_orders) > 0 else _buy_price
                sell_price = min(sell_orders) if len(sell_orders) > 0 else _sell_price

                self.simulation_cache[_type] = {
                    'buy_price': buy_price,
                    'sell_price': sell_price
                }

        buy_price = buy_price * quantity
        sell_price = sell_price * quantity

        if simulation_settings['margin'] > 0:
            if simulation_settings['margin_type'] == 0:

                buy_price = buy_price + simulation_settings['margin']
                sell_price = sell_price - simulation_settings['margin']

            else:
                buy_price = buy_price + buy_price * simulation_settings['margin'] / 100
                sell_price = sell_price - sell_price * simulation_settings['margin'] / 100

        broker = buy_price * simulation_settings['broker_fee'] / 100 if simulation_settings['broker_fee'] > 0 else 0
        tax = sell_price * simulation_settings['sales_tax'] / 100 if simulation_settings['sales_tax'] > 0 else 0

        profit = sell_price - buy_price - tax - broker

        if simulation_settings['wanted_margin'] > 0:

            multiplier = simulation_settings['wanted_margin'] / 100

            wanted_profit = (buy_price + tax + broker + simulation_settings['overhead']) * multiplier

            profit = wanted_profit
            sell_price = buy_price + wanted_profit

        return {
            'buy': buy_price,
            'sell': sell_price,
            'tax': tax,
            'broker': broker,
            'profit': profit
        }

    async def aggregatePortfolios(self):
        start = time.perf_counter()
        systemIndex = 0.01
        taxRate = 0.10
        user_settings = await db.GetAllUserSettings()
        adjustedPrices = self.getAdjustedPrices()

        if not cache.RedisAvailable():
            print("Skipping portfolio aggregation since redis is unavailable")
            return

        async for doc in db.portfolios.find():

            try:
                components = []
                materials = {}
                totalSpread = 0
                totalVolume = 0
                totalMaterialCost = 0
                totalInstallCost = 0
                portfolioValue = 0
                portfolioBuyValue = 0
                hourly = doc['hourlyChart']
                daily = doc['dailyChart']
                startingValue = doc['startingValue']
                efficiency = doc['efficiency']
                region = 10000002 # default region
                user_id = doc['user_id']

                if user_id not in user_settings:
                    print("Can't retrieve user settings for user %s and portfolio %s" % (user_id, doc['portfolioID']))
                    continue

                if 'market' not in user_settings[user_id]:
                    print("Defaulting region to Jita for user %s and portfolio %s" % (user_id, doc['portfolioID']))
                elif 'region' in user_settings[user_id]['market']:
                    region = user_settings[user_id]['market']['region']

                for component in doc['components']:

                    # TODO: Verify redis data is existent and correct
                    typeID = component['typeID']
                    typeIDStr = str(int(typeID))

                    minuteData = cache.redis.hgetall('cur:' + typeIDStr + '-' + str(region))
                    dailyData = cache.redis.hgetall('dly:' + typeIDStr + '-' + str(region))

                    # Sell value for trading and buy value for industry (buying an item)
                    unitPrice = float(minuteData[b'sellPercentile']) if doc['type'] == 0 else float(minuteData[b'buyPercentile'])
                    portfolioBuyValue += float(minuteData[b'buyPercentile']) * component['quantity']
                    adjustedPrice = adjustedPrices[typeID] if typeID in adjustedPrices else unitPrice
                    totalPrice = unitPrice * component['quantity']
                    totalAdjustedPrice = adjustedPrice * component['quantity']
                    spread = float(minuteData[b'spread'])
                    volume = float(dailyData[b'tradeVolume'])
                    matCost = 0
                    buildSpread = 0
                    simulation = None

                    if doc['type'] == 1:

                        # Installation cost/tax for the main blueprint itself
                        # TODO: The install cost needs to use the base quantity of the component before the efficiency
                        totalInstallCost += totalAdjustedPrice

                        _quantity = component['quantity']

                        if typeIDStr in blueprints:
                            if blueprints[typeIDStr]['quantity'] > 1:
                                _quantity = max(1, _quantity // blueprints[typeIDStr]['quantity'])

                        mats = self.getMaterialsFromComponent(component)

                        if len(mats) == 0:
                            matCost += totalPrice

                        else:
                            for mat in mats:

                                matQuantity = math.ceil(mat['quantity'] * _quantity * ((100 - efficiency) / 100))

                                matCost += float(
                                    cache.redis.hgetall('cur:' + str(mat['typeID']) + '-' + str(region))[b'buyPercentile']) * matQuantity
                                matCost = matCost + (matCost * systemIndex) + (matCost * systemIndex * taxRate)

                                if mat['typeID'] in materials:
                                    materials[mat['typeID']] += matQuantity
                                else:
                                    materials[mat['typeID']] = matQuantity

                        totalMaterialCost += matCost

                        if matCost != 0 and totalPrice != 0:
                            buildSpread = 100 - (matCost / totalPrice) * 100
                        else:
                            buildSpread = 0

                    elif doc['type'] == 0:

                        simulation_settings = {
                            'strategy': 0,
                            'margin_type': 0,
                            'sales_tax': 0,
                            'broker_fee': 0,
                            'margin': 0,
                            'wanted_margin': 0,
                            'overhead': 0
                        }

                        if 'market' in user_settings[user_id]:

                            market_settings = user_settings[user_id]['market']

                            if 'simulation_strategy' in market_settings:
                                simulation_settings['strategy'] = market_settings['simulation_strategy']

                            if 'simulation_margin_type' in market_settings:
                                simulation_settings['margin_type'] = market_settings['simulation_margin_type']

                            if 'simulation_sales_tax' in market_settings:
                                simulation_settings['sales_tax'] = market_settings['simulation_sales_tax']

                            if 'simulation_broker_fee' in market_settings:
                                simulation_settings['broker_fee'] = market_settings['simulation_broker_fee']

                            if 'simulation_margin' in market_settings:
                                simulation_settings['margin'] = market_settings['simulation_margin']

                            if 'simulation_wanted_profit' in market_settings:
                                simulation_settings['wanted_margin'] = market_settings['simulation_wanted_profit']

                        simulation = self.doSimulateTrade(typeID, component['quantity'], float(minuteData[b'buyPercentile']), float(minuteData[b'sellPercentile']), simulation_settings, region)

                    totalSpread += spread
                    totalVolume += volume
                    portfolioValue += totalPrice

                    components.append({
                        'unitPrice': unitPrice,
                        'totalPrice': totalPrice,
                        'spread': spread,
                        'volume': volume,
                        'typeID': component['typeID'],
                        'quantity': component['quantity'],
                        'materialCost': matCost,
                        'buildSpread': buildSpread,
                        'simulation': simulation
                    })

                materials = [{'typeID': k, 'quantity': materials[k]} for k in materials]

                avgSpread = totalSpread / len(doc['components'])

                if startingValue == 0:
                    startingValue = portfolioValue

                compareValue = hourly[-1:][0]['portfolioValue'] if len(hourly) > 0 else portfolioValue

                if portfolioValue != 0:
                    growth = 100 - (compareValue / portfolioValue) * 100
                else:
                    growth = 0

                if doc['type'] == 1:
                    baseMinuteData = cache.redis.hgetall('cur:' + str(doc['industryTypeID']) + '-' + str(region))

                    if float(baseMinuteData[b'sellPercentile']) != 0:
                        industrySpread = 100 - (portfolioValue / (
                        float(baseMinuteData[b'sellPercentile']) * doc['industryQuantity'])) * 100
                    else:
                        industrySpread = 0

                    industryValue = float(baseMinuteData[b'sellPercentile']) * doc['industryQuantity']

                    totalInstallCost = (totalInstallCost * systemIndex) + (totalInstallCost * systemIndex * taxRate)

                else:
                    industrySpread = 0
                    industryValue = 0

                hourlyResult = None

                # Hourly stats
                if settings.is_hourly:

                    if doc['type'] == 0:

                        hourlyResult = {
                            'time': settings.utcnow,
                            'portfolioValue': portfolioValue,
                            'avgSpread': avgSpread,
                            'growth': growth
                        }

                        hourly.append(hourlyResult)
                    else:

                        hourlyResult = {
                            'time': settings.utcnow,
                            'portfolioValue': portfolioValue,
                            'avgSpread': avgSpread,
                            'industryValue': industryValue,
                            'industrySpread': industrySpread,
                            'profitValue': industryValue - portfolioValue,
                            'materialValue': totalMaterialCost
                        }

                        hourly.append(hourlyResult)

                if settings.is_daily:

                    if doc['type'] == 0:

                        newestHourly = hourly[:24]

                        if len(newestHourly) > 0:
                            oldest = newestHourly[0:1][0]
                            newest = newestHourly[-1:][0]

                            dailyGrowth = 100 - (oldest['portfolioValue'] / newest['portfolioValue']) * 100

                            newDaily = hourlyResult
                            newDaily['growth'] = dailyGrowth

                            daily.append(newDaily)
                    else:

                        daily.append(hourlyResult)

                await db.portfolios.find_and_modify({'_id': ObjectId(oid=doc['_id'])}, {
                    '$set': {
                        'currentValue': portfolioValue,
                        'currentBuyValue': portfolioBuyValue,
                        'averageSpread': avgSpread,
                        'components': components,
                        'industrySpread': industrySpread,
                        'industryValue': industryValue,
                        'hourlyChart': hourly[:72],
                        'dailyChart': daily[:90],
                        'materials': materials,
                        'materialCost': totalMaterialCost,
                        'startingValue': startingValue,
                        'installationCost': totalInstallCost
                    }
                })

            except:
                traceback.print_exc()
                print("Failed to update portfolio %s" % doc['portfolioID'])

        await self.PublishPortfolios()

        print("Portfolios updated in %s seconds" % (time.perf_counter() - start))


# TODO: add error checking to every single request
class ProfitAggregator:
    def __init__(self):
        self._rowCount = 1000 # Number of rows to pull from api
        self._hour_offset = settings.hour_offset_utc.utctimetuple()
        self._sales = {} # char id -> [transactions]
        self._profits = {} # char id -> profit results

    def getCharacterTransactions(self, char_id, eve_key, eve_vcode, fromID=None) -> List:

        auth = (char_id, eve_key, eve_vcode, self._rowCount, "" if fromID is None else "&fromID="+str(fromID))
        url = "https://api.eveonline.com/char/WalletTransactions.xml.aspx?characterID=%s&keyID=%s&vCode=%s&rowCount=%s%s" % auth
        req = requests.get(url)
        rows = []

        try:
            tree = ET.fromstring(req.text)

            if tree.find('error') is not None:
                print("Error while pulling character transactions")
                return []

            rows = [i for i in list(tree.find('result').find('rowset')) if i.attrib['transactionFor'] == 'personal']

        except:
            pass

        return rows

    def getCorporationTransactions(self, wallet_key, eve_key, eve_vcode, fromID=None) -> List:

        auth = (wallet_key, eve_key, eve_vcode, self._rowCount, "" if fromID is None else "&fromID="+str(fromID))
        url = "https://api.eveonline.com/corp/WalletTransactions.xml.aspx?accountKey=%s&keyID=%s&vCode=%s&rowCount=%s%s" % auth
        req = requests.get(url)
        rows = []

        try:
            tree = ET.fromstring(req.text)

            if tree.find('error') is not None:
                print("Error while pulling corporation transactions")
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

        return journal

    def getAllCorporationTransactions(self, wallet_key, eve_key, eve_vcode) -> List:

        fromID = None
        journal = self.getCorporationTransactions(wallet_key, eve_key, eve_vcode, fromID)

        if len(journal) == 0:
            return []

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

        return journal

    def getCharacterJournalEntries(self, char_id, eve_key, eve_vcode) -> List:

        auth = (char_id, eve_key, eve_vcode)
        url = "https://api.eveonline.com/char/WalletJournal.xml.aspx?characterID=%s&keyID=%s&vCode=%s&rowCount=1000" % auth
        req = requests.get(url)
        rows = []

        try:
            tree = ET.fromstring(req.text)

            if tree.find('error') is not None:
                print("Error while pulling character journal")
                return []

            rows = list(tree.find('result').find('rowset'))
        except:
            pass

        data = [{key:row.attrib[key] for key in ('amount', 'refTypeID', 'date')} for row in rows if time.strptime(row.attrib['date'], '%Y-%m-%d %H:%M:%S') > self._hour_offset]

        return data

    def getCorporationJournalEntries(self, wallet_key, eve_key, eve_vcode) -> List:

        auth = (wallet_key, eve_key, eve_vcode)
        url = "https://api.eveonline.com/corp/WalletJournal.xml.aspx?accountKey=%s&keyID=%s&vCode=%s&rowCount=1000" % auth
        req = requests.get(url)
        rows = []

        try:
            tree = ET.fromstring(req.text)

            if tree.find('error') is not None:
                print("Error while pulling corporation journal")
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
                print("No buy data for type ID %s" % typeID)
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

        # TODO: Update or omit message
        if sales is None:
            print("Failed to match sales results for user %s" % user_id)
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

        # TODO: Update or omit message
        if profit is None:
            print("Failed to match profit results for user %s" % user_id)
            return []

        # Insert the hourly chart result for this run to be consumed right after
        this_hourly_result = {
            'user_id': user_id,
            'time': settings.utcnow,
            'frequency': 'hourly',
            'profit': profit['profit'],
            'taxes': profit['taxes'],
            'broker': profit['broker']
        }

        # First load all hourly results
        all_profit_hourly = await db.profit_chart.find({'frequency': 'hourly', 'user_id': user_id}).sort('time', DESCENDING).to_list(length=None)

        # Purge anything older than 48 hours
        if len(all_profit_hourly) >= 48:

            await db.profit_chart.remove({'user_id': user_id, 'frequency': 'hourly', 'time': { '$lte': all_profit_hourly[:48][-1]['time'] }})

        # Insert new result
        await db.profit_chart.insert(this_hourly_result)

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

            await db.profit_all_time.find_and_modify({'_id': ObjectId(oid=alltime['_id'])}, {
                '$set': {
                    'day': pastDay
                },
                '$inc': {
                    'alltime.profit': profit['profit'],
                    'alltime.broker': profit['broker'],
                    'alltime.taxes': profit['taxes']
                }
            })

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

        req = await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.get, url))

        rows = []

        try:
            tree = ET.fromstring(req.text)

            if tree.find('error') is not None:
                print(tree.find('error').text)
                print("Error while pulling character orders for user %s" % user_id)
                return

            rows = [row for row in list(tree.find('result').find('rowset')) if row.attrib['orderState'] == '0']

        except:
            pass

        orders = [{**{k:row.attrib[k] for k in ('orderID', 'orderState', 'volRemaining', 'issued', 'minVolume', 'stationID', 'volEntered', 'typeID', 'bid', 'price')},
                   **{'user_id': user_id, 'who': entity_name, 'whoID': char_id}} for row in rows]

        if len(orders) == 0:
            return

        await db.user_orders.insert(orders)

    async def loadCorporationOrders(self, user_id, wallet_key, entity_id, entity_name, eve_key, eve_vcode):

        auth = (eve_key, eve_vcode)
        url = "https://api.eveonline.com/corp/MarketOrders.xml.aspx?keyID=%s&vCode=%s" % auth

        req = await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.get, url))

        rows = []

        try:
            tree = ET.fromstring(req.text)

            if tree.find('error') is not None:
                print(tree.find('error').text)
                print("Error while pulling corporation orders for user %s" % user_id)
                return

            rows = [row for row in list(tree.find('result').find('rowset')) if row.attrib['orderState'] == '0']

        except:
            pass

        orders = [{**{k:row.attrib[k] for k in ('orderID', 'orderState', 'volRemaining', 'issued', 'minVolume', 'stationID', 'volEntered', 'typeID', 'bid', 'price')},
                   **{'user_id': user_id, 'who': entity_name, 'whoID': entity_id}}
                  for row in rows if row.attrib['accountKey'] == str(wallet_key)]

        if len(orders) == 0:
            return

        await db.user_orders.insert(orders)

    async def clearUserOrders(self, user_id):
        await db.user_orders.remove({'user_id': user_id})

    async def getCorporationBalance(self, user_id, wallet_key, eve_key, eve_vcode):

        auth = (eve_key, eve_vcode)
        url = "https://api.eveonline.com/corp/AccountBalance.xml.aspx?keyID=%s&vCode=%s" % auth

        req = await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.get, url))

        try:
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

        req = await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.get, url))

        try:
            tree = ET.fromstring(req.text)

            if tree.find('error') is not None:
                print(tree.find('error').text)
                print("Error while pulling character balance for user %s" % user_id)
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

    async def aggregateProfit(self):

        if not settings.is_hourly:
            return

        print("Aggregating profits")

        profit_start = time.perf_counter()
        transactions = []
        user_settings = await db.GetAllUserSettings()

        for user in user_settings.values():

            # Check if user has been set up yet and has any API keys
            if 'profiles' not in user:
                continue

            user_id = user['user_id']
            profiles_calculated = 0

            await self.clearUserOrders(user_id)

            for profile in user['profiles']:

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
                    await self.gatherCharacterProfitData(user_id, char_id, entity_name, eve_key, vcode)
                    await self.loadCharacterOrders(user_id, char_id, entity_name, eve_key, vcode)

                    wallet_balance = await self.getCharacterBalance(user_id, char_id, eve_key, vcode)
                else:
                    await self.gatherCorporationProfitData(user_id, corp_id, wallet_key, entity_name, eve_key, vcode)
                    await self.loadCorporationOrders(user_id, wallet_key, corp_id, entity_name, eve_key, vcode)

                    wallet_balance = await self.getCorporationBalance(user_id, wallet_key, eve_key, vcode)

                await self.updateWalletBalance(user_id, profile['id'], wallet_balance)

            transactions.extend(await self.updateTopItems(user_id))

            await self.updateAllTime(user_id)

        if len(transactions) > 0:
            await db.profit_transactions.insert(transactions)

        await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.post,
                                                                               'http://' + publish_url + '/publish/profit', timeout=5))
        await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.post,
                                                                               'http://' + publish_url + '/publish/settings', timeout=5))

        print("Profits aggregated in %s seconds" % (time.perf_counter() - profit_start))

class SubscriptionUpdater:
    def __init__(self):
        pass

    async def checkExpired(self):

        user_settings = await db.GetAllUserSettings()

        sub_notification = {
            "user_id": 0,
            "time": settings.utcnow,
            "read": False,
            "message": ""
        }

        async for sub in db.subscription.find():

            user = user_settings.get(sub['user_id'], None)

            if user is None:
                print("Failed to find user settings for user %s" % sub['user_id'])
                continue

            if 'subscription_date' not in sub or 'premium' not in sub:
                continue

            if sub['subscription_date'] is None:
                continue

            if sub['premium'] == False:
                continue

            if settings.utcnow - sub['subscription_date'] > timedelta(days=30):

                renew = True
                premium = True

                sub_notification['user_id'] = sub['user_id']

                if 'general' in user:
                    if 'auto_renew' in user['general']:
                        renew = True if user['general']['auto_renew'] == True else False

                if renew == True:

                    if sub['balance'] < premiumCost:
                        premium = False

                else:
                    premium = False

                if premium == True:

                    print("Renewing subscription for user %s " % sub['user_name'])
                    sub_notification['message'] = 'Your premium subscription has been automatically renewed'

                    await db.subscription.find_and_modify({'user_id': sub['user_id']}, {
                        '$set': {
                            'subscription_date': settings.utcnow
                        },
                        '$inc': {
                            'balance': -premiumCost
                        },
                        '$push': {
                            'history': {
                                'time': settings.utcnow,
                                'type': 1,
                                'amount': premiumCost,
                                'description': 'Subscription renewal',
                                'processed': True
                            }
                        }
                    })

                    await db.audit.insert({
                        'user_id': sub['user_id'],
                        'target': 0,
                        'balance': 0,
                        'action': 4,
                        'time': settings.utcnow
                    })

                    if sub['api_access'] == True:

                        new_balance = sub['balance'] - api_access_cost

                        if api_access_cost > new_balance:

                            await db.subscription.find_and_modify({'user_id': sub['user_id']}, {
                                '$set': {
                                    'api_access': False
                                }
                            })

                            await db.settings.find_and_modify({'user_id': sub['user_id']}, {
                                '$set': {
                                    'api_access': False
                                },
                            })

                            await db.audit.insert({
                                'user_id': sub['user_id'],
                                'target': 0,
                                'balance': 0,
                                'action': 14,
                                'time': settings.utcnow
                            })

                            await db.notifications.insert({
                                "user_id": sub['user_id'],
                                "time": settings.utcnow,
                                "read": False,
                                "message": "You did not have enough remaining balance to cover the cost of API access, and it has been disabled"
                            })

                        else:
                            await db.subscription.find_and_modify({'user_id': sub['user_id']}, {
                                '$set': {
                                    'subscription_date': settings.utcnow
                                },
                                '$inc': {
                                    'balance': -api_access_cost
                                },
                                '$push': {
                                    'history': {
                                        'time': settings.utcnow,
                                        'type': 1,
                                        'amount': api_access_cost,
                                        'description': 'API access renewal',
                                        'processed': True
                                    }
                                }
                            })

                            await db.audit.insert({
                                'user_id': sub['user_id'],
                                'target': 0,
                                'balance': 0,
                                'action': 15,
                                'time': settings.utcnow
                            })

                            await db.notifications.insert({
                                "user_id": sub['user_id'],
                                "time": settings.utcnow,
                                "read": False,
                                "message": "Your api access has been automatically renewed"
                            })
                else:

                    print("Ending subscription for user %s " % sub['user_name'])
                    sub_notification['message'] = 'Your premium subscription has expired and not automatically renewed'

                    await db.subscription.find_and_modify({'user_id': sub['user_id']}, {
                        '$set': {
                            'premium': False,
                            'api_access': False,
                            'subscription_date': None
                        }
                    })

                    await db.settings.find_and_modify({'user_id': sub['user_id']}, {
                        '$set': {
                            'premium': False,
                            'api_access': False
                        },
                    })

                    await db.audit.insert({
                        'user_id': sub['user_id'],
                        'target': 0,
                        'balance': 0,
                        'action': 9,
                        'time': settings.utcnow
                    })

                    await db.audit.insert({
                        'user_id': sub['user_id'],
                        'target': 0,
                        'balance': 0,
                        'action': 14,
                        'time': settings.utcnow
                    })

                    await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.post,
                                                                                         'http://' + publish_url + '/publish/settings/%s' % sub['user_id'],
                                                                                         timeout=5))

                await db.notifications.insert(sub_notification)

                await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.post,
                                                                                       'http://' + publish_url + '/publish/notifications/%s' % sub['user_id'],
                                                                                       timeout=5))

                await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.post,
                                                                                 'http://' + publish_url + '/publish/subscription/%s' % sub['user_id'],
                                                                                 timeout=5))

                await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.post,
                                                                                 'http://' + publish_url + '/publish/audit',
                                                                                 timeout=5))

if __name__ == "__main__":

    loop = asyncio.get_event_loop()

    print("Running at %s" % settings.utcnow)

    loop.run_until_complete(MarketAggregator().StartAggregation())
    loop.run_until_complete(PortfolioAggregator().aggregatePortfolios())
    loop.run_until_complete(ProfitAggregator().aggregateProfit())
    loop.run_until_complete(SubscriptionUpdater().checkExpired())

    loop.close()