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

# Load static data
with open('sde/blueprints.js', 'r', encoding='utf-8') as f:
    blueprints = json.loads(f.read())

with open('sde/market_ids.js', 'r', encoding='utf-8') as f:
    market_ids = json.loads(f.read())

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
                self._redis.hmset('dly:' + str(k) + '-' + str(reg['region']), reg)

        print("Loaded %s daily documents into cache in %s seconds" % (len(dailyDocs), time.perf_counter() - loadTimer))

    async def LoadCurrentRedisCache(self, aggregates):
        if not self.RedisAvailable():
            print("Skipping current redis cache load since redis is unavailable")

        loadTimer = time.perf_counter()

        try:
            # Keys use both the type ID and region for efficiency in retrieval, and to avoid nesting
            for v in aggregates:
                for reg in v['regions']:
                    self._redis.hmset('cur:' + str(v['type']) + '-' + str(reg['region']), reg)
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

        orders = [{'price': k['price'], 'region': region, 'type': k['type'], 'volume': k['volume'], 'buy': k['buy'],
                   'time': k['issued'], 'id': k['id'], 'stationID': k['stationID']}
                  for k in js['items']]

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

                    # If the volume of this order exceeds the entirety of the rest of the order volume for this item,
                    # then its an outcast
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
        self._existing_orders = []
        self._persisted_orders = []

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

            if _type not in accumulator:
                accumulator[_type] = {
                    'time': settings.utcnow,
                    'type': _type,
                    'regions': [
                        {
                            'region': region,
                            'tradeVolume': tradeVolume,
                            'tradeValue': tradeVolume * i['spreadValue'],
                            **{key:value for key, value in i.items() if key not in {'_id'}}

                        }
                    ]
                }
            else:
                accumulator[i['_id']['type']]['regions'].append(
                    {
                        'region': i['_id']['region'],
                        'tradeVolume': tradeVolume,
                        'tradeValue': tradeVolume * i['spreadValue'],
                        **{key: value for key, value in i.items() if key not in {'_id'}}
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

class MarketAggregator:
    def __init__(self):
        self._order_interface = OrderInterface()
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
        pass

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

    async def aggregatePortfolios(self):
        start = time.perf_counter()
        systemIndex = 0.01
        taxRate = 0.10
        region = 10000002

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
                hourly = doc['hourlyChart']
                daily = doc['dailyChart']
                startingValue = doc['startingValue']
                efficiency = doc['efficiency']

                for component in doc['components']:

                    minuteData = cache.redis.hgetall('cur:' + str(component['typeID']) + '-' + str(region))
                    dailyData = cache.redis.hgetall('dly:' + str(component['typeID']) + '-' + str(region))

                    unitPrice = float(minuteData[b'sellPercentile'])
                    totalPrice = unitPrice * component['quantity'];
                    spread = float(minuteData[b'spread'])
                    volume = float(dailyData[b'tradeVolume'])
                    matCost = 0
                    buildSpread = 0

                    if doc['type'] == 1:

                        # Installation cost/tax for the main blueprint itself
                        totalInstallCost += (totalPrice * systemIndex) + (totalPrice * systemIndex * taxRate)

                        _quantity = component['quantity']

                        if str(component['typeID']) in blueprints:
                            if blueprints[str(component['typeID'])]['quantity'] > 1:
                                _quantity = max(1, _quantity // blueprints[str(component['typeID'])]['quantity'])

                        mats = self._getMaterialsFromComponent(component)

                        if len(mats) == 0:

                            matCost += totalPrice

                        else:
                            for mat in mats:

                                matQuantity = math.ceil(mat['quantity'] * _quantity * ((100 - efficiency) / 100))

                                matCost += float(
                                    cache.redis.hgetall('cur:' + str(mat['typeID']) + '-' + str(region))[b'sellPercentile']) * matQuantity
                                matCost = matCost + (matCost * systemIndex) + (matCost * systemIndex * taxRate)

                                if mat['typeID'] in materials:
                                    materials[mat['typeID']] += matQuantity
                                else:
                                    materials[mat['typeID']] = matQuantity

                        totalMaterialCost += matCost

                        if totalPrice != 0:
                            buildSpread = 100 - (matCost / totalPrice) * 100
                        else:
                            buildSpread = 0

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
                        'buildSpread': buildSpread
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

                else:
                    industrySpread = 0
                    industryValue = 0

                hourlyResult = None

                # Hourly stats
                if settings.is_hourly == 00:

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
                        'averageSpread': avgSpread,
                        'components': components,
                        'industrySpread': industrySpread,
                        'industryValue': industryValue,
                        'hourlyChart': hourly,
                        'dailyChart': daily,
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

    def getTransactions(self, char_id, eve_key, eve_vcode, fromID=None) -> List:

        auth = (char_id, eve_key, eve_vcode, self._rowCount, "" if fromID is None else "&fromID="+str(fromID))
        url = "https://api.eveonline.com/char/WalletTransactions.xml.aspx?characterID=%s&keyID=%s&vCode=%s&rowCount=%s%s" % auth
        req = requests.get(url)
        rows = []

        try:
            tree = ET.fromstring(req.text)

            if tree.find('error') is not None:
                print("Error while pulling transactions")
                return []

            rows = list(tree.find('result').find('rowset'))

        except:
            pass

        return rows

    def getAllTransactions(self, char_id, eve_key, eve_vcode) -> List:

        fromID = None
        journal = self.getTransactions(char_id, eve_key, eve_vcode, fromID)

        if len(journal) == 0:
            return []

        fromID = journal[-1].attrib['transactionID']
        lastPull = len(journal)

        while lastPull >= self._rowCount-1:
            newJournal = self.getTransactions(char_id, eve_key, eve_vcode, fromID)
            lastPull = len(newJournal)
            if lastPull == 0:
                break

            journal.extend(newJournal)
            fromID = newJournal[-1].attrib['transactionID']

        if lastPull > 0:
            newJournal = self.getTransactions(char_id, eve_key, eve_vcode, fromID)
            journal.extend(newJournal)

        return journal

    def getJournalEntries(self, char_id, eve_key, eve_vcode) -> List:

        auth = (char_id, eve_key, eve_vcode)
        url = "https://api.eveonline.com/char/WalletJournal.xml.aspx?characterID=%s&keyID=%s&vCode=%s&rowCount=1000" % auth
        req = requests.get(url)
        rows = []

        try:
            tree = ET.fromstring(req.text)

            if tree.find('error') is not None:
                print("Error while pulling transactions")
                return []

            rows = list(tree.find('result').find('rowset'))
        except:
            pass

        data = [{key:row.attrib[key] for key in ('amount', 'refTypeID', 'date')} for row in rows if time.strptime(row.attrib['date'], '%Y-%m-%d %H:%M:%S') > self._hour_offset]

        return data

    # user_id -> owner of the ETF account
    # char_id -> character to make API calls for
    async def gatherProfitData(self, user_id, char_id, eve_key, eve_vcode):

        rows = self.getAllTransactions(char_id, eve_key, eve_vcode)
        journal = self.getJournalEntries(char_id, eve_key, eve_vcode)

        data = [{key: row.attrib[key] for key in (
            'typeName', 'journalTransactionID', 'transactionType', 'price', 'quantity', 'typeID',
            'transactionDateTime')} for row in rows]

        sells = [x for x in data if
                 time.strptime(x['transactionDateTime'], '%Y-%m-%d %H:%M:%S') > self._hour_offset and x[
                     'transactionType'] == 'sell']
        #buys = [x for x in data if
        #        time.strptime(x['transactionDateTime'], '%Y-%m-%d %H:%M:%S') > self._hour_offset and x[
        #            'transactionType'] == 'buy']

        groupedSells = dict()

        result = {'user_id': user_id, 'profit': 0, 'taxes': 0, 'time': settings.utcnow,
                  'frequency': 'hourly'}

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

            profit = sum([float(x['price']) - float(buy['price']) for x in groupedSells[typeID]])
            totalSell = sum([float(x['price']) for x in groupedSells[typeID]])
            quantity = sum([int(x['quantity']) for x in groupedSells[typeID]])

            sales.append({
                'type': typeID,
                'sellPrice': float(groupedSells[typeID][0]['price']),
                'name': groupedSells[typeID][0]['typeName'],
                'totalProfit': profit,
                'quantity': quantity,
                'avgPerUnit': profit / quantity,
                'totalSell': totalSell,
                'time': datetime.strptime(groupedSells[typeID][0]['transactionDateTime'],
                                          '%Y-%m-%d %H:%M:%S').replace(tzinfo=settings.utcnow.tzinfo)
            })

        taxEntries = [x for x in journal if x['refTypeID'] == '54']
        brokerEntries = [x for x in journal if x['refTypeID'] == '46']

        tax = sum(float(x['amount']) for x in taxEntries)
        broker = sum(float(x['amount']) for x in brokerEntries)
        totalProfit = sum(float(x['totalProfit']) for x in sales)

        result['taxes'] = tax
        result['broker'] = broker
        result['profit'] = totalProfit

        self._sales[user_id] = sales
        self._profits[user_id] = {'profit': result['profit'], 'taxes': result['taxes'],
                                         'broker': result['broker']}
        await db.profit_chart.insert(result)

    async def updateTopItems(self, user_id) -> List:
        top_items = await db.profit_top_items.find_one({'user_id': user_id})

        sales = self._sales[user_id] if user_id in self._sales else None

        if sales is None:
            print("Failed to match sales results for user %s" % user_id)
            return []

        data = [{key: row[key] for key in ('totalProfit', 'quantity', 'avgPerUnit', 'type', 'name')} for row in
                sales]

        # create new top list
        if top_items is None:
            print("User %s has no top items chart" % user_id)

        # modify/add to existing top list
        else:
            for item in data:

                find = [(i, j) for i, j in enumerate(top_items['items']) if j['type'] == item['type']]

                if len(find) == 0:
                    top_items['items'].append(item)

                else:
                    index, top = find[0]

                    top['quantity'] += item['quantity']
                    top['totalProfit'] += item['totalProfit']
                    top['avgPerUnit'] = top['totalProfit'] / top['quantity']

                    top_items['items'][index] = top

            await db.profit_top_items.find_and_modify({'user_id': user_id}, top_items)

        return [{**{key: row[key] for key in
                                 ('totalProfit', 'quantity', 'avgPerUnit', 'time', 'name', 'type')},
                              **{'user_id': user_id}} for row in sales]

    async def updateAlltime(self, user_id):

        profit = self._profits[user_id] if user_id in self._profits else None

        if profit is None:
            print("Failed to match profit results for user %s" % user_id)
            return []

        alltime = await db.profit_all_time.find_one({'user_id': user_id})

        if alltime is None:
            print("User %s has no all time doc" % user_id)

        else:
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

    async def loadUserOrders(self, user_id, char_id, eve_key, eve_vcode):

        auth = (char_id, eve_key, eve_vcode)
        url = "https://api.eveonline.com/char/MarketOrders.xml.aspx?characterID=%s&keyID=%s&vCode=%s" % auth

        req = await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.get, url))

        rows = []

        try:
            tree = ET.fromstring(req.text)

            if tree.find('error') is not None:
                print(tree.find('error').text)
                print("Error while pulling user orders for user %s" % user_id)
                return

            rows = [row for row in list(tree.find('result').find('rowset')) if row.attrib['orderState'] == '0']

        except:
            pass

        orders = [{**{k:row.attrib[k] for k in ('orderID', 'orderState', 'volRemaining', 'issued', 'minVolume', 'stationID', 'volEntered', 'typeID', 'bid', 'price')}, **{'user_id': user_id}} for row in rows]

        if len(orders) == 0:
            return

        await db.user_orders.remove({'user_id': user_id})
        await db.user_orders.insert(orders)

    async def aggregateProfit(self):

        if not settings.is_hourly:
            return

        print("Aggregating profits")

        profit_start = time.perf_counter()
        transactions = []

        async for user in db.settings.find():

            if 'eveApiKey' not in user:
                continue

            if len(user['eveApiKey']['keyID']) == 0 and len(user['eveApiKey']['vCode']) == 0:
                continue

            user_id = user['user_id']
            char_id = user['eveApiKey']['characterID']
            eve_key = user['eveApiKey']['keyID']
            eve_vcode = user['eveApiKey']['vCode']

            await self.gatherProfitData(user_id, char_id, eve_key, eve_vcode)

            transactions.extend(await self.updateTopItems(user_id))

            await self.updateAlltime(user_id)

            await self.loadUserOrders(user_id, char_id, eve_key, eve_vcode)

        if len(transactions) > 0:
            await db.profit_transactions.insert(transactions)

        await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.post,
                                                                               'http://' + publish_url + '/publish/profit', timeout=5))

        print("Profits aggregated in %s seconds" % (time.perf_counter() - profit_start))

if __name__ == "__main__":

    loop = asyncio.get_event_loop()

    print("Running at %s" % settings.utcnow)

    loop.run_until_complete(MarketAggregator().StartAggregation())
    loop.run_until_complete(PortfolioAggregator().aggregatePortfolios())
    loop.run_until_complete(ProfitAggregator().aggregateProfit())

    loop.close()