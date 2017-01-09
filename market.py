import asyncio
from typing import List, Dict
import requests
import json
import time
import functools
from datetime import timedelta

from bson.objectid import ObjectId
import traceback
import redis
import math
import sys
import os

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from aggregation import settings as _settings, database, redis_interface

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

# Use to override timed operations
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

os.chdir(os.path.dirname(sys.argv[0]))

# Load static data
with open(os.path.realpath('./sde/blueprints.js'), 'r', encoding='utf-8') as f:
    blueprints = json.loads(f.read())

with open(os.path.realpath('./sde/market_ids.js'), 'r', encoding='utf-8') as f:
    market_ids = json.loads(f.read())

with open(os.path.realpath('./sde/blueprint_basePrice.js'), 'r', encoding='utf-8') as f:
    blueprints_basePrice = json.loads(f.read())

    for i in blueprints_basePrice:
        override_basePrice[int(i)] = blueprints_basePrice[i]

settings = _settings.Settings()
db = database.DatabaseConnector()

class Utilities:
    def __init__(self):
        pass

    @staticmethod
    def SplitArray(_list, wanted_parts=1):
        length = len(_list)
        return [_list[i * length // wanted_parts: (i + 1) * length // wanted_parts]
                for i in range(wanted_parts)]

utilities = Utilities()

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
                diff = exist_orders_volume[i['id']] - i['volume']

                if diff <= 0:
                    continue

                if i['type'] in self._volume_changes[region]:
                    self._volume_changes[region][i['type']] += diff
                else:
                    self._volume_changes[region][i['type']] = diff

            # Consider volume from deleted orders
            '''
            if len(self._deleted_orders) > 0:

                type_to_avg_volume = {}
                type_to_total_buy_volume = {}
                type_to_total_sell_volume = {}

                # Pre-compute some basic data
                for i in filter_by_region(self._existing_orders, region):

                    # Ignore worthless orders
                    if i['price'] < 10:
                        continue

                    _type = i['type']

                    if _type not in type_to_avg_volume:
                        type_to_avg_volume[_type] = [i['volume']]
                    else:
                        type_to_avg_volume[_type].append(i['volume'])

                    if i['buy'] == True:
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
                            if _type not in type_to_total_buy_volume:
                                _check = 0
                            else:
                                _check = type_to_total_buy_volume[_type] - exist_orders_volume[i]
                            if _check > 1:
                                if exist_orders_volume[i] > _check:
                                    print("Order % of item %s exceeds realistic buy volume %s,%s" % (
                                        i, _type, exist_orders_volume[i],
                                        type_to_total_buy_volume[_type] - exist_orders_volume[i]))
                                    continue
                        else:
                            if _type not in type_to_total_sell_volume:
                                _check = 0
                            else:
                                _check = type_to_total_sell_volume[_type] - exist_orders_volume[i]
                            if _check > 1:
                                if exist_orders_volume[i] > _check:
                                    print("Order % of item %s exceeds realistic sell volume %s,%s" % (
                                        i, _type, exist_orders_volume[i],
                                        type_to_total_sell_volume[_type] - exist_orders_volume[i]))
                                    continue

                    if _type in type_to_avg_volume:
                        if exist_orders_volume[i] > type_to_avg_volume[_type] * 10:
                            print("Order % of item %s exceeds realistic avg %s,%s" % (
                                i, _type, exist_orders_volume[i], type_to_avg_volume[_type] * 10))
                            continue

                    if _type not in self._volume_changes[region]:
                        self._volume_changes[region][_type] = exist_orders_volume[i]
                    else:
                        self._volume_changes[region][_type] += exist_orders_volume[i]
            '''

            print("%s volume changes for region %s" % (len(self._volume_changes[region]), region))

        # Clean up orders that were pulled from DB for this task
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
            cache.LoadDailyRedisCache(self._aggregates_daily)


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
                    'time': 1,
                    'regions.region': 1,
                    'regions.spread': 1,
                    'regions.tradeVolume': 1,
                    'regions.buyPercentile': 1
                },
            },
            {
                '$unwind': "$regions"
            },
            {
                '$sort': { 'time': 1 } # Sort them so that we can pull the first document in the 7 day time series
            },
            {
                '$group': {
                    '_id': {"region": "$regions.region", "type": "$type"},
                    'spread': {'$avg': '$regions.spread'},
                    'tradeVolume': {'$avg': '$regions.tradeVolume'},
                    'velocity': {'$first': '$regions.buyPercentile'}
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
            velocity = 0

            if _type in self._aggregates_daily_sma:
                if region in self._aggregates_daily_sma[_type]:
                    spread_sma = self._aggregates_daily_sma[_type][region]['spread']
                    volume_sma = self._aggregates_daily_sma[_type][region]['tradeVolume']
                    velocity = self._aggregates_daily_sma[_type][region]['velocity']

            if _type not in accumulator:
                accumulator[_type] = {
                    'time': settings.utcnow,
                    'type': _type,
                    'regions': [
                        {
                            'region': region,
                            'spread_sma': spread_sma,
                            'volume_sma': volume_sma,
                            'velocity': (i['buyPercentile'] - velocity) if velocity is not 0 else 0,
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
                        'velocity': (i['buyPercentile'] - velocity) if velocity is not 0 else 0,
                        **{key: value for key, value in i.items() if key not in {'_id'}}
                    }
                )

        self._aggregates_daily = list(accumulator.values())

        await self.DoThreadedInsert(db.aggregates_daily, self._aggregates_daily)

        print("Daily data finished in %s seconds" % (time.perf_counter() - agg_timer))

        return self._aggregates_daily

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

cache = redis_interface.CacheInterface(orderInterface)

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
        self.system_indexes = {}
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

    def buildSystemIndexes(self):
        try:
            res = requests.get('https://crest-tq.eveonline.com/industry/systems/', timeout=10)

            doc = json.loads(res.text)

            for item in doc['items']:
                for activity in item['systemCostIndices']:
                    if activity['activityID'] == 3:
                        self.system_indexes[item['solarSystem']['id']] = activity['costIndex']

        except:
            print("Failed to load system indexes")

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

        taxRate = 0.10
        user_settings = await db.GetAllUserSettings()
        adjustedPrices = self.getAdjustedPrices()

        # Load up system cost indices
        self.buildSystemIndexes()

        if not cache.RedisAvailable():
            print("Skipping portfolio aggregation since redis is unavailable")
            return

        async for doc in db.portfolios.find():

            try:
                systemIndex = self.system_indexes.get(doc.get('buildSystem', 0), 0.0001)
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
                    unitPrice = float(minuteData.get(b'sellPercentile', 0)) if doc['type'] == 0 else float(minuteData.get(b'buyPercentile', 0))
                    portfolioBuyValue += float(minuteData.get(b'buyPercentile', 0)) * component['quantity']
                    adjustedPrice = adjustedPrices[typeID] if typeID in adjustedPrices else unitPrice
                    totalPrice = unitPrice * component['quantity']
                    totalAdjustedPrice = adjustedPrice * component['quantity']
                    spread = float(minuteData.get(b'spread', 0))
                    volume = float(dailyData.get(b'tradeVolume', 0))
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
                                    cache.redis.hgetall('cur:' + str(mat['typeID']) + '-' + str(region)).get(b'buyPercentile', 0)) * matQuantity
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

                        simulation = self.doSimulateTrade(typeID, component['quantity'], float(minuteData.get(b'buyPercentile', 0)), float(minuteData.get(b'sellPercentile', 0)), simulation_settings, region)

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

                    # Apply the overriding sell price for this portfolio if applicable
                    if doc.get('overrideSellPrice', None) is not None:
                        industryValue = doc.get('overrideSellPrice', 0) * doc['industryQuantity']
                    else:
                        industryValue = float(baseMinuteData.get(b'sellPercentile', 0)) * doc['industryQuantity']

                    if industryValue != 0:
                        industrySpread = 100 - (portfolioValue / industryValue) * 100
                    else:
                        industrySpread = 0

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

class SubscriptionUpdater:
    def __init__(self):
        pass

    async def updateSubscriptions(self):

        await asyncio.gather(*[
            self.checkExpired(),
            self.checkDeposits()
        ])

    async def checkDeposits(self):

        if not settings.is_hourly:
            return

        auth = (1000, 5682889, '***REMOVED***')
        url = "https://api.eveonline.com/corp/WalletJournal.xml.aspx?accountKey=%s&keyID=%s&vCode=%s&rowCount=1000" % auth
        req = requests.get(url)
        rows = []

        try:
            tree = ET.fromstring(req.text)

            if tree.find('error') is not None:
                print("Error while pulling corporation journal for checking deposits (%s, %s, %s): %s" % (auth[0], auth[1], auth[2], tree.find('error').text))
                return

            rows = list(tree.find('result').find('rowset'))
        except:
            pass

        data = [{key:row.attrib[key] for key in ('amount', 'refTypeID', 'date', 'ownerID1', 'ownerName1')} for row in rows if time.strptime(row.attrib['date'], '%Y-%m-%d %H:%M:%S') > settings.hour_offset_utc.utctimetuple()]

        deposits = [x for x in data if x['refTypeID'] == '10']

        user_settings = await db.GetAllUserSettings()

        for deposit in deposits:
            user_id = int(deposit['ownerID1'])
            amount = float(deposit['amount'])

            if user_id not in user_settings:
                print("Unknown user %s:%s has made a deposit of %s ISK" % (user_id, deposit['ownerName1'], amount))

                await db.audit.insert({
                    'user_id': 0,
                    'target': user_id,
                    'balance': amount,
                    'action': 0,
                    'time': settings.utcnow
                })

                continue

            await db.notifications.insert({
                "user_id": user_id,
                "time": settings.utcnow,
                "read": False,
                "message": "A deposit has been made into your account for the amount of %s ISK" % amount
            })

            await db.subscription.find_and_modify({'user_id': user_id}, {
                '$inc': {
                    'balance': amount
                },
                '$push': {
                    'history': {
                        'time': settings.utcnow,
                        'type': 0,
                        'amount': amount,
                        'description': 'Automated deposit',
                        'processed': True
                    }
                }
            })

            await db.audit.insert({
                'user_id': 0,
                'target': user_id,
                'balance': amount,
                'action': 0,
                'time': settings.utcnow
            })

            await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.post,
                                                                                   'http://' + publish_url + '/publish/notifications/%s' %
                                                                                   user_id,
                                                                                   timeout=5))

            await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.post,
                                                                                   'http://' + publish_url + '/publish/subscription/%s' %
                                                                                   user_id,
                                                                                   timeout=5))

        await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.post,
                                                                               'http://' + publish_url + '/publish/audit',
                                                                               timeout=5))

        return data

    async def checkExpired(self):

        user_settings = await db.GetAllUserSettings()

        all_subs = await db.subscription.find().to_list(length=None)

        for sub in all_subs:

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

                    await db.notifications.insert({
                        "user_id": sub['user_id'],
                        "time": settings.utcnow,
                        "read": False,
                        "message": 'Your premium subscription has been automatically renewed'
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

                    await db.notifications.insert({
                        "user_id": sub['user_id'],
                        "time": settings.utcnow,
                        "read": False,
                        "message": 'Your premium subscription has expired and not automatically renewed'
                    })

                    await db.audit.insert({
                        'user_id': sub['user_id'],
                        'target': 0,
                        'balance': 0,
                        'action': 9,
                        'time': settings.utcnow
                    })

                    ''' API Expired is already assumed by subscription ending
                    await db.audit.insert({
                        'user_id': sub['user_id'],
                        'target': 0,
                        'balance': 0,
                        'action': 14,
                        'time': settings.utcnow
                    })
                    '''

                    await asyncio.get_event_loop().run_in_executor(None, functools.partial(requests.post,
                                                                                         'http://' + publish_url + '/publish/settings/%s' % sub['user_id'],
                                                                                         timeout=5))
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
    script_start = time.perf_counter()

    loop.run_until_complete(MarketAggregator().StartAggregation())
    loop.run_until_complete(PortfolioAggregator().aggregatePortfolios())
    loop.run_until_complete(SubscriptionUpdater().updateSubscriptions())

    print("Finished in %s seconds" % (time.perf_counter() - script_start))

    loop.close()