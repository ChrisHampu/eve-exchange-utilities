import redis
import time
import asyncio
import traceback
from . import settings as _settings, database

settings = _settings.Settings()
db = database.db

forge_region = 10000002

class CacheInterface:
    def __init__(self, orderInterface):
        self._redis = None
        self.orderInterface = orderInterface
        self.price_cache = {}

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

    def GetItemSellPrice(self, typeid):
        if typeid in self.price_cache:
            return self.price_cache[typeid]

        doc = self._redis.hmget('cur:%s-%s' % (typeid, forge_region), ['sellPercentile'])

        if doc is None or doc[0] is None:
            self.price_cache[typeid] = 0

            return 0

        self.price_cache[typeid] = float(doc[0])

        return self.price_cache[typeid]

    async def PurgeStaleMarketData(self):

        print("Purging stale market data")
        purgeTimer = time.perf_counter()

        await asyncio.gather(*[
            db.aggregates_minutes.remove({'time': {'$lte': settings.minute_data_prune_time}}),
            db.aggregates_hourly.remove({'time': {'$lte': settings.hourly_data_prune_time}})
        ])

        if self.orderInterface._deleted_orders == None:
            print("Failed to purge stale market data from redis: No orders to delete")
        else:
            for order in self.orderInterface._deleted_orders:

                self._redis.delete('ord:%s' % order)

        print("Stale market data purged in %s seconds" % (time.perf_counter() - purgeTimer))

    def LoadHourlyRedisCache(self, aggregates):
        if not self.RedisAvailable():
            print("Skipping hourly redis cache load since redis is unavailable")

        print("Loading hourly documents into redis cache")

        loadTimer = time.perf_counter()
        hourlyDocs = {}

        for doc in aggregates:
            if doc['type'] in hourlyDocs:
                hourlyDocs[doc['type']] = doc

            else:
                hourlyDocs[doc['type']] = doc

        pip = self._redis.pipeline()

        for k in hourlyDocs:
            for reg in hourlyDocs[k]['regions']:
                pip.hmset('hrly:' + str(k) + '-' + str(reg['region']), {**reg, **{'type': k}})

        pip.execute()

        print("Loaded %s hourly documents into cache in %s seconds" % (len(hourlyDocs), time.perf_counter() - loadTimer))

    def LoadDailyRedisCache(self, aggregates):
        if not self.RedisAvailable():
            print("Skipping daily redis cache load since redis is unavailable")

        print("Loading daily documents into redis cache")

        loadTimer = time.perf_counter()
        dailyDocs = {}

        for doc in aggregates:
            if doc['type'] in dailyDocs:
                dailyDocs[doc['type']] = doc

            else:
                dailyDocs[doc['type']] = doc

        pip = self._redis.pipeline()

        for k in dailyDocs:
            for reg in dailyDocs[k]['regions']:
                pip.hmset('dly:' + str(k) + '-' + str(reg['region']), {**reg, **{'type': k}})

        pip.execute()

        print("Loaded %s daily documents into cache in %s seconds" % (len(dailyDocs), time.perf_counter() - loadTimer))

    async def LoadCurrentRedisCache(self, aggregates):
        if not self.RedisAvailable():
            print("Skipping current redis cache load since redis is unavailable")

        await asyncio.gather(*[
            self._loadAggregates(aggregates),
            self._loadOrders()
        ])

    async def _loadAggregates(self, aggregates):
        loadTimer = time.perf_counter()

        try:
            # Keys use both the type ID and region for efficiency in retrieval, and to avoid nesting

            pip = self._redis.pipeline()

            for v in aggregates:
                for reg in v['regions']:
                    pip.hmset('cur:' + str(v['type']) + '-' + str(reg['region']),
                                      {**reg, **{'type': v['type']}})

            pip.execute()

        except:
            traceback.print_exc()
            print("Failed to update current redis cache")

        print("Loaded current aggregates into cache in %s seconds" % (time.perf_counter() - loadTimer))

    async def _loadOrders(self):
        print("Loading market orders into redis")
        loadTimer = time.perf_counter()

        try:
            order_map = {}

            pip = self._redis.pipeline()

            for order in self.orderInterface._persisted_orders:
                pip.hmset('ord:' + str(order['id']), {
                    'volume': order['volume'],
                    'type': order['type'],
                    'price': order['price'],
                    'buy': order['buy'],
                    'stationID': order['stationID']
                })

                if order['type'] in order_map:
                    if order['region'] in order_map[order['type']]:
                        order_map[order['type']][order['region']].append(order['id'])
                    else:
                        order_map[order['type']][order['region']] = [order['id']]
                else:
                    order_map[order['type']] = {}

            pip.execute()

            pip = self._redis.pipeline()

            for type in order_map:
                for region in order_map[type]:
                    pip.delete('ord_cnt:%s-%s' % (type, region), *order_map[type][region])

            pip.execute()

            pip = self._redis.pipeline()

            for type in order_map:
                for region in order_map[type]:
                    pip.lpush('ord_cnt:%s-%s' % (type, region), *order_map[type][region])

            pip.execute()

        except:
            traceback.print_exc()
            print("Failed to update market order redis cache")

        print("Loaded current orders into cache in %s seconds" % (time.perf_counter() - loadTimer))
