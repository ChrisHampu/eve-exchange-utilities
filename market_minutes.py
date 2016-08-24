import sys
import multiprocessing
import requests
import json
import time
from datetime import datetime, timedelta
import rethinkdb as r
import traceback

import csv

Profile = False

# arguments
# 1: horizonDBName
# 2: orderTableName
# 3: aggregateTableName
# 4: hourlyTableName
# 5: dailyTableName

HorizonDB = sys.argv[1]
OrdersTable = sys.argv[2]
AggregateTable = sys.argv[3]
HourlyTable = sys.argv[4]
DailyTable = sys.argv[5]
volumeScratchTable = 'volume'

lowResPruneTime = 86400 # Prune 5-minute res documents older than 24 hours
medResPruneTime = 604800 # Prune hourly documents older than 1 week

dt = datetime.now()
now = datetime.now(r.make_timezone('00:00'))

tt = dt.timetuple()
utt = dt.utctimetuple()

def split_list(alist, wanted_parts=1):
  length = len(alist)
  return [ alist[i*length // wanted_parts: (i+1)*length // wanted_parts]
      for i in range(wanted_parts) ]

def getConnection():
  return r.connect(db=HorizonDB)

def loadPages(volumeChanges, pages):
  inserted = 0

  items = []

  for i in pages:

    pageTime = time.perf_counter()

    req = requests.get("https://crest-tq.eveonline.com/market/10000002/orders/all/?page=%s" % i)
    j = req.json()

    print("Fetched page %s in %s seconds" % (i, time.perf_counter() - pageTime))

    if 'items' not in j:
      continue

    items.append([k['id'] for k in j['items']])

    for item in j['items']:
      item['$hz_v$'] = 0

    try:
      changes = r.table(OrdersTable).insert(j['items'], durability="soft", return_changes=True, conflict="replace").run(getConnection())
      inserted += len(j['items'])

      for change in changes['changes']:

        if change['old_val'] is None:
          continue

        _type = change['old_val']['type']
        diff = change['old_val']['volume'] - change['new_val']['volume']

        if diff == 0:
          continue

        if _type not in volumeChanges:
          volumeChanges[_type] = diff
        else:
          volumeChanges[_type] += diff

    except Exception as e:
      print("DB error while processing page %s: %s" % (i, e))
      traceback.print_exc()

  return items

if __name__ == '__main__':

  start, useHourly, useDaily  =  (time.perf_counter(), False, False)

  print("Executing at %s" % dt)

  if (tt.tm_min == 00):
    print("Writing hourly data")
    useHourly = True

  # 11 AM UTC (EVE downtime)
  if (utt.tm_hour == 11 and useHourly == True):
    print("Writing daily data")
    useDaily = True

  req = requests.get("https://crest-tq.eveonline.com/market/10000002/orders/all/")

  js = req.json()

  pageCount = js['pageCount']

  for item in js['items']:
    item['$hz_v$'] = 0

  volumeChanges = {}

  changes = r.table(OrdersTable).insert(js['items'], durability="soft", return_changes=True, conflict="replace").run(getConnection())

  for change in changes['changes']:

    if change['old_val'] is None:
      continue

    _type = change['old_val']['type']
    diff = change['old_val']['volume'] - change['new_val']['volume']

    if diff == 0:
      continue

    if _type not in volumeChanges:
      volumeChanges[_type] = diff
    else:
      volumeChanges[_type] += diff

  print("Working on %s pages" % pageCount)

  workers = min(multiprocessing.cpu_count(), pageCount)

  print("Executing using %s workers" % workers)

  work = split_list(range(2,pageCount+1), workers)

  orderIDs = []

  with multiprocessing.Pool(processes=workers) as pool:

    results = [pool.apply_async(loadPages, (volumeChanges, work[i])) for i in range(0, len(work))]

    orderIDs = [res.get() for res in results]
    orderIDs = [k for i in orderIDs for j in i for k in j]

    orderIDs.extend([k['id'] for k in js['items']])

  print("Finished in %s seconds " % (time.perf_counter() - start))

  _orderIDs = set(orderIDs)
  existingOrders = list(r.db(HorizonDB).table(OrdersTable).filter(lambda doc: (doc['stationID'] == 60003760) | (doc['stationID'] > 1000000000000)).pluck('id', 'volume', 'type').run(getConnection(), array_limit=300000))

  existingOrderVolume = dict([(d['id'],d['volume']) for d in existingOrders])
  existingOrderID2Type = dict([(d['id'],d['type']) for d in existingOrders])
  existingOrderIDs = [i['id'] for i in existingOrders]

  toDelete = [v for v in existingOrderIDs if v not in _orderIDs]

  # Group total volume by type
  print("Starting volume anomaly detection")
  anomalyStart = time.perf_counter()
  typeToAvgVolume = {}

  for i in existingOrderIDs:
    if i not in typeToAvgVolume:
      typeToAvgVolume[existingOrderID2Type[i]] = [existingOrderVolume[i]]
    else:
      typeToAvgVolume[existingOrderID2Type[i]].append(existingOrderVolume[i])

  # Average out the volumes
  for i in typeToAvgVolume:
    typeToAvgVolume[i] = sum(typeToAvgVolume[i]) / len(typeToAvgVolume[i])

  anoms = 0

  # Iterate, verify, and sum the documents that need to be removed
  for i in toDelete:
    _type = existingOrderID2Type[i]

    if existingOrderVolume[i] > typeToAvgVolume[_type] * 1000:
      anoms += 1
      continue

    if i not in volumeChanges:
      volumeChanges[_type] = existingOrderVolume[i]
    else:
      volumeChanges[_type] += existingOrderVolume[i]

  print("%s orders are anomalous vs %s orders to be deleted and %s existing orders" % (anoms, len(toDelete), len(existingOrderIDs)))
  print("Finished anomaly detection in %s seconds " % (time.perf_counter() - anomalyStart))

  volumeConnection = getConnection()
  volumeIDs = volumeChanges.keys()

  volumeDocs = list(r.table("volume").get_all(r.args(volumeIDs)).run(volumeConnection))

  existingKeys = [d['id'] for d in volumeDocs]

  inserts = []

  for _id in volumeIDs:

    if _id not in existingKeys:
      inserts.append({'id': _id, 'volume': volumeChanges[_id]})

    else:
      r.table("volume").get(_id).update({'volume': r.row['volume'] + volumeChanges[_id]}, durability="soft", return_changes=False).run(volumeConnection)

  if len(inserts) > 0:
    r.table("volume").insert(inserts, durability="soft", return_changes=False).run(volumeConnection)

  print("Starting aggregation")
  aggTimer = time.perf_counter()

  # TODO Benchmark plucking only the 3 relevant fields
  aggregates = (r.table(OrdersTable)
  .filter( lambda doc: (doc['buy'] == True) & (doc['price'] > 1 ) & ((doc['stationID'] == 60003760) | (doc['stationID'] > 1000000000000)) )
  .group("type")
  .map( lambda doc: { 
    'price': doc["price"], 'volume': doc["volume"] 
  })
  .ungroup()
  .map( lambda doc: {
    'buy': doc["reduction"].order_by(r.desc("price")).slice(0, r.expr([1, r.expr(0.95).mul(doc["reduction"].count()).floor()]).max()).map( lambda rec: {
      'type': doc["group"],
      'count': 1,
      'total': rec["price"],
      'max': rec["price"],
      'min': rec["price"],
      'volume': rec["volume"]
    })
    .reduce( lambda left, right: {
      'max': r.branch(r.gt(left['max'], right['max']), left['max'], right['max']),
      'min': r.branch(r.gt(left['min'], right['min']), right['min'], left['min']),
      'total': left['total'].add(right['total']),
      'volume': left['volume'].add(right['volume']),
      'count': left['count'].add(right['count']),
      'type': left['type'],
    }),
    'buyPercentile': doc["reduction"].order_by(r.desc("price")).slice(0, r.expr([1, r.expr(0.05).mul(doc["reduction"].count()).floor()]).max()).map( lambda rec: {
      'count': 1,
      'total': rec["price"],
    })
    .reduce( lambda left, right: {
      'total': left['total'].add(right['total']),
      'count': left['count'].add(right['count']),
    })
  })
  .map( lambda doc: {
    'type': doc["buy"]["type"],
    'buymax': doc["buy"]["max"],
    'buymin': doc["buy"]["min"],
    'buyavg': doc["buy"]["total"].div(doc["buy"]["count"]),
    'buyFifthPercentile': doc["buyPercentile"]["total"].div(doc["buyPercentile"]["count"]),
    'volume': doc["buy"]["volume"]
  })
  .union(
    r.table(OrdersTable)
    .filter({'buy': False})
    .group("type")
    .map( lambda doc: {
      'price': doc["price"], 'volume': doc["volume"]
    })
    .ungroup()
    .map( lambda doc: {
        'sell': doc["reduction"].order_by(r.asc("price")).slice(0, r.expr([1, r.expr(0.95).mul(doc["reduction"].count()).floor()]).max()).map( lambda rec: {
          'type': doc["group"],
          'count': 1,
          'total': rec["price"],
          'price': rec["price"],
          'max': rec["price"],
          'min': rec["price"],
          'volume': rec["volume"]
        })
        .reduce( lambda left, right: {
          'max': r.branch(r.gt(left['max'], right['max']), left['max'], right['max']),
          'min': r.branch(r.gt(left['min'], right['min']), right['min'], left['min']),
          'total': left['total'].add(right['total']),
          'volume': left['volume'].add(right['volume']),
          'count': left['count'].add(right['count']),
          'type': left['type'],
          'price': left['price']
        }),
        'sellPercentile': doc["reduction"].order_by(r.asc("price")).slice(0, r.expr([1, r.expr(0.05).mul(doc["reduction"].count()).floor()]).max()).map( lambda rec: {
          'count': 1,
          'total': rec["price"],
        })
        .reduce( lambda left, right: {
          'total': left['total'].add(right['total']),
          'count': left['count'].add(right['count']),
        })
    })
    .map( lambda doc: {
      'type': doc["sell"]["type"],
      'sellmax': doc["sell"]["max"],
      'sellmin': doc["sell"]["min"],
      'sellavg': doc["sell"]["total"].div(doc["sell"]["count"]),
      'sellFifthPercentile': doc["sellPercentile"]["total"].div(doc["sellPercentile"]["count"]),
      'volume': doc["sell"]["volume"]
    })
  )
  .group("type")
  .ungroup()
  .map( lambda group: {
    '$hz_v$': 0,
    'type': group["group"],
    'sellAvg': group["reduction"][1]["sellavg"].default(0),
    'sellMin': group["reduction"][1]["sellmin"].default(0),
    'sellMax': group["reduction"][1]["sellmax"].default(0),
    'sellFifthPercentile': group["reduction"][1]["sellFifthPercentile"].default(0),
    'sellVolume': group["reduction"][1]["volume"].default(0),
    'buyVolume': group["reduction"][0]["volume"].default(0),
    'buyFifthPercentile': group["reduction"][0]["buyFifthPercentile"].default(0),
    'close': group["reduction"][0]["buyavg"].default(0),
    'low': group["reduction"][0]["buymin"].default(0),
    'high': group["reduction"][0]["buymax"].default(0),
    'spread': r.expr(100).sub(group["reduction"][0]["buyFifthPercentile"].default(1).div(group["reduction"][1]["sellFifthPercentile"].default(1)).mul(r.expr(100))),
    'spreadValue': group["reduction"][1]["fifthPercentile"].default(1).div(100).mul(r.expr(100).sub(group["reduction"][0]["buyFifthPercentile"].default(1).div(group["reduction"][1]["sellFifthPercentile"].default(1)).mul(r.expr(100)))),
    'tradeValue': group["reduction"][0]["volume"].default(0).mul(group["reduction"][1]["sellFifthPercentile"].default(1).div(100).mul(r.expr(100).sub(group["reduction"][0]["buyFifthPercentile"].default(1).div(group["reduction"][1]["sellFifthPercentile"].default(1)).mul(r.expr(100)))))
  })
  .run(getConnection(), array_limit=300000, profile=Profile)
  )

  for v in aggregates:
    v['time'] = now
    if v['type'] in volumeIDs:
      v['tradeVolume'] = volumeChanges[v['type']]
    else:
      v['tradeVolume'] = 0

  r.table(AggregateTable).insert(aggregates, return_changes=False).run(getConnection())

  print("Aggregation finished in %s seconds" % (time.perf_counter() - aggTimer))

  if useHourly == True:

    print("Beginning hourly aggregation")
    hourlyTimer = time.perf_counter()
    hourlyConn = getConnection()

    volume = list(r.table("volume").run(hourlyConn))

    volumeData = dict([(d['id'],d['volume']) for d in volume])

    volumeKeys = volumeData.keys()

    for v in aggregates:
      if v['type'] in volumeKeys:
        v['tradeVolume'] = volumeData[v['type']]

    r.table(HourlyTable).insert(aggregates, return_changes=False).run(hourlyConn)

    print("Finished hourly aggregation in %s seconds" % (time.perf_counter() - hourlyTimer))

  if useDaily == True:

    print("Beginning daily aggregation")
    dailyStart = time.perf_counter()

    dailyAggregates = list(r.table(HourlyTable)
    .filter(lambda doc:
      r.now().sub(doc["time"]).lt(86400)
    )
    .group("type")
    .map(lambda doc: {
      'sellAvg': doc["sellAvg"],
      'sellMin': doc["sellMin"],
      'sellFifthPercentile': doc["sellFifthPercentile"],
      'buyFifthPercentile': doc["buyFifthPercentile"],
      'sellVolume': doc["sellVolume"],
      'buyVolume': doc["buyVolume"],
      'close': doc["close"],
      'low': doc["low"],
      'high': doc["high"],
      'spread': doc["spread"],
      'spreadValue': doc["spreadValue"],
      'tradeValue': doc["tradeValue"],
      'tradeVolume': doc["tradeVolume"],
      'count': 1,
    })
    .reduce(lambda left, right: {
      'count': left["count"].add(right["count"]),
      'sellAvg': left["sellAvg"].add(right["sellAvg"]),
      'sellMin': r.branch(r.gt(left["sellMin"], right['sellMin']), right['sellMin'], left['sellMin']),
      'low': r.branch(r.gt(left["low"], right['low']), right['low'], left['low']),
      'high': r.branch(r.gt(left['high'], right['high']), left['high'], right['high']),
      'sellFifthPercentile': left["sellFifthPercentile"].add(right["sellFifthPercentile"]),
      'buyFifthPercentile': left["buyFifthPercentile"].add(right["buyFifthPercentile"]),
      'sellVolume': left["sellVolume"].add(right["sellVolume"]),
      'buyVolume': left["buyVolume"].add(right["buyVolume"]),
      'close': left["close"].add(right["close"]),
      'spread': left["spread"].add(right["spread"]),
      'spreadValue': left["spreadValue"].add(right["spreadValue"]),
      'tradeValue': left["tradeValue"].add(right["tradeValue"]),
      'tradeVolume': left["tradeVolume"].add(right["tradeVolume"])
    })
    .ungroup()
    .map(lambda doc: {
      'type': doc["group"],
      'buyVolume': doc["reduction"]["buyVolume"].div(doc["reduction"]["count"]),
      'close': doc["reduction"]["close"].div(doc["reduction"]["count"]),
      'sellAvg': doc["reduction"]["sellAvg"].div(doc["reduction"]["count"]),
      'sellVolume': doc["reduction"]["sellVolume"].div(doc["reduction"]["count"]),
      'spread': doc["reduction"]["spread"].div(doc["reduction"]["count"]),
      'tradeValue': doc["reduction"]["tradeValue"].div(doc["reduction"]["count"]),
      'spreadValue': doc["reduction"]["spreadValue"].div(doc["reduction"]["count"]),
      'sellFifthPercentile': doc["reduction"]["sellFifthPercentile"].div(doc["reduction"]["count"]),
      'buyFifthPercentile': doc["reduction"]["buyFifthPercentile"].div(doc["reduction"]["count"]),
      'low': doc["reduction"]["low"],
      'high': doc["reduction"]["high"],
      'sellMin': doc["reduction"]["sellMin"],
      'tradeVolume': doc["reduction"]["tradeVolume"],
      'time': now,
      '$hz_v$': 0
    })
    .run(getConnection()))

    r.table(DailyTable).insert(dailyAggregates, return_changes=False).run(getConnection())

    print("Daily aggregation finished in %s seconds" % (time.perf_counter() - dailyStart))

  print("Flushing stale data")

  flushConnection = getConnection()
  flushTotalTimer = time.perf_counter()

  flushTimer = time.perf_counter()
  print("Flushing low resolution data")
  r.table(AggregateTable).filter(lambda doc: r.now().sub(doc["time"]).gt(lowResPruneTime)).delete(durability="soft").run(flushConnection)
  print("Low res data flushed in %s seconds" % (time.perf_counter() - flushTimer))

  if useHourly == True:

    print("Flushing medium resolution data")

    flushTimer = time.perf_counter()
    prepareTimer = time.perf_counter()

    hourlyToDelete = []
    hourlyDelta = timedelta(seconds=medResPruneTime)
    docs = r.table(HourlyTable).run(flushConnection)
    
    for doc in docs:
      if now - doc['time'] > hourlyDelta:
        hourlyToDelete.append(doc['id'])

    print("Calculated %s documents to delete in %s seconds" % (len(hourlyToDelete), time.perf_counter() - prepareTimer))

    try:
      r.table(HourlyTable).get_all(r.args(hourlyToDelete)).delete().run(flushConnection)
    except:
      traceback.print_exc()

    print("Medium res data flushed in %s seconds" % (time.perf_counter() - flushTimer))

  flushTimer = time.perf_counter()
  print("Flushing stale orders")
  r.table(OrdersTable).get_all(r.args(toDelete)).delete(durability="soft").run(flushConnection)
  print("Stale orders flushed in %s seconds" % (time.perf_counter() - flushTimer))

  if useHourly == True:

    flushTimer = time.perf_counter()
    print("Flushing stale volume data")
    r.table('volume').delete(durability="soft").run(flushConnection)
    print("Stale volume data flushed in %s seconds" % (time.perf_counter() - flushTimer))

  print("Stale data flushed in total of %s seconds" % (time.perf_counter() - flushTotalTimer))

  print("Total time taken is %s seconds" % (time.perf_counter() - start))