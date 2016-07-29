import multiprocessing
import requests
import json
import time
from datetime import datetime
import rethinkdb as r

import csv

Profile = False
PrintCSV = False
PrintJSON = False

# arguments
# 1: horizonDBName
# 2: orderTableName
# 3: aggregateTableName

HorizonDB = sys.argv[1]
OrdersTable = sys.argv[2]
AggregateTable = sys.argv[3]

def split_list(alist, wanted_parts=1):
  length = len(alist)
  return [ alist[i*length // wanted_parts: (i+1)*length // wanted_parts]
      for i in range(wanted_parts) ]

def loadPages(pages):
  horizon_conn, inserted = (r.connect(db=HorizonDB), 0)

  for i in pages:
    req = requests.get("https://crest-tq.eveonline.com/market/10000002/orders/all/?page=%s" % i)
    j = req.json()
    if 'items' not in j:
      continue
    for item in j['items']: item['$hz_v$'] = 0
    r.table(OrdersTable).insert(j['items'], durability="soft", return_changes=False, conflict="replace").run(horizon_conn)
    inserted += len(j['items'])

  return inserted

if __name__ == '__main__':

  agg_conn, horizon_conn, start  =  (r.connect(db=HorizonDB), r.connect(db=HorizonDB), time.perf_counter())

  dt = datetime.now()

  print("Executing at %s" % dt)

  tt = dt.timetuple()

  if (tt.tm_hour == 13 and tt.tm_min == 55):

    print("Flushing stale market orders")

    r.db('market').table('orders').delete({}).run(conn)

    print("Stale orders flushed")

  req = requests.get("https://crest-tq.eveonline.com/market/10000002/orders/all/")

  js = req.json()

  pageCount = js['pageCount']

  for item in js['items']: item['$hz_v$'] = 0

  r.table(OrdersTable).insert(js['items'], durability="soft", return_changes=False, conflict="replace").run(horizon_conn)

  print("Working on %s pages" % pageCount)

  workers = max(multiprocessing.cpu_count(), pageCount)

  work = split_list(range(2,pageCount+1), workers)

  with multiprocessing.Pool(processes=workers) as pool:

    results = [pool.apply_async(loadPages, ([work[i]])) for i in range(0, len(work))]

    print("Wrote %s documents" % (sum([res.get() for res in results])))

  print("Finished in %s seconds " % (time.perf_counter() - start))

  print("Starting aggregation")
  aggTimer = time.perf_counter()

  aggregates = (r.table(OrdersTable)
  .filter( lambda doc: (doc['buy'] == True) & (doc['price'] > 1 ) )
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
    'frequency': "minutes",
    'time': r.now(),
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
  .run(agg_conn, array_limit=300000, profile=Profile)
  )

  data = aggregates

  r.table(AggregateTable).insert(data, return_changes=False).run(horizon_conn)

  print("Aggregation finished in %s seconds" % (time.perf_counter() - aggTimer))
  print("Total time taken is %s seconds" % (time.perf_counter() - start))
  
  if PrintJSON:
    print("Dumping json data")
    with open('aggregates.json', 'w', newline='') as file:
      json.dump(data, file)

  if PrintCSV:
    print("Dumping csv data")
    filename = "./aggregates/aggregates_%s_%s_%s.csv" % (tt.tm_mday, tt.tm_hour, tt.tm_min)
    with open(filename, 'w', newline='') as csvfile:
      writer = csv.DictWriter(csvfile, delimiter=',', quotechar='"', fieldnames=data[0].keys(), quoting=csv.QUOTE_MINIMAL)
      writer.writeheader()
      for row in data:
        writer.writerow(row)

