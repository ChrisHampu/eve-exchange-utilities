import sys
import time
from datetime import datetime
import rethinkdb as r

# arguments
# 1: horizonDBName
# 2: aggregateTableName

HorizonDB = sys.argv[1]
AggregateTable = sys.argv[2]

if __name__ == '__main__':

  agg_conn, start  =  (r.connect(db=HorizonDB), time.perf_counter())

  dt = datetime.now()

  print("Executing at %s" % dt)

  print("Starting hourly aggregation")

  aggregates = (r.table(AggregateTable)
  .filter(lambda doc:
    doc["frequency"].eq["minutes"] & r.now().sub(doc["time"]).lt(3600)
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
    'tradeValue': left["tradeValue"].add(right["tradeValue"])
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
    'frequency': "hours",
    'time': r.now(),
    '$hz_v$': 0
  })
  .run(agg_conn)
  )

  data = aggregates

  r.table(AggregateTable).insert(data, return_changes=False).run(agg_conn)

  print("Total time taken is %s seconds" % (time.perf_counter() - start))

