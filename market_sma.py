import rethinkdb as r
import time

def getConnection():
  return r.connect(db='horizon_test')

if __name__ == '__main__':

  pageTime = time.perf_counter()

  spread_sma = dict()
  volume_sma = dict()
  grouped = dict()

  _slice = 7

  docs = r.table('aggregates_daily').run(getConnection())

  for i in docs:

    if i['type'] in grouped:
      grouped[i['type']].append({arg: i[arg] for arg in ('time', 'spread', 'tradeVolume')})
    else:
      grouped[i['type']] = [{arg: i[arg] for arg in ('time', 'spread', 'tradeVolume')}]

  for g in grouped:

    _sorted = sorted(grouped[g], key=lambda doc: doc['time'], reverse=True)[:_slice-1]

    if g == 29668:
      print(len(_sorted))

    spread_sma[g] = sum([i['spread'] for i in _sorted])
    volume_sma[g] = sum([i['tradeVolume'] for i in _sorted])

  print(volume_sma[29668] / _slice)
  print(spread_sma[29668] / _slice)

  print("%s seconds" % (time.perf_counter() - pageTime))