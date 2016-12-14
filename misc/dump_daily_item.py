import asyncio
import csv
import time
import sys
import traceback
import os
import json
from motor import motor_asyncio

client = motor_asyncio.AsyncIOMotorClient()

database = client.eveexchange
aggregates_daily = database.aggregates_daily

try:
  typeid = int(sys.argv[1])
except:
  print("Pass typeid as command line argument")
  sys.exit()

headers = ['type','region','time','buyMin','buyAvg','buyMax','buyPercentile','sellMin',
          'sellAvg','sellMax','sellPercentile','spread','spread_sma','tradeVolume','volume_sma','velocity',
          'spreadValue','tradeValue']

market_ids = []
market_id_to_name = {}

with open('../sde/market_groups.js', 'r', encoding='utf-8') as f:
    read_data = f.read()
    market_groups_json = read_data
    market_groups = json.loads(read_data)

    _getItems = lambda items: [x['id'] for x in items]

    def _getGroups(group, ids):
        if 'items' in group:
            ids.extend(_getItems(group['items']))
            for item in group['items']:
              market_id_to_name[int(item['id'])] = item['name']
        for _group in group['childGroups']:
            _getGroups(_group, ids)

    for group in market_groups:
        _getGroups(group, market_ids)

async def DumpCSV():

  with open('daily_aggregates_%s.csv' % market_id_to_name.get(typeid, typeid), 'w', newline='') as file:

    writer = csv.DictWriter(file, delimiter=',', quotechar='"',
                            quoting=csv.QUOTE_MINIMAL, fieldnames=headers)

    writer.writeheader()

    try:
      async for doc in aggregates_daily.find({'type': typeid}):

        for region in doc.get('regions'):

          writer.writerow({**{
            'type': typeid,
            'time': doc['time'],
            'velocity': 0,
          }, **region})
    except:
      traceback.print_exc()

if __name__ == "__main__":

  loop = asyncio.get_event_loop()

  script_start = time.perf_counter()

  loop.run_until_complete(DumpCSV())

  print("Finished in %s seconds" % (time.perf_counter() - script_start))

  loop.close()