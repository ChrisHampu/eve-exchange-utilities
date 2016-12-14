import asyncio
import csv
import time
import sys
import traceback
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

async def DumpCSV():

  with open('daily_aggregates_%s.csv' % typeid, 'w', newline='') as file:

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