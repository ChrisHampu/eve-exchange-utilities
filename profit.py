import sys
import requests
import time
from datetime import datetime, timedelta, tzinfo
import rethinkdb as r

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

# arguments
# 1: horizonDBName
# 2: settingsTableName
# 3: profitChartTableName
# 4: topItemsTableName

HorizonDB = sys.argv[1]
SettingsTable = sys.argv[2]
ProfitChartTable = sys.argv[3]
TopitemsTable = sys.argv[4]
AlltimeTable = sys.argv[5]
TransactionsTable = sys.argv[6]
rowCount = 1000

def getTransactions(fromID=None):

  auth = (user['eveApiKey']['characterID'], user['eveApiKey']['keyID'], user['eveApiKey']['vCode'], rowCount, "" if fromID is None else "&fromID="+str(fromID))

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

def getAllTransactions():

  fromID = None
  journal = getTransactions(fromID)

  if len(journal) == 0:
    return []

  fromID = journal[-1].attrib['transactionID']
  lastPull = len(journal)
  journal[0].attrib['transactionID']

  while lastPull >= rowCount-1:
    newJournal = getTransactions(fromID)
    lastPull = len(newJournal)
    if lastPull == 0:
      break
    journal.extend(newJournal)
    fromID = newJournal[-1].attrib['transactionID']

  if lastPull > 0:
    newJournal = getTransactions(fromID)
    journal.extend(newJournal)

  return journal

def getJournalEntries():

  auth = (user['eveApiKey']['characterID'], user['eveApiKey']['keyID'], user['eveApiKey']['vCode'])

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

  data = [{key:row.attrib[key] for key in ('amount', 'refTypeID', 'date')} for row in rows if time.strptime(row.attrib['date'], '%Y-%m-%d %H:%M:%S') > offset]

  return data

if __name__ == '__main__':

  horizon_conn, start  =  (r.connect(db=HorizonDB), time.perf_counter())

  print("Executing at %s" %  datetime.now())

  now = datetime.now(r.make_timezone('00:00'))
  nowtuple = now.utctimetuple()

  dt = datetime.utcnow() - timedelta(hours=1)
  offset = dt.utctimetuple()

  settings = list(r.table(SettingsTable).run(horizon_conn))

  results = []
  transactions = []
  salesResults = dict()
  profitResults = dict()

  for user in settings:
    
    if 'eveApiKey' not in user:
      continue

    if len(user['eveApiKey']['keyID']) and len(user['eveApiKey']['vCode']):

      rows = getAllTransactions()
      journal = getJournalEntries()

      data = [{key:row.attrib[key] for key in ('typeName', 'journalTransactionID', 'transactionType', 'price', 'quantity', 'typeID', 'transactionDateTime')} for row in rows]

      sells = [x for x in data if time.strptime(x['transactionDateTime'], '%Y-%m-%d %H:%M:%S') > offset and x['transactionType'] == 'sell']
      buys = [x for x in data if time.strptime(x['transactionDateTime'], '%Y-%m-%d %H:%M:%S') > offset and x['transactionType'] == 'buy']

      groupedSells = dict()

      result = { 'userID': user['userID'], 'profit': 0, 'taxes': 0, '$hz_v$': 0, 'time': now , 'frequency': 'hourly' }

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
          'time': datetime.strptime(groupedSells[typeID][0]['transactionDateTime'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=datetime.now(r.make_timezone('00:00')).tzinfo)
        })

      taxEntries = [x for x in journal if x['refTypeID'] == '54']
      brokerEntries = [x for x in journal if x['refTypeID'] == '46']

      tax = sum(float(x['amount']) for x in taxEntries)
      broker = sum(float(x['amount']) for x in brokerEntries)
      totalProfit = sum(float(x['totalProfit']) for x in sales)

      result['taxes'] = tax
      result['broker'] = broker
      result['profit'] = totalProfit

      salesResults[user['userID']] = sales
      profitResults[user['userID']] = {'profit': result['profit'], 'taxes': result['taxes'], 'broker': result['broker']}

      results.append(result)

  r.table(ProfitChartTable).insert(results, durability="soft", return_changes=False, conflict="replace").run(horizon_conn)

  userList = [user['userID'] for user in settings if user['eveApiKey'] is not None and len(user['eveApiKey']['keyID']) > 0]

  top_items = list(r.table(TopitemsTable).get_all(r.args(userList), index='userID').run(horizon_conn))

  for charID in userList:

    _charID = charID

    doc = next((x for x in top_items if x['userID'] == _charID), None)

    sales = salesResults[_charID] if _charID in salesResults else None

    if sales is None:
      print("Failed to match results for user %s" % _charID)
      continue

    data = [{key:row[key] for key in ('totalProfit', 'quantity', 'avgPerUnit', 'type', 'name')} for row in sales]

    transactions.extend([{**{key:row[key] for key in ('totalProfit', 'quantity', 'avgPerUnit', 'time', 'name', 'type')}, **{'userID': _charID, '$hz_v$': 0}} for row in sales])

    # create new top list
    if doc is None:
      
      print("Creating new top items record for user %s" % _charID)
      r.table(TopitemsTable).insert({'$hz_v$': 0, 'userID': _charID, 'items': data}).run(horizon_conn)

    # modify/add to existing top list
    else:
      for item in data:

        find = [(i, j) for i, j in enumerate(doc['items']) if j['type'] == item['type']]
        
        if len(find) == 0:
          doc['items'].append(item)

        else:
          index, top = find[0]

          top['quantity'] += item['quantity']
          top['totalProfit'] += item['totalProfit']
          top['avgPerUnit'] = top['totalProfit'] / top['quantity']

          doc['items'][index] = top

      r.table(TopitemsTable).update(doc).run(horizon_conn)
    
    # modify/add to alltime profit record
    profit = profitResults[_charID] if _charID in profitResults else None

    if profit is None:
      continue

    existingDoc = list(r.table(AlltimeTable).filter(lambda doc: doc['userID'] == _charID).limit(1).run(horizon_conn))

    if len(existingDoc) == 0:
      print("Creating new alltime record for user %s" % _charID)

      newDoc = {
        'userID': _charID, 
        'alltime': profit, 
        'day': profit, 
        'week': profit,
        'month': profit,
        'biannual': profit
      }

      r.table(AlltimeTable).insert(newDoc, durability="soft", return_changes=False).run(horizon_conn)

    else:
      existingDoc = existingDoc[0]

      hourlyDocs = list(r.table(ProfitChartTable).filter(lambda doc: (doc['frequency'] == "hourly") & (doc['userID'] == _charID)).order_by(r.desc("time")).limit(24).run(horizon_conn))

      sumDocs = lambda docs: { 'profit': sum([i['profit'] for i in docs]), 'broker': sum([i['broker'] for i in docs]), 'taxes': sum([i['taxes'] for i in docs])}

      pastDay = sumDocs(hourlyDocs)

      r.table(AlltimeTable).get(existingDoc['id']).update({'alltime': { 'profit': r.row['alltime']['profit'] + profit['profit'], 'broker': r.row['alltime']['broker'] + profit['broker'], 'taxes': r.row['alltime']['taxes'] + profit['taxes'] }, 'day': pastDay}).run(horizon_conn)

  r.table(TransactionsTable).insert(transactions, durability="soft", return_changes=False, conflict="replace").run(horizon_conn)

  # Start daily task at 11 AM UTC (EVE downtime)
  if nowtuple.tm_hour == 11:
  #if True:

    print("Performing daily task")

    dailyDocs = []

    # Aggregate hourly documents into a daily document
    for user in settings:

      userID = user['userID']
      
      if 'eveApiKey' not in user:
        continue

      if len(user['eveApiKey']['keyID']) and len(user['eveApiKey']['vCode']):

        hourlyDocs = list(r.table(ProfitChartTable).filter(lambda doc: (doc['frequency'] == "hourly") & (doc['userID'] == userID)).order_by(r.desc("time")).limit(24).run(horizon_conn))

        if len(hourlyDocs) == 0:
          continue

        dailyDoc = { 'userID': userID, 'profit': 0, 'taxes': 0, 'broker': 0, '$hz_v$': 0, 'time': now , 'frequency': 'daily' }

        for doc in hourlyDocs:
          dailyDoc['profit'] += doc['profit']
          dailyDoc['broker'] += doc['broker']
          dailyDoc['taxes'] += doc['taxes']

        dailyDocs.append(dailyDoc)

    # Insert new daily results
    if len(dailyDocs) > 0:
      r.table(ProfitChartTable).insert(dailyDocs, durability="soft", return_changes=False).run(horizon_conn)

    # Update alltime data for each user
    for user in settings:

      userID = user['userID']
      
      if 'eveApiKey' not in user:
        continue

      if len(user['eveApiKey']['keyID']) and len(user['eveApiKey']['vCode']):

        dailyDocs = [{'profit': i['profit'], 'broker': i['broker'], 'taxes': i['taxes']} for i in list(r.table(ProfitChartTable).filter(lambda doc: (doc['frequency'] == "daily") & (doc['userID'] == userID)).order_by(r.desc("time")).limit(90).run(horizon_conn))]

        sumDocs = lambda docs: { 'profit': sum([i['profit'] for i in docs]), 'broker': sum([i['broker'] for i in docs]), 'taxes': sum([i['taxes'] for i in docs])}

        pastWeek = sumDocs(dailyDocs[:7])
        pastMonth = sumDocs(dailyDocs[:30])
        pastBiannual = sumDocs(dailyDocs[:90])

        existingDoc = list(r.table(AlltimeTable).filter(lambda doc: doc['userID'] == userID).limit(1).run(horizon_conn))

        if len(existingDoc) == 0:
          print("No alltime doc for user %s" % userID)
          continue

        else:
          existingDoc = existingDoc[0]

          r.table(AlltimeTable).get(existingDoc['id']).update({'week': pastWeek, 'month': pastMonth, 'biannual': pastBiannual}).run(horizon_conn)






'''
overall profit calc
  
every 24 hours:

1.
sum the past 24 profit_chart documents
insert to database
sum the past 7 of the 24hr reports
insert to database as past 7 day report
sum the past 4 of the 7 day reports
insert to database as past 30 day report
sum all the past 30 day reports 

2. increment alltime report using past 24 hour data 

structure
{
  "userID": id,
  "alltime": double,
  "24_hours": double,
  "7_days": double,
  "30 days": double,
  "90 days": double
}

'''