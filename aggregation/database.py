from motor import motor_asyncio
from typing import Dict
import traceback
import time

class DatabaseConnector:
    def __init__(self):
        self.client = motor_asyncio.AsyncIOMotorClient()
        self.database = self.client.test
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
        self.subscription = self.database.subscription
        self.notifications = self.database.notifications
        self.audit = self.database.audit_log
        self.user_assets = self.database.user_assets
        self.tickers = self.database.tickers
        self.alerts = self.database.alerts
        self.alerts_log = self.database.alerts_log

        self.settings_cache = None

        print("Initialized database connector")

    async def GetAllUserSettings(self) -> Dict:
        if self.settings_cache is not None:
            return self.settings_cache

        print("Loading all user settings from database")

        self.settings_cache = {}
        settings = None
        tries = 0

        while tries < 3:
            try:
                settings = await self.settings.find().to_list(length=None)
                if len(settings) > 0:
                    break
            except:
                traceback.print_exc()
                print("Failed to load user settings from database (tries=%s)" % tries)
                tries += 1
                time.sleep(1)

        if settings == None:
            print("Settings were not loaded after 3 tries. Aborting")
            return self.settings_cache

        # Build a user id -> settings document map for easy access on demand
        for user in settings:

            # Sanity check
            if 'user_id' not in user:
                continue

            self.settings_cache[user['user_id']] = user

        print("Successfully loaded user settings")

        return self.settings_cache

db = DatabaseConnector()
