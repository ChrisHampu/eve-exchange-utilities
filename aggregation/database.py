from motor import motor_asyncio
from typing import Dict

class DatabaseConnector:
    def __init__(self):
        self.client = motor_asyncio.AsyncIOMotorClient()
        self.database = self.client.eveexchange
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

        self.settings_cache = None

    async def GetAllUserSettings(self) -> Dict:
        if self.settings_cache is not None:
            return self.settings_cache

        self.settings_cache = {}

        # Build a user id -> settings document map for easy access on demand
        async for user in self.settings.find():

            # Sanity check
            if 'user_id' not in user:
                continue

            self.settings_cache[user['user_id']] = user

        return self.settings_cache
