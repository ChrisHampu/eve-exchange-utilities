import asyncio
import sys
import os
import time

sys.path.append(os.path.dirname(os.path.realpath(__file__)))

from aggregation import settings as _settings, mail_interface as mail

config = _settings.Settings()

if __name__ == "__main__":

    loop = asyncio.get_event_loop()

    print("Running at %s" % config.utcnow)
    script_start = time.perf_counter()

    loop.run_until_complete(mail.MailInterface().handleMailUpdates())

    print("Finished in %s seconds" % (time.perf_counter() - script_start))

    loop.close()
