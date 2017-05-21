from requests import Request, Session
import time
import functools
import traceback
import json

from . import settings as _settings, esi, redis_interface

mail_delay = 12 # seconds between mail sends
mails_per_run = 25 # number of mails to send in a 5 minute interval

config = _settings.Settings()
cache = redis_interface.CacheInterface(None)

class MailInterface:
    def __init__(self):
        self.esi_access_token = esi.get_esi_access_token()
        cached_id = cache.redis.get('esi_mail_sender')
        self.sender_id = int(cached_id) if cached_id is not None else None
        self.mail_wait_queue = "esi_mail_wait_queue"
        self.mail_work_queue = "esi_mail_work_queue"

    def queueMail(self, recipient_id, subject, body):

        obj = {
            'recipient_id': recipient_id,
            'body': body,
            'subject': subject
        }

        cache.redis.lpush(self.mail_wait_queue, json.dumps(obj))

        print('Queued new mail for %s' % recipient_id)

    async def handleMailUpdates(self):

        if self.esi_access_token == None:
            print("ESI access token unavailable!")
            return

        mail_queue_size = cache.redis.llen(self.mail_wait_queue)
        mails_sent = 0

        if mail_queue_size == 0:
            print("No mails to send")
            return

        if self.sender_id is None:
            print("No mail sender id")
            return

        headers = {
            **config.standard_headers,
            'Content-Type': 'application/json',
            'Authorization': 'Bearer %s' % self.esi_access_token
        }

        s = Session()
        prepped_req = s.prepare_request(Request('POST', 'https://esi.tech.ccp.is/v1/characters/%s/mail/' % self.sender_id, headers=headers, data="{}"))

        while cache.redis.llen(self.mail_wait_queue) > 0 and mails_sent < mails_per_run:

            try:
                work = cache.redis.rpoplpush(self.mail_wait_queue, self.mail_work_queue)

                if work is None:
                    return

                mail = json.loads(str(work, 'utf-8'))

                if 'body' in mail and 'recipient_id' in mail and 'subject' in mail:

                    prepped_req.body = json.dumps({
                        'approved_cost': 0,
                        'body': mail['body'],
                        'recipients': [
                            {
                                'recipient_id': mail['recipient_id'],
                                'recipient_type': 'character'
                            }
                        ],
                        'subject': mail['subject']
                    })

                    prepped_req.headers['Content-Length'] = len(prepped_req.body)

                    print("Sending mail to %s at %s" % (mail['recipient_id'], config.utcnow))

                    resp = s.send(prepped_req, timeout=5)

                    resp.raise_for_status()

                    print("Sent mail id %s" % resp.text)

                    mails_sent += 1
            except:
                traceback.print_exc()

            cache.redis.lrem(self.mail_work_queue, 1, work)

            time.sleep(mail_delay) 
