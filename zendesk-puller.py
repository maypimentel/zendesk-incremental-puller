import requests
import json
import logging
import pydash
import urllib
import os
from time import sleep
from decouple import config
from pymongo import MongoClient

logger = logging.getLogger(__name__)
ZENDESK_USER = config('ZENDESK_USER', cast=str)
ZENDESK_TOKEN = config('ZENDESK_TOKEN', cast=str)
ZENDESK_URL = config('ZENDESK_URL', cast=str)
SERVICE_REQUEST_TIMEOUT = config('SERVICE_REQUEST_TIMEOUT', default=60000, cast=int)
MONGODB_URI = config('MONGODB_URI', cast=str)
MONGODB_DB = config('MONGODB_DB', cast=str)
client = MongoClient(MONGODB_URI)
db = client[MONGODB_DB]
try:
    db.tickets.create_index([('id', pymongo.ASCENDING)], unique=True)
    db.users.create_index([('id', pymongo.ASCENDING)], unique=True)
    db.users.create_index([('email', pymongo.ASCENDING)])
except Exception as err:
    logger.error('Not existis collection: {}'.format(err))

ticketFileds = [
    'id',
    'requester_id',
    'url',
    'external_id',
    'type',
    'subject',
    'raw_subject',
    'description',
    'priority',
    'status',
    'recipient',
    'is_public',
    'created_at',
    'updated_at',
    'via',
    'tags'
]

ticketCustomFields = [
    ('id_pedido', 22677584),
    ('voucher', 22677664),
    ('motivo', 22330734)
]

userFields = [
    'id',
    'email',
    'name',
    'created_at',
    'updated_at',
    'locale',
    'phone',
    'url',
    'active',
    'suspended',
    'role'
]

def make_request(startTime=None, cursor=None):
    try:
        qs = {
            'include': 'users',
            'per_page': 1000,
        }
        if cursor:
            qs['cursor'] = cursor
        elif startTime:
            qs['start_time'] = startTime
        response = requests.get(
            url='{}/api/v2/incremental/tickets/cursor.json'.format(ZENDESK_URL),
            params=qs,
            auth=(
                ZENDESK_USER,
                ZENDESK_TOKEN
            ),
            headers={"content-type": "application/json"},
            timeout=SERVICE_REQUEST_TIMEOUT
        )
        if response.status_code == requests.codes.ok:
            print(response)
            return response.json()
        elif response.status_code == 429:
            sleep(int(response.headers['retry-after']))
            return make_request(startTime)
    except requests.Timeout as err:
        logger.error('Timeout error, retaing in 10s')
        sleep(120)
        return make_request(startTime)
    except Exception as err:
        logger.error('Error: {}'.format(err))


def manage_users(users):
    filteredUsers = []
    for user in users:
        if pydash.get(user, 'role') == 'end-user':
            filteredUsers.append({ k: v for (k, v) in user.items() if k in userFields })
    for user in filteredUsers:
        if db.users.find_one({'id': user.get('id')}):
            del user['id']
            db.users.update_one({'id': user.get('id')}, {'$set': user})
        else:
            db.users.insert_one(user)
    return filteredUsers

def manage_tickets(tickets):
    filteredTickets = []
    for ticket in tickets:
        filterTicket = {}
        for key in ticket.keys():
            if key in ticketFileds:
                filterTicket[key] = urllib.parse.unquote(ticket[key]) if type(ticket[key]) is str else ticket[key]
            if key == 'custom_fields':
                for custom in ticket[key]:
                    for n, i in ticketCustomFields:
                        if custom['id'] == i:
                            filterTicket[n] = urllib.parse.unquote(custom['value']) if type(custom['value']) is str else custom['value']
            if db.tickets.find_one({'id': filterTicket.get('id')}):
                db.tickets.update_one({'id': filterTicket.get('id')}, {'$set': filterTicket})
            else:
                db.tickets.insert_one(filterTicket)
        filteredTickets.append(filterTicket)
    return filteredTickets

def main():
    endOfStream = False
    cursor = False
    startTime = 1583106695
    while not endOfStream:
        response = make_request(startTime, cursor)
        startTime = pydash.get(response, 'end_time')
        cursor = pydash.get(response, 'after_cursor')
        endOfStream = pydash.get(response, 'end_of_stream')
        # Testing with only one request
        # endOfStream = True
        users = pydash.get(response, 'users')
        if users and len(users) > 0:
            response['users'] = manage_users(users)
        tickets = pydash.get(response, 'tickets')
        if tickets and len(tickets) >  0:
            response['tickets'] = manage_tickets(tickets)


if __name__ == "__main__":
    main()