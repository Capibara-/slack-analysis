import calendar
import glob
import json
import os
import re
import uuid
from datetime import datetime
from multiprocessing import Pool

import requests

CHANNELS_DIR = '/Users/gabik/git/slack-stuff/2018-full'
CHANNELS_GLOB = '*.json'
USERS_FILE = './users.json'
INDEX_NAME = 'slack'
ES_URL = "http://localhost:9200"
ES_MAPPING = {
    "properties": {
        "timestamp": {
            "type": "date",
            "format": "strict_date_hour_minute_second"
        },
        "text": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 512
                }
            }
        },
        "is_thread": {
            "type": "boolean"
        }
    }
}


def main():
    pool = Pool(10)
    delete_index(INDEX_NAME)
    delete_index(INDEX_NAME + '-v2')
    put_mapping(INDEX_NAME, ES_MAPPING)

    users = get_user_data()
    for filepath in glob.iglob('{}/**/*.json'.format(CHANNELS_DIR)):
        with open(filepath, 'r') as channel_file:
            print("[*] Indexing {}.".format(filepath))
            channel_name = channel_name_from_path(filepath)
            msgs = json.loads(channel_file.read())
            filtered_msgs = [msg for msg in map(lambda m: extract_data(m, users, channel_name), msgs) if
                             msg['text']]

            pool.map(write_msg_to_es, filtered_msgs)


# Helpers:
def get_user_data():
    with open(USERS_FILE, 'r') as users_file:
        return json.loads(users_file.read())


def channel_name_from_path(fname):
    return os.path.basename(os.path.split(fname)[0])


# Slack alert msg processing:
def alert_status_for(msg):
    alert_open_regex = r'CRIT:.*'
    alert_close_regex = r'OK:.*'
    alert_ack_regex = r'.*acknowledged in.*'
    if re.match(alert_open_regex, msg['text']):
        return "OPEN"
    elif re.match(alert_close_regex, msg['text']):
        return "CLOSE"
    elif re.match(alert_ack_regex, msg['text']):
        return "ACK"
    return None


def extract_data(msg, users, channel_name):
    text = relevant_text(msg)
    date_data = timestamp_for(msg)
    return  {'user': username_for(msg, users),
             'is_thread': check_is_thread(msg),
             'thread_ts': extract_thread_ts(msg),
            'channel': channel_name,
            'text': text,
            'alert_status': alert_status_for(msg),
            'skip_error_info': skip_error_info(text),
            'weekday': date_data['weekday'],
            'afterhours': date_data['afterhours'],
            'timestamp': date_data['timestamp']}



def check_is_thread(msg):
    return 'thread_ts' in msg


def extract_thread_ts(msg):
    return msg['thread_ts'] if 'thread_ts' in msg  else None

def timestamp_for(msg):
    date = datetime.fromtimestamp(int(float(msg['ts'])))
    weekday = calendar.day_name[date.weekday()]
    afterhours = date.hour >= 19 or weekday == "Friday" or weekday == "Saturday"
    return {'timestamp': date.strftime("%Y-%m-%dT%H:%M:%S"),
            'weekday': weekday,
            'afterhours': afterhours}


def skip_error_info(msg):
    if 'skipped' in msg.lower():
        match = re.match(".*db-mysql-(.*)\..*\.wixprod\.net.*", msg, re.DOTALL)
        if match:
            return {'db_name': match.group(1)}


def username_for(msg, users):
    if 'bot_id' in msg:
        try:
            profiles = [user['profile'] for user in users]
            return next(profile['real_name'] for profile in profiles if
                        'bot_id' in profile and profile['bot_id'] == msg['bot_id'])
        except Exception:
            return msg['bot_id']
    if 'user' in msg:
        try:
            profile = next(user['profile'] for user in users if 'id' in user and user['id'] == msg['user'])
            return profile['real_name']
        except Exception as e:
            return msg['user']
    return "UNKNOWN"


def relevant_text(msg):
    text = []
    if 'text' in msg:
        text += [msg['text'].replace('\n', '')]

    if 'attachments' in msg:
        text += [attachment['text'].replace('\n', '') for attachment in msg['attachments'] if 'text' in attachment]
        text += [attachment['pretext'].replace('\n', '') for attachment in msg['attachments'] if 'pretext' in attachment]

    return '; '.join(text)


# ElasticSearch:
def write_msg_to_es(msg):
    put_to_es(msg, INDEX_NAME, str(uuid.uuid4()))


def delete_index(index_name):
    requests.delete('{}/{}'.format(ES_URL, index_name))


def put_mapping(index_name, mapping):
    index_url = '{}/{}'.format(ES_URL, index_name)
    url = '{}/_mapping/_doc'.format(index_url)
    response = requests.put(index_url)
    response.raise_for_status()

    response = requests.put(url, json=mapping)
    response.raise_for_status()
    return response.status_code == 200


def put_to_es(msg, type_name, id):
    url = '{}/{}/_doc/{}'.format(ES_URL, type_name, id)
    response = requests.put(url, json=msg)
    return response.status_code == 200


if __name__ == '__main__':
    main()
