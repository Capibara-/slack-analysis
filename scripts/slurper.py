import json
import os
import time

import requests
import urllib

CHANNELS_FILE = './channels.json'
OUTPUT_DIR = "./channel-data"
RELEVANT_CHANNELS = ['html-server', 'html-server-urgent']
TOKEN = "YEA_RIGHT"
BASE_URL = "https://slack.com/api/channels.history"
PAGE_SIZE = 1000
MAX_MSGS = PAGE_SIZE * 2
SLEEP_INTERVAL_MS = 1000


def main():
    channels = get_channel_data()
    create_dir_if_not_exist(OUTPUT_DIR)

    for channel_id, channel_name in channels:
        print("[+] Downloading data for channel {} AKA {}.".format(channel_id, channel_name))
        page = get_page(channel_id)
        msgs = page
        for i in range(0, MAX_MSGS // PAGE_SIZE - 1):
            time.sleep(SLEEP_INTERVAL_MS / 1000.0)
            timestamp = page[-1]['ts']
            page = get_page(channel_id, timestamp)
            msgs += page

            if len(page) < PAGE_SIZE:
                break

        print("[+] Writing {} msgs for channel {}.".format(len(msgs), channel_name))
        write_json(msgs, os.path.join(OUTPUT_DIR, '{}.json'.format(channel_name)))


def get_page(channel, timestamp=None):
    if timestamp:
        params = {"token": TOKEN, "count": PAGE_SIZE, "channel": channel, "latest": timestamp, "inclusive": False}
    else:
        params = {"token": TOKEN, "count": PAGE_SIZE, "channel": channel}
    url = '{}?{}'.format(BASE_URL, urllib.urlencode(params))
    response = requests.get(url)
    response.raise_for_status()
    return response.json()['messages']


def create_dir_if_not_exist(dirname):
    if not os.path.exists(dirname):
        os.makedirs(dirname)


def write_json(data, filepath):
    with open(filepath, "w+") as fout:
        fout.write(json.dumps(data))


def get_channel_data():
    with open(CHANNELS_FILE, 'r') as chan_file:
        content = json.loads(chan_file.read())
        return [(chan['id'], chan['name']) for chan in content if chan['name'] in RELEVANT_CHANNELS]


if __name__ == '__main__':
    main()
