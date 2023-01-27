# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import unittest

from lifecycle.connect_record import CLIENT_ID_KEY, CONNECT_NUM_KEY, CONNECT_TIME_KEY, DISCONNECT_NUM_KEY, \
    DISCONNECT_TIME_KEY, STATUS_KEY
from lifecycle.lifecycle import update

MSG_CLIENT_ID_KEY = "clientId"
TEST_CLIENT_ID = "test-id"
TIMESTAMP_KEY = "timestamp"
TEST_TIMESTAMP = 1671506794644
EVENT_TYPE_KEY = "eventType"
EVENT_TYPE_CONNECTED = "connected"
EVENT_TYPE_DISCONNECTED = "disconnected"
VERSION_NUMBER_KEY = "versionNumber"


class DynamoDBTableMock:
    """ A DynamoDB mock-up for testing """
    def __init__(self):
        self.table = {}

    def reset(self):
        self.table = {}

    def put_item(self, Item):
        self.table[Item[CLIENT_ID_KEY]] = Item

    def get_item(self, Key):
        client_id = Key[CLIENT_ID_KEY]
        if client_id not in self.table:
            return {}
        return {"Item": self.table[client_id]}


table = DynamoDBTableMock()


def lambda_handler(event, context):
    update(event, table)


class MyTestCase(unittest.TestCase):
    def setUp(self) -> None:
        table.reset()

    def _assert_record(self, status, connect_timestamp, disconnect_timestamp, connect_num, disconnect_num):
        record = table.get_item({CLIENT_ID_KEY: TEST_CLIENT_ID})["Item"]
        self.assertEqual(record[STATUS_KEY], status)
        if connect_timestamp is not None:
            self.assertEqual(record[CONNECT_NUM_KEY], connect_num)
            self.assertEqual(record[CONNECT_TIME_KEY], connect_timestamp)
        if disconnect_timestamp is not None:
            self.assertEqual(record[DISCONNECT_NUM_KEY], disconnect_num)
            self.assertEqual(record[DISCONNECT_TIME_KEY], disconnect_timestamp)

    def test_connected_only(self):
        msg1 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP,
            EVENT_TYPE_KEY: EVENT_TYPE_CONNECTED,
            VERSION_NUMBER_KEY: 0
        }
        lambda_handler(msg1, None)
        self._assert_record("Connected", TEST_TIMESTAMP, None, 0, None)

    def test_disconnected_only(self):
        msg1 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP,
            EVENT_TYPE_KEY: EVENT_TYPE_DISCONNECTED,
            VERSION_NUMBER_KEY: 0
        }
        lambda_handler(msg1, None)
        self._assert_record("Disconnected", None, TEST_TIMESTAMP, None, 0)

    def test_connect_disconnect(self):
        msg1 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP,
            EVENT_TYPE_KEY: EVENT_TYPE_CONNECTED,
            VERSION_NUMBER_KEY: 0
        }
        msg2 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP + 1,
            EVENT_TYPE_KEY: EVENT_TYPE_DISCONNECTED,
            VERSION_NUMBER_KEY: 0
        }
        lambda_handler(msg1, None)
        lambda_handler(msg2, None)
        self._assert_record("Disconnected", TEST_TIMESTAMP, TEST_TIMESTAMP + 1, 0, 0)

    def test_connect_disconnect_inverted(self):
        msg1 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP,
            EVENT_TYPE_KEY: EVENT_TYPE_CONNECTED,
            VERSION_NUMBER_KEY: 0
        }
        msg2 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP + 1,
            EVENT_TYPE_KEY: EVENT_TYPE_DISCONNECTED,
            VERSION_NUMBER_KEY: 0
        }
        lambda_handler(msg2, None)
        lambda_handler(msg1, None)
        self._assert_record("Disconnected", TEST_TIMESTAMP, TEST_TIMESTAMP + 1, 0, 0)

    def test_connect_disconnect_duplicated1(self):
        msg1 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP,
            EVENT_TYPE_KEY: EVENT_TYPE_CONNECTED,
            VERSION_NUMBER_KEY: 0
        }
        msg2 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP + 1,
            EVENT_TYPE_KEY: EVENT_TYPE_DISCONNECTED,
            VERSION_NUMBER_KEY: 0
        }
        lambda_handler(msg1, None)
        lambda_handler(msg1, None)
        lambda_handler(msg2, None)
        self._assert_record("Disconnected", TEST_TIMESTAMP, TEST_TIMESTAMP + 1, 0, 0)

    def test_connect_disconnect_duplicated2(self):
        msg1 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP,
            EVENT_TYPE_KEY: EVENT_TYPE_CONNECTED,
            VERSION_NUMBER_KEY: 0
        }
        msg2 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP + 1,
            EVENT_TYPE_KEY: EVENT_TYPE_DISCONNECTED,
            VERSION_NUMBER_KEY: 0
        }
        lambda_handler(msg1, None)
        lambda_handler(msg2, None)
        lambda_handler(msg1, None)
        self._assert_record("Disconnected", TEST_TIMESTAMP, TEST_TIMESTAMP + 1, 0, 0)

    def test_connect_disconnect_duplicated3(self):
        msg1 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP,
            EVENT_TYPE_KEY: EVENT_TYPE_CONNECTED,
            VERSION_NUMBER_KEY: 0
        }
        msg2 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP + 1,
            EVENT_TYPE_KEY: EVENT_TYPE_DISCONNECTED,
            VERSION_NUMBER_KEY: 0
        }
        lambda_handler(msg1, None)
        lambda_handler(msg2, None)
        lambda_handler(msg2, None)
        self._assert_record("Disconnected", TEST_TIMESTAMP, TEST_TIMESTAMP + 1, 0, 0)

    def test_connect_disconnect_connect(self):
        msg1 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP,
            EVENT_TYPE_KEY: EVENT_TYPE_CONNECTED,
            VERSION_NUMBER_KEY: 0
        }
        msg2 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP + 1,
            EVENT_TYPE_KEY: EVENT_TYPE_DISCONNECTED,
            VERSION_NUMBER_KEY: 0
        }
        msg3 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP + 2,
            EVENT_TYPE_KEY: EVENT_TYPE_CONNECTED,
            VERSION_NUMBER_KEY: 2
        }
        lambda_handler(msg1, None)
        lambda_handler(msg2, None)
        lambda_handler(msg3, None)
        self._assert_record("Connected", TEST_TIMESTAMP + 2, TEST_TIMESTAMP + 1, 2, 0)

    def test_connect_disconnect_connect_inverted1(self):
        msg1 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP,
            EVENT_TYPE_KEY: EVENT_TYPE_CONNECTED,
            VERSION_NUMBER_KEY: 0
        }
        msg2 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP + 1,
            EVENT_TYPE_KEY: EVENT_TYPE_DISCONNECTED,
            VERSION_NUMBER_KEY: 0
        }
        msg3 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP + 2,
            EVENT_TYPE_KEY: EVENT_TYPE_CONNECTED,
            VERSION_NUMBER_KEY: 2
        }
        lambda_handler(msg1, None)
        lambda_handler(msg3, None)
        lambda_handler(msg2, None)
        self._assert_record("Connected", TEST_TIMESTAMP + 2, TEST_TIMESTAMP + 1, 2, 0)

    def test_connect_disconnect_connect_inverted2(self):
        msg1 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP,
            EVENT_TYPE_KEY: EVENT_TYPE_CONNECTED,
            VERSION_NUMBER_KEY: 0
        }
        msg2 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP + 1,
            EVENT_TYPE_KEY: EVENT_TYPE_DISCONNECTED,
            VERSION_NUMBER_KEY: 0
        }
        msg3 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP + 2,
            EVENT_TYPE_KEY: EVENT_TYPE_CONNECTED,
            VERSION_NUMBER_KEY: 2
        }
        lambda_handler(msg3, None)
        lambda_handler(msg2, None)
        lambda_handler(msg1, None)
        self._assert_record("Connected", TEST_TIMESTAMP + 2, TEST_TIMESTAMP + 1, 2, 0)

    def test_five_messages(self):
        msg1 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP,
            EVENT_TYPE_KEY: EVENT_TYPE_CONNECTED,
            VERSION_NUMBER_KEY: 0
        }
        msg2 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP + 1,
            EVENT_TYPE_KEY: EVENT_TYPE_DISCONNECTED,
            VERSION_NUMBER_KEY: 0
        }
        msg3 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP + 2,
            EVENT_TYPE_KEY: EVENT_TYPE_CONNECTED,
            VERSION_NUMBER_KEY: 2
        }
        msg4 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP + 3,
            EVENT_TYPE_KEY: EVENT_TYPE_DISCONNECTED,
            VERSION_NUMBER_KEY: 2
        }
        msg5 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP + 4,
            EVENT_TYPE_KEY: EVENT_TYPE_CONNECTED,
            VERSION_NUMBER_KEY: 4
        }
        lambda_handler(msg1, None)
        lambda_handler(msg2, None)
        lambda_handler(msg3, None)
        lambda_handler(msg4, None)
        lambda_handler(msg5, None)
        self._assert_record("Connected", TEST_TIMESTAMP + 4, TEST_TIMESTAMP + 3, 4, 2)

    def test_five_messages_inverted(self):
        msg1 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP,
            EVENT_TYPE_KEY: EVENT_TYPE_CONNECTED,
            VERSION_NUMBER_KEY: 0
        }
        msg2 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP + 1,
            EVENT_TYPE_KEY: EVENT_TYPE_DISCONNECTED,
            VERSION_NUMBER_KEY: 0
        }
        msg3 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP + 2,
            EVENT_TYPE_KEY: EVENT_TYPE_CONNECTED,
            VERSION_NUMBER_KEY: 2
        }
        msg4 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP + 3,
            EVENT_TYPE_KEY: EVENT_TYPE_DISCONNECTED,
            VERSION_NUMBER_KEY: 2
        }
        msg5 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP + 4,
            EVENT_TYPE_KEY: EVENT_TYPE_CONNECTED,
            VERSION_NUMBER_KEY: 4
        }
        lambda_handler(msg1, None)
        lambda_handler(msg4, None)
        lambda_handler(msg3, None)
        lambda_handler(msg5, None)
        lambda_handler(msg2, None)
        self._assert_record("Connected", TEST_TIMESTAMP + 4, TEST_TIMESTAMP + 3, 4, 2)

    def test_connect_after_an_hour(self):
        msg1 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP,
            EVENT_TYPE_KEY: EVENT_TYPE_CONNECTED,
            VERSION_NUMBER_KEY: 10
        }
        msg2 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP + 1,
            EVENT_TYPE_KEY: EVENT_TYPE_DISCONNECTED,
            VERSION_NUMBER_KEY: 10
        }
        msg3 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP + 3600001,
            EVENT_TYPE_KEY: EVENT_TYPE_CONNECTED,
            VERSION_NUMBER_KEY: 0
        }
        lambda_handler(msg1, None)
        lambda_handler(msg2, None)
        lambda_handler(msg3, None)
        self._assert_record("Connected", TEST_TIMESTAMP + 3600001, TEST_TIMESTAMP + 1, 0, 10)

    def test_connect_disconnect_after_an_hour(self):
        msg1 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP,
            EVENT_TYPE_KEY: EVENT_TYPE_DISCONNECTED,
            VERSION_NUMBER_KEY: 10
        }
        msg2 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP + 3600000,
            EVENT_TYPE_KEY: EVENT_TYPE_CONNECTED,
            VERSION_NUMBER_KEY: 0
        }
        msg3 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP + 3600001,
            EVENT_TYPE_KEY: EVENT_TYPE_DISCONNECTED,
            VERSION_NUMBER_KEY: 0
        }
        lambda_handler(msg1, None)
        lambda_handler(msg2, None)
        lambda_handler(msg3, None)
        self._assert_record("Disconnected", TEST_TIMESTAMP + 3600000, TEST_TIMESTAMP + 3600001, 0, 0)

    def test_connect_disconnect_after_an_hour_inverted(self):
        msg1 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP,
            EVENT_TYPE_KEY: EVENT_TYPE_DISCONNECTED,
            VERSION_NUMBER_KEY: 10
        }
        msg2 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP + 3600000,
            EVENT_TYPE_KEY: EVENT_TYPE_CONNECTED,
            VERSION_NUMBER_KEY: 0
        }
        msg3 = {
            MSG_CLIENT_ID_KEY: TEST_CLIENT_ID,
            TIMESTAMP_KEY: TEST_TIMESTAMP + 3600001,
            EVENT_TYPE_KEY: EVENT_TYPE_DISCONNECTED,
            VERSION_NUMBER_KEY: 0
        }
        lambda_handler(msg1, None)
        lambda_handler(msg3, None)
        lambda_handler(msg2, None)
        self._assert_record("Disconnected", TEST_TIMESTAMP + 3600000, TEST_TIMESTAMP + 3600001, 0, 0)


if __name__ == '__main__':
    unittest.main()
