import os
import logging
import threading

from common import middleware, fruit_item
from common.message_protocol.internal import ProtocolMessage

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.items_by_id = {}

    def _process_data(self, msg):
        logging.info(f"Process data")
        items = self.items_by_id.get(msg.id, {})
        [fruit, amount] = msg.fruit_items.pop()

        items[fruit] = items.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

        self.items_by_id[msg.id] = items

    def _process_eof(self, msg):
        logging.info(f"Broadcasting data messages")
        items = self.items_by_id.get(msg.id, {})
        for final_fruit_item in items.values():
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(
                    ProtocolMessage(msg.id, [[final_fruit_item.fruit, final_fruit_item.amount]]).serialize()
                )

        logging.info(f"Broadcasting EOF message")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(msg.serialize())


    def process_data_messsage(self, message, ack, nack):
        msg = ProtocolMessage.deserialize(message)
        if msg.is_eof():
            self._process_eof(msg)
        else:
            self._process_data(msg)
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
