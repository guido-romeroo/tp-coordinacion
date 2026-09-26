import os
import logging
import bisect

from common import middleware, fruit_item
from common.message_protocol.internal import ProtocolMessage, ProtocolMessageType

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top_by_id = {}
        self.eof_amount_by_id = {}

    def _process_data(self, msg: ProtocolMessage):
        logging.info("Processing data message")
        [fruit, amount] = msg.payload.pop()
        top = self.fruit_top_by_id.get(msg.client_id, [])
        item = fruit_item.FruitItem(fruit, amount)
        for i in range(len(top)):
            if top[i].fruit == fruit:
                item = top.pop(i)
                item = item + fruit_item.FruitItem(
                    fruit, amount
                )
                break
        bisect.insort(top, item)
        self.fruit_top_by_id[msg.client_id] = top

    # TODO: contar la cantidad de eofs recibidos y calcular cuando sea igual a sum_amount
    def _process_eof(self, msg: ProtocolMessage):
        self.eof_amount_by_id[msg.client_id] = self.eof_amount_by_id.get(msg.client_id, 0) + 1
        logging.info("Received EOF")
        if self.eof_amount_by_id[msg.client_id] < SUM_AMOUNT:
            return
        top = self.fruit_top_by_id.get(msg.client_id, [])
        fruit_chunk = list(top[-TOP_SIZE:])
        fruit_chunk.reverse()
        fruit_top = list(
            map(
                lambda fruit_item: [fruit_item.fruit, fruit_item.amount],
                fruit_chunk,
            )
        )
        self.output_queue.send(ProtocolMessage(ProtocolMessageType.FRUITS, msg.client_id, msg.msg_id, fruit_top).serialize())
        del self.fruit_top_by_id[msg.client_id]
        del self.eof_amount_by_id[msg.client_id]

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        msg = ProtocolMessage.deserialize(message)
        if msg.is_eof():
            self._process_eof(msg)
        else:
            self._process_data(msg)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
