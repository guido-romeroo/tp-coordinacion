import os
import logging
import heapq
import signal

from common import middleware, fruit_item
from common.message_protocol.internal import ProtocolMessage

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top_by_id = {}
        self.top_count_by_id = {}

    def merge_tops(self, msg: ProtocolMessage):
        logging.info("Merging tops")
        top = self.fruit_top_by_id.get(msg.client_id, [])
        if not top:
            return msg.payload
        return list(heapq.merge(top, msg.payload, key=lambda pair: pair[1], reverse=True))[0:TOP_SIZE]

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        msg = ProtocolMessage.deserialize(message)
        self.fruit_top_by_id[msg.client_id] = self.merge_tops(msg)
        self.top_count_by_id[msg.client_id] = self.top_count_by_id.get(msg.client_id, 0) + 1
        if self.top_count_by_id[msg.client_id] == AGGREGATION_AMOUNT:
            msg.payload = self.fruit_top_by_id[msg.client_id]
            self.output_queue.send(msg.serialize())
            del self.fruit_top_by_id[msg.client_id]
            del self.top_count_by_id[msg.client_id]
        ack()

    def handle_sigterm(self, _signum, _frame):
        logging.info("SIGTERM received, shutting down...")
        self.input_queue.stop_consuming()

    def start(self):
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        try:
            self.input_queue.start_consuming(self.process_messsage)
        except Exception as e:
            logging.error(f"Error starting consuming: {e}")
            self.input_queue.stop_consuming()
        finally:
            self.input_queue.close()
            self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
