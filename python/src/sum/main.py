import os
import logging
import queue
import threading

from common import middleware, fruit_item
from common.message_protocol.internal import ProtocolMessage, ProtocolMessageType

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
        self.data_output_exchanges = SumFilter._construct_data_output_exchanges()
        self.messages_queue = queue.Queue()
        self.items_by_client_id = {}
        self.msg_processed_count = {}
    
    @staticmethod
    def _construct_data_output_exchanges():
        data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            data_output_exchanges.append(data_output_exchange)
        return data_output_exchanges
    
    def _process_data(self, msg: ProtocolMessage):
        logging.info(f"Process data")
        items = self.items_by_client_id.get(msg.client_id, {})
        [fruit, amount] = msg.payload.pop()

        items[fruit] = items.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

        self.items_by_client_id[msg.client_id] = items
        self.msg_processed_count[msg.client_id] = self.msg_processed_count.get(msg.client_id, 0) + 1

    # TODO: enviar cada fruta a un mismo agreggator, unificar esto para todas las instancias de sum, para evitar que los tops parciales se calculen incompletos
    def _send_sums(self, msg: ProtocolMessage):
        logging.info(f"Sending sums messages")
        items = self.items_by_client_id.get(msg.client_id, {})
        for final_fruit_item in items.values():
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(
                    ProtocolMessage(ProtocolMessageType.FRUITS, msg.client_id, msg.msg_id, [[final_fruit_item.fruit, final_fruit_item.amount]]).serialize()
                )
    def broadcast_eof_to_other_sums(self, msg: ProtocolMessage):
        logging.info(f"Broadcasting EOF message to other sums")
        peers = [f"{SUM_CONTROL_EXCHANGE}_{i}" for i in range(SUM_AMOUNT) if i != ID]
        if not peers:
            return
        control_output = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, SUM_CONTROL_EXCHANGE, peers)
        control_output.send(msg.serialize())

    def _send_eof_to_aggregators(self, msg: ProtocolMessage):
        logging.info(f"Sending EOF message to aggregation filters")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(msg.serialize())
        if msg.client_id in self.items_by_client_id:
            del self.items_by_client_id[msg.client_id]
            del self.msg_processed_count[msg.client_id]

    def _get_next_sum_id_in_ring(self, msg: ProtocolMessage):
        peer = (ID + 1) % SUM_AMOUNT
        if peer in msg.payload[1]:
            peer = (peer + 1) % SUM_AMOUNT
        return peer
    
    def _process_eof(self, msg: ProtocolMessage):
        logging.info(f"Process EOF, client_id: {msg.client_id}, missing amount: {msg.payload[0]}, finalized ids: {msg.payload[1]}, processed count: {self.msg_processed_count[msg.client_id]}")
        if not msg.payload[1]:
            self.msg_processed_count[msg.client_id] += 1 # solo el que recibe el eof de la queue del cliente
            msg.payload[1].append(ID)
        if msg.payload[0] == 0: # me hicieron broadcast de EOF
            self._send_sums(msg)
            self._send_eof_to_aggregators(msg)
            return
        missing_amount = msg.payload[0] - self.msg_processed_count.get(msg.client_id, 0) 
        msg.payload[0] = missing_amount 
        self.msg_processed_count[msg.client_id] = 0

        if missing_amount < 0:
            logging.error(f"Missing amount is negative: {missing_amount}, msg.payload: {msg.payload}")
            raise ValueError(f"Missing amount is negative: {missing_amount}")
        
        if missing_amount == 0:
            self.broadcast_eof_to_other_sums(msg)
            self._send_sums(msg)
            self._send_eof_to_aggregators(msg)
            return

        peer = self._get_next_sum_id_in_ring(msg)
        control_output = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_CONTROL_EXCHANGE}_{peer}"]
        )
        control_output.send(msg.serialize())


    def process_data_messsage(self, msg: ProtocolMessage):
        if msg.is_eof():
            self._process_eof(msg)
        else:
            self._process_data(msg)

    def _listen_client_messages(self):
        input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        def dispatch_client_message(message, ack, nack):
            try:
                msg: ProtocolMessage = ProtocolMessage.deserialize(message)
                ack_queue = queue.Queue()
                self.messages_queue.put((msg, ack_queue))
                ack_queue.get()
                ack()
            except Exception as e:
                logging.error(f"Error processing message: {e}")
                nack()
                input_queue.stop_consuming()

        input_queue.start_consuming(dispatch_client_message)
        input_queue.close()

    def _listen_control_messages(self):
        control_input = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_CONTROL_EXCHANGE}_{ID}"]
        )
        def dispatch_control_message(message, ack, nack):
            try:
                msg: ProtocolMessage = ProtocolMessage.deserialize(message)
                ack_queue = queue.Queue()
                self.messages_queue.put((msg, ack_queue))
                ack_queue.get()
                ack()
            except Exception as e:
                logging.error(f"Error processing message: {e}")
                nack()
                control_input.stop_consuming()

        control_input.start_consuming(dispatch_control_message)
        control_input.close()
    
    def start(self):
        client_listener = threading.Thread(target=self._listen_client_messages)
        control_listener = threading.Thread(target=self._listen_control_messages)
        client_listener.start()
        control_listener.start()
        ack_queue = None
        while True:
            try:
                msg, ack_queue = self.messages_queue.get()
                self.process_data_messsage(msg)
                ack_queue.put(True)
            except Exception as e:
                logging.error(f"Error processing message: {e}")
                self.messages_queue.shutdown(immediate=False)
                if ack_queue:
                    ack_queue.shutdown(immediate=True)
                for exchange in self.data_output_exchanges:
                    exchange.close()
                break
        control_listener.join()
        client_listener.join()

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
