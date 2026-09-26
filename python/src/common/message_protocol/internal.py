ID_SIZE = 8
AMOUNT_SIZE = 4
FRUIT_LENGTH_SIZE = 4
TYPE_SIZE = 1

class ProtocolMessageType:
    FRUITS = 0
    EOF = 1

class ProtocolMessage:
    def __init__(self, type, client_id, msg_id, payload):
        self.type = type
        self.client_id = client_id
        # TODO: eliminar msg_id
        self.msg_id = msg_id
        self.payload = payload

    @staticmethod
    def _deserialize_fruit_items_payload(protocol_msg, offset):
        fruit_items = []
        while offset < len(protocol_msg):
            fruit_length = int.from_bytes(protocol_msg[offset : offset + FRUIT_LENGTH_SIZE], "big")
            offset += FRUIT_LENGTH_SIZE

            fruit_bytes = protocol_msg[offset : offset + fruit_length]
            fruit = fruit_bytes.decode("utf-8")
            offset += fruit_length
            
            amount = int.from_bytes(protocol_msg[offset : offset + AMOUNT_SIZE], "big")
            fruit_items.append([fruit, amount])
            offset += AMOUNT_SIZE
        return fruit_items

    @staticmethod
    def _deserialize_eof_payload(protocol_msg, offset):
        missing_amount = int.from_bytes(protocol_msg[offset : offset + ID_SIZE], "big")
        offset += ID_SIZE
        finalized_ids = []
        while offset < len(protocol_msg):
            finalized_id = int.from_bytes(protocol_msg[offset : offset + ID_SIZE], "big")
            finalized_ids.append(finalized_id)
            offset += ID_SIZE
        return [missing_amount, finalized_ids]

    @staticmethod
    def _deserialize_payload(type, offset, payload):
        if type == ProtocolMessageType.EOF:
            return ProtocolMessage._deserialize_eof_payload(offset, payload)
        return ProtocolMessage._deserialize_fruit_items_payload(offset, payload)

    @staticmethod
    def deserialize(protocol_msg):
        type = int.from_bytes(protocol_msg[:TYPE_SIZE], "big")
        offset = TYPE_SIZE
        client_id = int.from_bytes(protocol_msg[offset : offset + ID_SIZE], "big")
        offset += ID_SIZE
        msg_id = int.from_bytes(protocol_msg[offset : offset + ID_SIZE], "big")
        offset += ID_SIZE
        deserialized_payload = ProtocolMessage._deserialize_payload(type, protocol_msg, offset)
        
        return ProtocolMessage(type, client_id, msg_id, deserialized_payload)
    
    def _serialize_id(self, id):
        return id.to_bytes(ID_SIZE, "big")

    def _serialize_type(self):
        return self.type.to_bytes(TYPE_SIZE, "big")

    def _serialize_payload(self):
        if self.type == ProtocolMessageType.EOF:
            return self._serialize_eof_data()
        return self._serialize_fruit_items()

    def _serialize_eof_data(self):
        # [catidad_faltante, [id_terminado, ..., id_terminado]]
        serialized = b''
        serialized += self._serialize_id(self.payload[0])
        for id in self.payload[1]:
            serialized += self._serialize_id(id)
        return serialized
    
    def _serialize_fruit_items(self):
        serialized = b''
        for fruit_item in self.payload:
            fruit_bytes = fruit_item[0].encode("utf-8")
            fruit_length = len(fruit_bytes).to_bytes(FRUIT_LENGTH_SIZE, "big")
            serialized += fruit_length
            serialized += fruit_bytes
            serialized += fruit_item[1].to_bytes(AMOUNT_SIZE, "big")
        return serialized
    
    def serialize(self):
        serialized = b''
        serialized += self._serialize_type()
        serialized += self._serialize_id(self.client_id)
        serialized += self._serialize_id(self.msg_id)
        serialized += self._serialize_payload()
        return serialized

    def is_eof(self):
        return self.type == ProtocolMessageType.EOF