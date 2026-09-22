ID_SIZE = 8
AMOUNT_SIZE = 4
FRUIT_LENGTH_SIZE = 4

class ProtocolMessage:
    def __init__(self, id, fruit_items):
        self.id = id
        self.fruit_items = fruit_items

    @staticmethod
    def deserialize(protocol_msg):
        id = int.from_bytes(protocol_msg[:ID_SIZE], "big")
        fruit_items = []
        offset = ID_SIZE
        while offset < len(protocol_msg):
            fruit_length = int.from_bytes(protocol_msg[offset : offset + FRUIT_LENGTH_SIZE], "big")
            offset += FRUIT_LENGTH_SIZE

            fruit_bytes = protocol_msg[offset : offset + fruit_length]
            fruit = fruit_bytes.decode("utf-8")
            offset += fruit_length
            
            amount = int.from_bytes(protocol_msg[offset : offset + AMOUNT_SIZE], "big")
            fruit_items.append([fruit, amount])
            offset += AMOUNT_SIZE
        return ProtocolMessage(id, fruit_items)
    
    def _serialize_id(self):
        return self.id.to_bytes(ID_SIZE, "big")

    def _serialize_fruit_items(self):
        serialized = b''
        for fruit_item in self.fruit_items:
            fruit_bytes = fruit_item[0].encode("utf-8")
            fruit_length = len(fruit_bytes).to_bytes(FRUIT_LENGTH_SIZE, "big")
            serialized += fruit_length
            serialized += fruit_bytes
            serialized += fruit_item[1].to_bytes(AMOUNT_SIZE, "big")
        return serialized
    
    def serialize(self):
        serialized = b''
        serialized += self._serialize_id()
        serialized += self._serialize_fruit_items()
        return serialized

    def is_eof(self):
        return len(self.fruit_items) == 0