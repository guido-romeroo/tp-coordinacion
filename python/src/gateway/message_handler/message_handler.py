from common.message_protocol.internal import ProtocolMessage


class MessageHandler:
    _next_id = 0
    def __init__(self):
        self._id = MessageHandler._next_id
        MessageHandler._next_id += 1
    
    def serialize_data_message(self, message):
        return ProtocolMessage(self._id, [message]).serialize()

    def serialize_eof_message(self, message):
        return ProtocolMessage(self._id, []).serialize()

    def deserialize_result_message(self, message):
        message = ProtocolMessage.deserialize(message)
        if message.id != self._id:
            return None
        return message.fruit_items
