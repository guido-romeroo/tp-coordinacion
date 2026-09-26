from common.message_protocol.internal import ProtocolMessage, ProtocolMessageType


class MessageHandler:
    _next_id = 0
    def __init__(self):
        self.client_id = MessageHandler._next_id
        MessageHandler._next_id += 1
        self.msg_id = 0
    
    def serialize_data_message(self, message):
        self.msg_id += 1
        return ProtocolMessage(ProtocolMessageType.FRUITS, self.client_id, self.msg_id, [message]).serialize()

    def serialize_eof_message(self, message):
        self.msg_id += 1
        return ProtocolMessage(ProtocolMessageType.EOF, self.client_id, self.msg_id, [self.msg_id, []]).serialize()

    def deserialize_result_message(self, message):
        message = ProtocolMessage.deserialize(message)
        if message.client_id != self.client_id:
            return None
        if message.type != ProtocolMessageType.FRUITS:
            raise RuntimeError("Unexpected message type")
        return message.payload
