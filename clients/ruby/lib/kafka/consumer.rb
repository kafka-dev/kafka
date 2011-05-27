module Kafka
  class Consumer

    include Kafka::IO

    CONSUME_REQUEST_TYPE = Kafka::RequestType::FETCH
    MAX_SIZE = 1048576 # 1 MB
    DEFAULT_POLLING_INTERVAL = 2 # 2 seconds

    attr_accessor :topic, :partition, :offset, :max_size, :request_type, :polling

    def initialize(options = {})
      self.topic        = options[:topic]        || "test"
      self.partition    = options[:partition]    || 0
      self.host         = options[:host]         || "localhost"
      self.port         = options[:port]         || 9092
      self.offset       = options[:offset]       || 0
      self.max_size     = options[:max_size]     || MAX_SIZE
      self.request_type = options[:request_type] || CONSUME_REQUEST_TYPE
      self.polling      = options[:polling]      || DEFAULT_POLLING_INTERVAL
      self.connect(self.host, self.port)
    end

    # REQUEST TYPE ID + TOPIC LENGTH + TOPIC + PARTITION + OFFSET + MAX SIZE
    def request_size
      2 + 2 + topic.length + 4 + 8 + 4
    end

    def encode_request_size
      [self.request_size].pack("N")
    end

    def encode_request(request_type, topic, partition, offset, max_size)
      request_type = [request_type].pack("n")
      topic        = [topic.length].pack('n') + topic
      partition    = [partition].pack("N")
      offset       = [offset].pack("Q").reverse # DIY 64bit big endian integer
      max_size     = [max_size].pack("N")

      request_type + topic + partition + offset + max_size
    end

    def consume
      self.send_consume_request         # request data
      data = self.read_data_response    # read data response
      self.parse_message_set_from(data) # parse message set
    end

    def loop(&block)
      messages = []
      while(true) do
        messages = self.consume
        block.call(messages) if messages && !messages.empty?
        sleep(self.polling)
      end
    end

    def read_data_response
      data_length = self.socket.read(4).unpack("N").shift # read length
      data = self.socket.read(data_length)                # read message set
      data[2, data.length]                                # we start with a 2 byte offset
    end

    def send_consume_request
      self.write(self.encode_request_size) # write request_size
      self.write(self.encode_request(self.request_type, self.topic, self.partition, self.offset, self.max_size)) # write request
    end

    def parse_message_set_from(data)
      messages = []
      processed = 0
      length = data.length - 4
      while(processed <= length) do
        message_size = data[processed, 4].unpack("N").shift
        messages << Kafka::Message.parse_from(data[processed, message_size + 4])
        processed += 4 + message_size
      end
      self.offset += processed
      messages
    end
  end
end
