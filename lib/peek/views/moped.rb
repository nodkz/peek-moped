require 'moped'
require 'atomic'

class Moped::Node
  class << self
    attr_accessor :command_time, :command_count, :command_operations
  end
  self.command_count = Atomic.new(0)
  self.command_time = Atomic.new(0)
  self.command_operations = Atomic.new([])

  def logging_with_peek(*args, &block)
    start = Time.now
    logging_without_peek(*args, &block)
  ensure
    duration = (Time.now - start)
    Moped::Node.command_time.update { |value| value + duration }
    Moped::Node.command_count.update { |value| value + 1 }
    Moped::Node.command_operations.update { |value| value << [Moped::Node.command_count.get, duration, args[0], peek_app_backtrace] }
  end
  alias_method_chain :logging, :peek

  def peek_app_backtrace
    root_path = Rails.root.to_s
    caller.grep(%r{^#{root_path}/(app|lib)}) { |v| v.sub(root_path, '') }
  end
end

module Peek
  module Views
    class Moped < View
      OP_CODES = {
        '1'    => ['*', 'Reply to a client request. responseTo is set'],
        '1000' => ['*', 'Generic msg command followed by a string'],
        '2001' => ['U', 'Update document'],
        '2002' => ['I', 'Insert new document'],
        '2003' => ['*', 'Formerly used for OP_GET_BY_OID'],
        '2004' => ['R', 'Query a collection'],
        '2005' => ['RR','Get more data from a query'],
        '2006' => ['D', 'Delete documents'],
        '2007' => ['*', 'Tell database client is done with a cursor']
      }

      def duration
        ::Moped::Node.command_time.value
      end

      def op_code_info op_code
        key = op_code.to_s
        if OP_CODES.has_key? key
          OP_CODES[key]
        else
          [key, 'Undescribed op_code of mongodb wired protocol']
        end
      end

      def formatted_duration
        ms = duration * 1000
        if ms >= 1000
          "%.2fms" % ms
        else
          "%.0fms" % ms
        end
      end

      def calls
        ::Moped::Node.command_count.value
      end

      def operations
        ::Moped::Node.command_operations.value
      end

      def results
        { :duration => formatted_duration, :calls => calls, :operations => operations.inspect }
      end

      private

      def setup_subscribers
        # Reset each counter when a new request starts
        before_request do
          ::Moped::Node.command_time.value = 0
          ::Moped::Node.command_count.value = 0
          ::Moped::Node.command_operations.value = []
        end
      end
    end
  end
end