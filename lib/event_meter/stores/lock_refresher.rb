require_relative "../errors"

module EventMeter
  module Stores
    class LockRefresher
      JOIN_TIMEOUT = 1.0

      def initialize(interval:, refresh:, failure_message:, thread_name:)
        @interval = interval
        @refresh = refresh
        @failure_message = failure_message
        @thread_name = thread_name
        @mutex = Mutex.new
        @condition = ConditionVariable.new
        @stopped = false
      end

      def start(owner: Thread.current)
        @owner = owner
        @thread = Thread.new { run }
        self
      end

      def stop
        @mutex.synchronize do
          @stopped = true
          @condition.broadcast
        end

        @thread&.join(JOIN_TIMEOUT)
      end

      private

      def run
        Thread.current.report_on_exception = false
        Thread.current.name = @thread_name if Thread.current.respond_to?(:name=)

        loop do
          break if stopped_after_wait?

          refreshed = @refresh.call
          raise LockLostError, @failure_message unless refreshed
        end
      rescue LockLostError => error
        notify_owner(error)
      rescue StandardError => error
        notify_owner(LockLostError.new("#{@failure_message}: #{error.class}: #{error.message}"))
      end

      def stopped_after_wait?
        @mutex.synchronize do
          @condition.wait(@mutex, @interval) unless @stopped
          @stopped
        end
      end

      def notify_owner(error)
        owner = @mutex.synchronize do
          already_stopped = @stopped
          @stopped = true
          @condition.broadcast
          already_stopped ? nil : @owner
        end

        # Losing the refresh means another process can acquire this lock while
        # the owner still writes. Interrupt the owner immediately instead of
        # letting split-brain processing continue until the block returns.
        owner&.raise(error)
      rescue ThreadError
        nil
      end
    end
  end
end
