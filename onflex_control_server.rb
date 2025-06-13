require "json"
require "logger"

require "async/http/endpoint"
require "async/process"
require "async/websocket"

begin
  require "net/ssh"

rescue LoadError
end

module OnFlexControlServer
  class Server
    BROKER_HEARTBEAT_INTERVAL = 60.0
    BROKER_RECONNECT_INTERVAL = 60.0

    MT_CALL = 2
    MT_CALL_RESULT = 3

    RECEIVE_TIMEOUT = 8.0

    def initialize logger: Logger.new(nil)
      @logger = logger

      configuration = JSON.load_file File.expand_path("../config/onflex.cfg", __dir__), symbolize_names: true

      @endpoint = Async::HTTP::Endpoint.parse "ws://onflex.manageon.co.kr:24411/servers/manageon/onflex/%s" % [
        configuration.dig(:ocpp_client, :endpoint)[/[^\/]+$/],
      ]

      configuration = JSON.load_file File.expand_path("../config/onflex_control_server.cfg", __dir__), symbolize_names: true

      @ssh_parameters = [
        configuration.dig(:ssh_server, :host),
        configuration.dig(:ssh_server, :user),
        {
          keepalive: true,
          key_data: configuration.dig(:ssh_server, :passphrase),
          port: configuration.dig(:ssh_server, :port),
        }
      ]

      @sending_messages = Async::Queue.new
      @received_messages = []
      @received_notification = Async::Notification.new
    end

    def receive_call
      until index = @received_messages.find_index { |message| message[0] == MT_CALL }
        @received_notification.wait
      end

      @received_messages.delete_at index
    end

    def receive_call_result message_id
      until index = @received_messages.find_index { |message| message[0] != MT_CALL && message[1] == message_id }
        @received_notification.wait
      end

      @received_messages.delete_at index
    end

    def run
      parent_task = Async::Task.current

      loop do
        Async::WebSocket::Client.connect @endpoint do |connection|
          @logger.add Logger::INFO, "Connected", :Connection

          connection_task = parent_task.async do |connection_task|
            connection_task.async do |task|
              while text_message = connection.read
                @logger.add Logger::INFO, "Received (#{ text_message.to_str} )", :ReceiveTask

                @received_messages.push JSON.parse text_message.to_str, symbolize_names: true
                @received_notification.signal
              end

            rescue
              @logger.add Logger::ERROR, "#{ $!.message[..80] }\n  #{ $!.backtrace.join "\n  " }", :ReceiveTask

            ensure
              connection_task.stop
            end

            connection_task.async do |task|
              while message = @sending_messages.dequeue
                text_message = JSON.generate message
                connection.send_text text_message
                connection.flush

                @logger.add Logger::INFO, "Sent (#{ text_message } )", :SendingTask
              end

            rescue
              @logger.add Logger::ERROR, "#{ $!.message[..80] }\n  #{ $!.backtrace.join "\n  " }", :SendingTask

            ensure
              connection_task.stop
            end

            connection_task.async do |task|
              while true
                sleep BROKER_HEARTBEAT_INTERVAL

                task.with_timeout RECEIVE_TIMEOUT do
                  message_id = SecureRandom.uuid.delete("-")[..15]
                  send [MT_CALL, message_id, :Heartbeat, {} ]
                  receive_call_result message_id
                end
              end

            rescue
              @logger.add Logger::ERROR, "#{ $!.message[..80] }\n  #{ $!.backtrace.join "\n  " }", :HeartbeatTask

            ensure
              connection_task.stop
            end

            ssh_tunneling_task = nil

            while message = receive_call
              payload = (
                case message[2].to_sym
                when :Shell
                  { output: Async::Process.capture(*message[3][:command], err: [:child, :out] ) }

                when :StartSSHTunneling
                  if ssh_tunneling_task
                    { status: :rejected }

                  else
                    ssh_tunneling_task = connection_task.async do |task|
                      thread = Thread.new do
                        Net::SSH.start *@ssh_parameters do |ssh|
                          @logger.add Logger::INFO, "SSH Server Connected", :SSHThreadTask

                          ssh.forward.remote *message[3].values_at(:local_port, :local_host, :remote_port, :remote_host)
                          ssh.loop { true }

                        ensure
                          @logger.add Logger::INFO, "SSH Server Disconnected", :SSHThreadTask
                        end

                      rescue
                        @logger.add Logger::ERROR, "#{ $!.message[..80] }\n  #{ $!.backtrace.join "\n  " }", :SSHThreadTask

                      ensure
                        thread = nil
                      end

                      thread.join

                    rescue
                      @logger.add Logger::ERROR, "#{ $!.message[..80] }\n  #{ $!.backtrace.join "\n  " }", :SSHTask

                    ensure
                      thread&.terminate

                      ssh_tunneling_task = nil
                    end

                    { status: :accepted }
                  end

                when :StopSSHTunneling
                  if ssh_tunneling_task
                    ssh_tunneling_task.stop

                    { status: :accepted }

                  else
                    { status: :rejected }
                  end

                else
                  { status: :unsupported }
                end
              )

              send [MT_CALL_RESULT, message[1], payload]
            end
          end

          connection_task.wait

        ensure
          @logger.add Logger::INFO, "Disconnected", :Connection
        end

        sleep BROKER_RECONNECT_INTERVAL

      rescue
        @logger.add Logger::ERROR, "#{ $!.message[..80] }\n  #{ $!.backtrace.join "\n  " }", :ConnectionTask

        sleep BROKER_RECONNECT_INTERVAL

        retry
      end
    end

    def send message
      @sending_messages.enqueue message
    end

    def self.run ...
      self.new(...).run
    end
  end
end

Process.setproctitle "onflex_control_server"

logger = Logger.new File.expand_path("../log/onflex_control_server.log", __dir__), "daily"

Sync do |parent_task|
  Signal.trap :INT do
    parent_task.stop
  end

  Signal.trap :TERM do
    parent_task.stop
  end

  OnFlexControlServer::Server.run logger:
end
