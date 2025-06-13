require "fileutils"
require "json"
require "logger"
require "securerandom"

require "async"
require "async/barrier"
require "async/http/internet"
require "async/notification"
require "async/process"
require "async/queue"
require "async/websocket"
require "extralite"
require "ffi"
require "protocol/websocket/json_message"
require "seven_zip_ruby"

require_relative "onflex_classes"

module OnFlex
  FIRMWARE_VERSION = "1.10a"

  LOOP_INTERVAL = 0.1

  class OnFlex
    CP_MONITOR_READ_INTERVAL = 0.5

    ID_TAG_READER_RECEIVE_INTERVAL = 0.5

    LED_DISPLAY_BLINK_INTERVAL = 0.5
    LED_DISPLAY_UPDATE_INTERVAL = 1.0

    POWER_METER_LOG_INTERVAL = 300.0
    POWER_METER_RECEIVE_TIMEOUT = 4.0
    POWER_METER_SEND_INTERVAL = 1.0

    THERMOMETER_MONITOR_LOG_INTERVAL = 300.0
    THERMOMETER_MONITOR_READ_INTERVAL = 1.0

    OCPP_CLIENT_TIMEOUT = 8.0
    OCPP_CLIENT_METER_VALUES_SEND_INTERVAL = 60.0

    CALIBRATION_TIME = 10.0
    ERROR_DISPLAY_TIME = 4.0
    REBOOT_DELAY = 4.0
    RELAY_TIMEOUT = 4.0

    def run_mode_a
      Async do |parent_task|
        @logger.info "OnFlex, Start Mode-A"

        ocpp_client = OCPPClient.new settings: @settings[:ocpp_client], logger: @logger
        ocpp_client.start

        remote_start_transaction_message = nil

        led_display_queue = Async::LimitedQueue.new

        start_meter_value = nil
        charging_unit_price = nil

        parent_task.async do |task|
          @led_display.set_leds [:led41], :on

          barrier = Async::Barrier.new

          while true
            action, payload = led_display_queue.dequeue

            barrier.stop

            case action
            when :connecting
              @led_display.set_leds [:led11, :led12, :led13, :led14, :led15], :off

              barrier.async do |task|
                state = :on

                while true
                  @led_display.set_leds [:led11], state

                  sleep LED_DISPLAY_BLINK_INTERVAL

                  state = state == :on ? :off : :on
                end
              end

            when :id_tag_receiving
              @led_display.set_leds [:led13, :led14, :led15], :off
              @led_display.set_leds [:led11, :led12], :on

              barrier.async do |task|
                state = :on

                while true
                  @led_display.set_leds [:led12], state

                  sleep LED_DISPLAY_BLINK_INTERVAL

                  state = state == :on ? :off : :on
                end
              end

            when :authenticating
              @led_display.set_leds [:led14, :led15], :off
              @led_display.set_leds [:led11, :led12, :led13], :on

              barrier.async do |task|
                state = :on

                while true
                  @led_display.set_leds [:led13], state

                  sleep LED_DISPLAY_BLINK_INTERVAL

                  state = state == :on ? :off : :on
                end
              end

            when :charging
              @led_display.set_leds [:led15], :off
              @led_display.set_leds [:led11, :led12, :led13, :led14], :on

              barrier.async do |task|
                state = :on

                while true
                  @led_display.set_leds [:led14], state

                  sleep LED_DISPLAY_BLINK_INTERVAL

                  state = state == :on ? :off : :on
                end
              end

            when :finished
              @led_display.set_leds [:led11, :led12, :led13, :led14, :led15], :on

              barrier.async do |task|
                state = :on

                while true
                  @led_display.set_leds [:led15], state

                  sleep LED_DISPLAY_BLINK_INTERVAL

                  state = state == :on ? :off : :on
                end
              end
            end

            case action
            when :connecting, :id_tag_receiving, :authenticating
              @led_display.set_leds [:led22, :led23], :off
              @led_display.set_leds [:led21], :on
              @led_display.set_leds [:led32, :led33], :off
              @led_display.set_leds [:led31], :on

              barrier.async do |task|
                while true
                  @led_display.set_digits "%7.2f" % (@power_meter_values.meter_value / 1_000.0)

                  sleep LED_DISPLAY_UPDATE_INTERVAL
                end
              end

            when :charging
              @led_display.set_leds [:led22, :led23], :off
              @led_display.set_leds [:led21], :on

              barrier.async do |task|
                types = [:meter_value, :current, :cost].cycle

                task.async do |task|
                  while true
                    sleep 4.0

                    types.next
                  end
                end

                while true
                  case types.peek
                  when :meter_value
                    @led_display.set_leds [:led32, :led33], :off
                    @led_display.set_leds [:led31], :on
                    @led_display.set_digits "%7.2f" % ( (@power_meter_values.meter_value - start_meter_value) / 1_000.0)

                  when :current
                    @led_display.set_leds [:led31, :led33], :off
                    @led_display.set_leds [:led32], :on
                    @led_display.set_digits "%7.2f" % (@power_meter_values.current / 1_000.0)

                  when :cost
                    @led_display.set_leds [:led31, :led32], :off
                    @led_display.set_leds [:led33], :on
                    @led_display.set_digits "%6d" % ( (@power_meter_values.meter_value - start_meter_value) / 1_000.0 * charging_unit_price).truncate
                  end

                  sleep LED_DISPLAY_UPDATE_INTERVAL
                end
              end

            when :finished
              @led_display.set_leds [:led11, :led12, :led13, :led14, :led15], :on

              barrier.async do |task|
                types = [:meter_value, :cost].cycle

                task.async do |task|
                  while true
                    sleep 4.0

                    types.next
                  end
                end

                while true
                  case types.peek
                  when :meter_value
                    @led_display.set_leds [:led32, :led33], :off
                    @led_display.set_leds [:led31], :on
                    @led_display.set_digits "%7.2f" % ( (@power_meter_values.meter_value - start_meter_value) / 1_000.0)

                  when :cost
                    @led_display.set_leds [:led31, :led32], :off
                    @led_display.set_leds [:led33], :on
                    @led_display.set_digits "%6d" % ( (@power_meter_values.meter_value - start_meter_value) / 1_000.0 * charging_unit_price).truncate
                  end

                  sleep LED_DISPLAY_UPDATE_INTERVAL
                end
              end
            end

            sleep LOOP_INTERVAL
          end
        end

        parent_task.async do |task|
          firmware_update_task = nil

          loop do
            message = ocpp_client.receive_call

            case message.action
            when :Reset
              ocpp_client.send OCPPClient::CallResultMessage.new message.message_id, { status: :Accepted }

              sleep REBOOT_DELAY

              system "reboot -f"

            when :RemoteStartTransaction

              if remote_start_transaction_message
                status = :Rejected

              else
                remote_start_transaction_message = message

                status = :Accepted
              end

              ocpp_client.send OCPPClient::CallResultMessage.new message.message_id, { status: }

            when :RemoteStopTransaction
              remote_stop_transaction_message = message

              ocpp_client.send OCPPClient::CallResultMessage.new message.message_id, { status: :Accepted }

            when :UpdateFirmware
              ocpp_client.send OCPPClient::CallResultMessage.new message.message_id

              firmware_update_task&.close

              firmware_update_task = task.async do |task|
                task.with_timeout OCPP_CLIENT_TIMEOUT do
                  ocpp_client.request OCPPClient::CallMessage.new OCPPClient.generate_message_id, :FirmwareStatusNotification, {
                    status: :Downloading
                  }
                end

                internet = Async::HTTP::Internet.new

                base_path = File.expand_path "..", __dir__
                file_name = File.join base_path, "firmwares/#{ SecureRandom.uuid.unpack("a8xa4xa4").join }.bin"

                begin
                  response = internet.get message.payload[:location]
                  response.save file_name

                  task.with_timeout OCPP_CLIENT_TIMEOUT do
                    ocpp_client.request OCPPClient::CallMessage.new OCPPClient.generate_message_id, :FirmwareStatusNotification, {
                      status: :Downloaded
                    }
                  end

                rescue
                  task.with_timeout OCPP_CLIENT_TIMEOUT do
                    ocpp_client.request OCPPClient::CallMessage.new OCPPClient.generate_message_id, :FirmwareStatusNotification, {
                      status: :DownloadFailed
                    }
                  end
                end

                task.with_timeout OCPP_CLIENT_TIMEOUT do
                  ocpp_client.request OCPPClient::CallMessage.new OCPPClient.generate_message_id, :FirmwareStatusNotification, {
                    status: :Installing
                  }
                end

                download_thread = Thread.new do
                  File.open file_name, "rb" do |file|
                    SevenZipRuby::Reader.extract_all file, base_path, password: "lFat{N))zs{@fL9W"
                  end
                end

                begin
                  download_thread.join

                  task.with_timeout OCPP_CLIENT_TIMEOUT do
                    ocpp_client.request OCPPClient::CallMessage.new OCPPClient.generate_message_id, :FirmwareStatusNotification, {
                      status: :Installed
                    }
                  end

                rescue
                  task.with_timeout OCPP_CLIENT_TIMEOUT do
                    ocpp_client.request OCPPClient::CallMessage.new OCPPClient.generate_message_id, :FirmwareStatusNotification, {
                      status: :InstallationFailed
                    }
                  end
                end

                File.delete file_name

                sleep REBOOT_DELAY

                system "reboot -f"

              ensure
                firmware_update_task = nil
              end

            else
              ocpp_client.send OCPPClient::CallResultMessage.new message.message_id, { status: :NotSupported }
            end

            sleep LOOP_INTERVAL

          rescue
            sleep LOOP_INTERVAL
          end
        end

        ocpp_status = nil
        start_transaction_id = nil
        old_cp_status = nil

        @audio_queue.enqueue :charger_start

        led_display_queue.enqueue :connecting

        loop do
          @pwm_controller.set_duty_cycle 100
          @relay.set_state :open

          authorized = nil

          start_id_tag = nil
          stop_id_tag = nil

          authorize_task = nil

          remote_start_transaction_message = nil
          remote_stop_transaction_message = nil

          @id_tag_reader.clear

          while true
            if id_tag = @id_tag_reader.receive
              if @id_tags[:manufacturer_mode].include? id_tag
                @mode = :mode_c

                parent_task.stop

              elsif @id_tags[:calibration_mode].include? id_tag
                @mode = :mode_b

                parent_task.stop

              elsif !authorize_task
                authorize_task = parent_task.async do |task|
                  message = task.with_timeout OCPP_CLIENT_TIMEOUT do
                    ocpp_client.request OCPPClient::CallMessage.new OCPPClient.generate_message_id, :Authorize, {
                      idTag: id_tag
                    }
                  end

                  case message.payload[:idTagInfo][:status].to_sym
                  when :Accepted
                    start_id_tag = id_tag

                  when :Blocked
                    @audio_queue.enqueue :insufficient_cash

                  else
                    @audio_queue.enqueue :invalid_card
                  end

                rescue
                  @audio_queue.enqueue :server_connect_failed

                ensure
                  authorize_task = nil
                end
              end
            end

            if start_id_tag
              break
            end

            unless old_cp_status == @cp_status
              old_cp_status = @cp_status

              case @cp_status
              when :a
                led_display_queue.enqueue :connecting

                unless ocpp_status == :Available
                  if ocpp_status
                    @audio_queue.enqueue :ev_disconnected
                  end

                  ocpp_status = :Available

                  parent_task.async do |task|
                    task.with_timeout OCPP_CLIENT_TIMEOUT do
                      ocpp_client.request OCPPClient::CallMessage.new OCPPClient.generate_message_id, :StatusNotification, {
                        connectorId: 1,
                        errorCode: :NoError,
                        status: ocpp_status,
                      }

                    rescue Async::TimeoutError
                    end
                  end
                end

              when :b
                led_display_queue.enqueue :id_tag_receiving

                unless ocpp_status == :Preparing
                  ocpp_status = :Preparing

                  parent_task.async do |task|
                    task.with_timeout OCPP_CLIENT_TIMEOUT do
                      ocpp_client.request OCPPClient::CallMessage.new OCPPClient.generate_message_id, :StatusNotification, {
                        connectorId: 1,
                        errorCode: :NoError,
                        status: ocpp_status,
                      }

                    rescue Async::TimeoutError
                    end
                  end

                  @audio_queue.enqueue :ev_connected
                end
              end
            end

            if remote_start_transaction_message
              start_id_tag = remote_start_transaction_message.payload[:idTag]

              break
            end

            sleep LOOP_INTERVAL
          end

          @audio_queue.enqueue :charging_waiting

          authorized = true

          unless ocpp_status == :Preparing
            ocpp_status = :Preparing

            parent_task.async do |task|
              task.with_timeout OCPP_CLIENT_TIMEOUT do
                ocpp_client.request OCPPClient::CallMessage.new OCPPClient.generate_message_id, :StatusNotification, {
                  connectorId: 1,
                  errorCode: :NoError,
                  status: ocpp_status,
                }

              rescue Async::TimeoutError
              end
            end
          end

          parent_task.with_timeout 30.0 do
            until @cp_status == :b
              sleep LOOP_INTERVAL
            end

            @pwm_controller.set_current 32

            until @cp_status == :c
              sleep LOOP_INTERVAL
            end

          rescue Async::TimeoutError
            @audio_queue.enqueue :authority_timeout

            authorized = false
          end

          next unless authorized

          start_meter_value = @power_meter_values.meter_value

          @relay.set_state :close

          led_display_queue.enqueue :charging

          message = parent_task.with_timeout OCPP_CLIENT_TIMEOUT do
            ocpp_client.request OCPPClient::CallMessage.new OCPPClient.generate_message_id, :StartTransaction, {
              connectorId: 1,
              idTag: start_id_tag,
              meterStart: start_meter_value,
              reservationId: 0,
              timestamp: Time.now.utc.strftime("%FT%T.%LZ"),
            }

          rescue Async::TimeoutError
          end

          start_transaction_id = message.payload[:transactionId]

          ocpp_status = :Charging

          parent_task.async do |task|
            task.with_timeout OCPP_CLIENT_TIMEOUT do
              ocpp_client.request OCPPClient::CallMessage.new OCPPClient.generate_message_id, :StatusNotification, {
                connectorId: 1,
                errorCode: :NoError,
                status: ocpp_status,
              }

            rescue Async::TimeoutError
            end
          end

          message = parent_task.with_timeout OCPP_CLIENT_TIMEOUT do
            ocpp_client.request OCPPClient::CallMessage.new OCPPClient.generate_message_id, :DataTransfer, {
              vendorId: @settings[:ocpp_client][:vendor_id],
              messageId: :MaximumCharge,
              data: start_transaction_id,
            }

          rescue Async::TimeoutError
          end

          charging_amount = message.payload[:data].to_i

          message = parent_task.with_timeout OCPP_CLIENT_TIMEOUT do
            ocpp_client.request OCPPClient::CallMessage.new OCPPClient.generate_message_id, :DataTransfer, {
              vendorId: @settings[:ocpp_client][:vendor_id],
              messageId: :GetPrice,
              data: start_transaction_id,
            }

          rescue Async::TimeoutError
          end

          charging_unit_price = message.payload[:data].to_f

          meter_values_task = parent_task.async do |task|
            while true do
              sleep OCPP_CLIENT_METER_VALUES_SEND_INTERVAL

              task.with_timeout OCPP_CLIENT_TIMEOUT do
                ocpp_client.request OCPPClient::CallMessage.new OCPPClient.generate_message_id, :MeterValues, {
                  connectorId: 1,
                  transactionId: start_transaction_id,
                  meterValue: [ {
                    timestamp: Time.now.utc.strftime("%FT%T.%LZ"),
                    sampledValue: [ {
                      value: (@power_meter_values.meter_value - start_meter_value).to_s,
                      unit: "Wh",
                    } ],
                  } ],
                }

              rescue Async::TimeoutError
              end
            end
          end

          @audio_queue.enqueue :charging_started

          @id_tag_reader.clear

          id_tag_read_interval = Async::Clock.now + 8.0

          while true
            if @cp_status == :a || @cp_status == :b
              break
            end

            if (id_tag_read_interval < Async::Clock.now) && (id_tag = @id_tag_reader.receive)
              if start_id_tag == id_tag
                stop_id_tag = id_tag

                break

              else
                @audio_queue.enqueue :charging_stop_failed
              end
            end

            if @power_meter_values.meter_value - start_meter_value > charging_amount
              break
            end

            if remote_stop_transaction_message
              transaction_id = remote_stop_transaction_message.payload[:transactionId]

              remote_stop_transaction_message = nil

              if start_transaction_id == transaction_id
                break
              end
            end

            sleep LOOP_INTERVAL
          end

          meter_values_task.stop

          @pwm_controller.set_duty_cycle 3
          @relay.set_state :open

          Thread.new do
            sleep 600
            @relay.set_state :close
          end

          led_display_queue.enqueue :finished

          parent_task.async do |task|
            task.with_timeout OCPP_CLIENT_TIMEOUT do
              ocpp_client.request OCPPClient::CallMessage.new OCPPClient.generate_message_id, :StopTransaction, {
                idTag: stop_id_tag || start_id_tag,
                meterStop:   @power_meter_values.meter_value,
                timestamp: Time.now.utc.strftime("%FT%T.%LZ"),
                transactionId: start_transaction_id,
                reason: :Local,
                transactionData: [ {
                  timestamp: Time.now.utc.strftime("%FT%T.%LZ"),
                  sampledValue: [ {
                    value: (@power_meter_values.meter_value - start_meter_value).to_s,
                    unit: "Wh",
                  } ],
                } ],
              }

            rescue Async::TimeoutError
            end
          end

          ocpp_status = :Finishing

          parent_task.async do |task|
            task.with_timeout OCPP_CLIENT_TIMEOUT do
              ocpp_client.request OCPPClient::CallMessage.new OCPPClient.generate_message_id, :StatusNotification, {
                connectorId: 1,
                errorCode: :NoError,
                status: ocpp_status,
              }

            rescue Async::TimeoutError
            end
          end

          @audio_queue.enqueue :charging_stopped

          while true
            if @cp_status == :a
              break
            end

            sleep LOOP_INTERVAL
          end

          old_cp_status = :b

        rescue
          @logger.error "#{ $!.class.name }, #{ $!.message }\n  - #{ $@.join "\n  - " }"
        end

      ensure
        @relay.set_state :open
        @pwm_controller.set_duty_cycle 100
        @led_display.clear

        @logger.info "OnFlex, Stop Mode-A"
      end
    end
  end
end

LibC_PRCTL.set_process_name "onflex"
Process.setproctitle "onflex"

if ENV["ONFLEX_ENV"] == "development"
  logger = Logger.new STDOUT

else
  logger = Logger.new File.expand_path("../log/onflex.log", __dir__), "daily"
end

Sync do |parent_task|
  Signal.trap :INT do
    parent_task.stop
  end

  Signal.trap :TERM do
    parent_task.stop
  end

  file_name = "/etc/systemd/system/onflex_updater.service"

  unless File.exist? file_name
    Sync do |task|
      logger.add Logger::INFO, "Started", :OnFlexUpdaterTask

      task.with_timeout 240.0 do
        Async::Process.spawn "echo #{ Shellwords.shellescape <<-EOF.strip } > #{ file_name }"
[Unit]
Description=OnFlexUpdater
After=network-online.target

[Service]
ExecStart=/root/.rbenv/shims/ruby -C /root/onflex -r bundler/setup /root/onflex/app/onflex_updater.rb
RemainAfterExit=true
Type=oneshot
User=root

[Install]
WantedBy=multi-user.target
        EOF

        Async::Process.spawn "systemctl daemon-reload"
        Async::Process.spawn "systemctl enable onflex_updater"
        Async::Process.spawn "systemctl start onflex_updater"
      end

    rescue
      logger.add Logger::ERROR, "#{ $!.message[..80] }\n  #{ $!.backtrace.join "\n  " }", :OnFlexUpdaterTask

    ensure
      logger.add Logger::INFO, "Stopped", :OnFlexUpdaterTask
    end
  end

  Sync do |task|
    Async::Process.spawn "amixer sset PCM 100%"
  end

  result = PIGPIO.gpioInitialise

  if result < 0
    raise RuntimeError, "PIGPIO, gpioInitialise failed (#{ result })"
  end

  begin
    OnFlex::OnFlex.run logger: logger

  ensure
    PIGPIO.gpioTerminate
  end

rescue
  logger.error "OnFlex Error (#{ $!.message[..80] })\n  #{ $!.backtrace.join "\n  " }"
end
