module PIGPIO
  extend FFI::Library

  ffi_lib File.expand_path "../lib/pigpio/#{ RUBY_PLATFORM }/libpigpio.so", __dir__

  PI_OFF = 0
  PI_ON = 1

  PI_CLEAR = 0
  PI_SET = 1

  PI_LOW = 0
  PI_HIGH = 1

  PI_INPUT = 0
  PI_OUTPUT = 1
  PI_ALT0 = 4
  PI_ALT1 = 5
  PI_ALT2 = 6
  PI_ALT3 = 7
  PI_ALT4 = 3
  PI_ALT5 = 2

  PI_PUD_OFF = 0
  PI_PUD_DOWN = 1
  PI_PUD_UP = 2

  # int gpioInitialise(void)
  attach_function :gpioInitialise, [], :int

  # void gpioTerminate(void)
  attach_function :gpioTerminate, [], :void

  # int gpioSetMode(unsigned gpio, unsigned mode)
  attach_function :gpioSetMode, [:uint, :uint], :int

  # int gpioGetMode(unsigned gpio)
  attach_function :gpioGetMode, [:uint], :int

  # int gpioSetPullUpDown(unsigned gpio, unsigned pud)
  attach_function :gpioSetPullUpDown, [:uint, :uint], :int

  # int gpioRead(unsigned gpio)
  attach_function :gpioRead, [:uint], :int

  # int gpioWrite(unsigned gpio, unsigned level)
  attach_function :gpioWrite, [:uint, :uint], :int

  # int gpioPWM(unsigned user_gpio, unsigned dutycycle)
  attach_function :gpioPWM, [:uint, :uint], :int

  # int gpioSetPWMfrequency(unsigned user_gpio, unsigned frequency)
  attach_function :gpioSetPWMfrequency, [:uint, :uint], :int

  # int gpioSetPWMrange(unsigned user_gpio, unsigned range)
  attach_function :gpioSetPWMrange, [:uint, :uint], :int

  # int gpioHardwarePWM(unsigned gpio, unsigned frequency, unsigned dutycycle)
  attach_function :gpioHardwarePWM, [:uint, :uint, :uint], :int

  # int serOpen(char *sertty, unsigned baud, unsigned serFlags);
  attach_function :serOpen, [:string, :uint, :uint], :int

  # int serClose(unsigned handle);
  attach_function :serClose, [:uint], :int

  # int serWrite(unsigned handle, char *buf, unsigned count);
  attach_function :serWrite, [:uint, :string, :uint], :int

  # int serRead(unsigned handle, char *buf, unsigned count);
  attach_function :serRead, [:uint, :pointer, :uint], :int

  # int serDataAvailable(unsigned handle);
  attach_function :serDataAvailable, [:uint], :int

  # int spiOpen(unsigned spiChan, unsigned baud, unsigned spiFlags)
  attach_function :spiOpen, [:uint, :uint, :uint], :int

  # int spiClose(unsigned handle)
  attach_function :spiClose, [:uint], :int

  # int spiXfer(unsigned handle, char *txBuf, char *rxBuf, unsigned count)
  attach_function :spiXfer, [:uint, :pointer, :pointer, :uint], :int

  # uint32_t gpioDelay(uint32_t micros);
  attach_function :gpioDelay, [:uint32], :uint32

  # uint32_t gpioTick(void);
  attach_function :gpioTick, [], :uint32
end

module OnFlex
  class ACMonitor
    def initialize
      PIGPIO.gpioSetMode PIN_AC_MONITOR, PIGPIO::PI_INPUT
      PIGPIO.gpioSetPullUpDown PIN_AC_MONITOR, PIGPIO::PI_PUD_DOWN
    end

    def get_state
      PIGPIO.gpioRead(PIN_AC_MONITOR) == PIGPIO::PI_HIGH ? :on : :off
    end

  private
    PIN_AC_MONITOR = 27
  end

  class ADCReader
    def initialize
      @write_buffer = FFI::MemoryPointer.new :uchar, BUFFER_SIZE
      @read_buffer = FFI::MemoryPointer.new :uchar, BUFFER_SIZE
      @handle = -1

      PIGPIO.gpioSetMode PIN_SPI1_MISO, PIGPIO::PI_ALT4
      PIGPIO.gpioSetMode PIN_SPI1_MOSI, PIGPIO::PI_ALT4
      PIGPIO.gpioSetMode PIN_SPI1_SCLK, PIGPIO::PI_ALT4
      PIGPIO.gpioSetMode PIN_SPI1_CE2, PIGPIO::PI_OUTPUT
      PIGPIO.gpioWrite PIN_SPI1_CE2, PIGPIO::PI_HIGH
    end

    def open
      return unless @handle < 0

      @handle = PIGPIO.spiOpen CHANNEL, BAUD_RATE, SPI_FLAGS_AUX_SPI

      if @handle < 0
        raise RuntimeError, "spiOpen failed (#{ @handle })"
      end
    end

    def close
      return if @handle < 0

      PIGPIO.spiClose @handle

      @handle = -1
    end

    def read channel, count
      @write_buffer.write_array_of_uchar [0x01, 0x80 | (channel << 4), 0x00]

      Array.new(count) {
        PIGPIO.gpioWrite PIN_SPI1_CE2, PIGPIO::PI_LOW
        PIGPIO.spiXfer @handle, @write_buffer, @read_buffer, BUFFER_SIZE
        PIGPIO.gpioWrite PIN_SPI1_CE2, PIGPIO::PI_HIGH

        value = @read_buffer.read_array_of_uchar BUFFER_SIZE
        ( (value[1] << 8) | value[2] ) & 0x03FF
      }
    end

  private
    PIN_SPI1_CE2 = 16
    PIN_SPI1_MISO = 19
    PIN_SPI1_MOSI = 20
    PIN_SPI1_SCLK = 21

    SPI_FLAGS_AUX_SPI = 256

    BAUD_RATE = 200000
    BUFFER_SIZE = 3
    CHANNEL = 2
  end

  class Button
    def initialize
      PIGPIO.gpioSetMode PIN_BUTTON, PIGPIO::PI_INPUT
      PIGPIO.gpioSetPullUpDown PIN_BUTTON, PIGPIO::PI_PUD_UP
    end

    def state
      PIGPIO.gpioRead(PIN_BUTTON) == PIGPIO::PI_LOW ? :pressed : :unpressed
    end

  private
    PIN_BUTTON = 2
  end

  class CPMonitor
    def initialize reader
      @reader = reader
    end

    def get_state
      count_a, count_b, count_c, count_d = 0, 0, 0, 0

      @reader.read(ADC_CP_MONITOR, BUFFER_SIZE).reverse_each { |value|
        case value
        when 963.9563..1065.4254
          if (count_a += 1) > 5
            break :a
          end

        when 807.7187..892.7418
          if (count_b += 1) > 5
            break :b
          end

        when 657.3769..726.5745
          if (count_c += 1) > 5
            break :c
          end

        when 486.4000..537.6000
          if (count_d += 1) > 5
            break :d
          end
        end
      }
    end

  private
    ADC_CP_MONITOR = 1

    BUFFER_SIZE = 80
  end


  class IDTagReader
    def initialize
      @pointer = FFI::MemoryPointer.new :uchar, BUFFER_SIZE
    end

    def clear
      @read_buffer.clear

      while (length = PIGPIO.serRead @handle, @pointer, BUFFER_SIZE) > 0
      end
    end

    def close
      return unless @handle

      PIGPIO.serClose @handle

      @handle = nil
    end

    def open
      return if @handle

      handle = PIGPIO.serOpen PORT, BAUD_RATE, 0

      if handle < 0
        raise RuntimeError, "serOpen failed (%d)" % handle
      end

      @handle = handle
      @read_buffer = ""
    end

    def receive
      result = nil

      while (length = PIGPIO.serRead @handle, @pointer, BUFFER_SIZE) > 0
        @read_buffer.concat @pointer.read_string_length length

        while (position = @read_buffer.index "S")
          @read_buffer.slice 0, position - 1

          break if @read_buffer.length < 4

          type, length = @read_buffer.unpack "xCC"

          break if @read_buffer.length < length + 4

          result = (@read_buffer.slice! 0, length + 4)[3, length].unpack1 "H*"
        end
      end

      result
    end

  private
    BAUD_RATE = 9600
    BUFFER_SIZE = 1024
    PORT = "/dev/ttyAMA2"
  end

  class LEDDisplay
    def initialize
      @buffer_a = Array.new BUFFER_SIZE, 0x00
      @buffer_b = Array.new BUFFER_SIZE, 0xFF
      @update_count = 0

      PIGPIO.gpioSetMode PIN_LED_CLK, PIGPIO::PI_OUTPUT
      PIGPIO.gpioSetMode PIN_LED_DAT, PIGPIO::PI_OUTPUT
      PIGPIO.gpioWrite PIN_LED_CLK, PIGPIO::PI_HIGH
      PIGPIO.gpioWrite PIN_LED_DAT, PIGPIO::PI_HIGH
    end

    def clear
      @buffer_a.fill 0x00

      changed
    end

    def set_brightness value
      PIGPIO.gpioWrite PIN_LED_DAT, PIGPIO::PI_LOW
      PIGPIO.gpioWrite PIN_LED_CLK, PIGPIO::PI_LOW

      write_byte value > 0 ? TM1640_CMD3 | TM1640_DSP_ON | (value & 7) : TM1640_CMD3

      PIGPIO.gpioWrite PIN_LED_DAT, PIGPIO::PI_LOW
      PIGPIO.gpioWrite PIN_LED_CLK, PIGPIO::PI_HIGH
      PIGPIO.gpioWrite PIN_LED_DAT, PIGPIO::PI_HIGH
    end

    def set_digits digits
      position = 0

      digits.each_char do |char|
        if char == "." && (position > 0)
          @buffer_a[position - 1] |= 0x80

        else
          if (position >= 3) && (position <= 4)
            @buffer_a[position] = DIGIT_LED_SEGMENTS.fetch char, 0

          else
            @buffer_a[position] = @buffer_a[position] & 0x80 | DIGIT_LED_SEGMENTS.fetch(char, 0)
          end

          position += 1
        end
      end

      changed
    end

    def set_leds leds, state
      if state == :on
        leds.each do |led|
          LED_SEGMENTS[led].each do |position, segment|
            @buffer_a[position] |= segment
          end
        end

      else
        leds.each do |led|
          LED_SEGMENTS[led].each do |position, segment|
            @buffer_a[position] &= ~segment
          end
        end
      end

      changed
    end

    def update
      return unless block_given?

      @update_count += 1

      begin
        yield

      ensure
        @update_count -= 1

        changed
      end
    end

  private
    PIN_LED_CLK = 17
    PIN_LED_DAT = 3

    TM1640_CMD1 = 0x44
    TM1640_CMD2 = 0xC0
    TM1640_CMD3 = 0x80
    TM1640_DSP_ON = 0x08

    BUFFER_SIZE = 9

    DIGIT_LED_SEGMENTS = Hash[
      " ", 0b00000000, "0", 0b00111111, "1", 0b00000110, "2", 0b01011011,
      "3", 0b01001111, "4", 0b01100110, "5", 0b01101101, "6", 0b01111101,
      "7", 0b00000111, "8", 0b01111111, "9", 0b01101111, "a", 0b01110111,
      "b", 0b01111100, "c", 0b00111001, "d", 0b01011110, "e", 0b01111001,
      "f", 0b01110001, "h", 0b01110110, "i", 0b00000100, "l", 0b00111000,
      "n", 0b00110111, "n", 0b01010100, "o", 0b01011100, "p", 0b01110011,
      "r", 0b01010000, "t", 0b01111000, "u", 0b00111110, "-", 0b01000000,
      "_", 0b00001000, ".", 0b10000000, "=", 0b01001000,
    ]

    LED_SEGMENTS = Hash[
      :led11, [ [6, 0b11000000], [0, 0b10000000] ],
      :led12, [ [7, 0b00000111] ],
      :led13, [ [7, 0b00111000] ],
      :led14, [ [7, 0b11000000], [8, 0b00000001] ],
      :led15, [ [8, 0b00000110] ],
      :led21, [ [6, 0b00110000] ],
      :led22, [ [6, 0b00001100] ],
      :led23, [ [6, 0b00000011] ],
      :led31, [ [1, 0b10000000] ],
      :led32, [ [2, 0b10000000] ],
      :led33, [ [5, 0b10000000] ],
      :led41, [ [8, 0b00111000] ]
    ]

    def changed
      cmd1_sent = false

      @buffer_a.each_with_index do |segment, position|
        unless @buffer_b[position] == segment
          unless cmd1_sent
            PIGPIO.gpioWrite PIN_LED_DAT, PIGPIO::PI_LOW
            PIGPIO.gpioWrite PIN_LED_CLK, PIGPIO::PI_LOW

            write_byte TM1640_CMD1

            PIGPIO.gpioWrite PIN_LED_DAT, PIGPIO::PI_LOW
            PIGPIO.gpioWrite PIN_LED_CLK, PIGPIO::PI_HIGH
            PIGPIO.gpioWrite PIN_LED_DAT, PIGPIO::PI_HIGH

            cmd1_sent = true
          end

          PIGPIO.gpioWrite PIN_LED_DAT, PIGPIO::PI_LOW
          PIGPIO.gpioWrite PIN_LED_CLK, PIGPIO::PI_LOW

          write_byte TM1640_CMD2 | position
          write_byte segment

          PIGPIO.gpioWrite PIN_LED_DAT, PIGPIO::PI_LOW
          PIGPIO.gpioWrite PIN_LED_CLK, PIGPIO::PI_HIGH
          PIGPIO.gpioWrite PIN_LED_DAT, PIGPIO::PI_HIGH

          @buffer_b[position] = segment
        end
      end
    end

    def write_byte byte
      0.upto 7 do |position|
        PIGPIO.gpioWrite PIN_LED_DAT, byte[position]
        PIGPIO.gpioWrite PIN_LED_CLK, PIGPIO::PI_HIGH
        PIGPIO.gpioWrite PIN_LED_CLK, PIGPIO::PI_LOW
      end
    end
  end

  class PowerMeter
    def initialize
      @buffer = FFI::MemoryPointer.new :uchar, BUFFER_SIZE
    end

    def close
      return unless @handle

      PIGPIO.serClose @handle

      @handle = nil
    end

    def open
      return if @handle

      handle = PIGPIO.serOpen PORT, BAUD_RATE, 0

      if handle < 0
        raise RuntimeError, "serOpen failed (%d)" % handle
      end

      @handle = handle
      @read_buffer = ""
    end

    def receive
      while (length = PIGPIO.serRead @handle, @buffer, BUFFER_SIZE) > 0
        @read_buffer.concat @buffer.read_string_length length

        if @read_buffer[-1] == ">"
          break @read_buffer.slice! 0..-1
        end

        sleep LOOP_INTERVAL
      end
    end

    def send command
      PIGPIO.serWrite @handle, command, command.length
    end

  private
    BAUD_RATE = 19200
    BUFFER_SIZE = 1024
    PORT = "/dev/ttyAMA1"
    RECEIVE_INTERVAL = 0.5
  end

  class PowerLed
    def initialize
      PIGPIO.gpioSetMode PIN_LED_GREEN, PIGPIO::PI_OUTPUT
      PIGPIO.gpioWrite PIN_LED_GREEN, PIGPIO::PI_HIGH
      PIGPIO.gpioSetMode PIN_LED_RED, PIGPIO::PI_OUTPUT
      PIGPIO.gpioWrite PIN_LED_RED, PIGPIO::PI_HIGH
    end

    def set_state led, state
      case led
      when :green
        PIGPIO.gpioWrite PIN_LED_GREEN, state == :on ? PIGPIO::PI_LOW : PIGPIO::PI_HIGH

      when :red
        PIGPIO.gpioWrite PIN_LED_RED, state == :on ? PIGPIO::PI_LOW : PIGPIO::PI_HIGH
      end
    end

  private
    PIN_LED_GREEN = 22
    PIN_LED_RED = 7
  end

  class PWMController
    def initialize
      PIGPIO.gpioSetMode PIN_PWM, PIGPIO::PI_ALT5

      set_duty_cycle 100
    end

    def set_duty_cycle value
      PIGPIO.gpioSetPWMfrequency PIN_PWM, 1_000
      PIGPIO.gpioPWM PIN_PWM, (value * 2.55).truncate
    end

    def set_current value
      case value
      when 6..51
        set_duty_cycle value / 0.6

      when 51..80
        set_duty_cycle value / 2.5 + 64.0
      end
    end

  private
    PIN_PWM = 18
  end

  class RCD
    def initialize
      PIGPIO.gpioSetMode PIN_RCD_CON, PIGPIO::PI_OUTPUT
      PIGPIO.gpioWrite PIN_RCD_CON, PIGPIO::PI_LOW
      PIGPIO.gpioSetMode PIN_RCD_MON, PIGPIO::PI_INPUT
      PIGPIO.gpioSetPullUpDown PIN_RCD_MON, PIGPIO::PI_PUD_UP
    end

    def state
      PIGPIO.gpioRead(PIN_RCD_MON) == PIGPIO::PI_LOW ? :abnormal : :normal
    end

    def active= value
      PIGPIO.gpioWrite PIN_RCD_CON, value ? PIGPIO::PI_HIGH : PIGPIO::PI_LOW
    end

  private
    PIN_RCD_CON = 45
    PIN_RCD_MON = 44
  end

  class Relay
    def initialize
      PIGPIO.gpioSetMode PIN_RELAY_OPEN, PIGPIO::PI_OUTPUT
      PIGPIO.gpioWrite PIN_RELAY_OPEN, PIGPIO::PI_LOW
      PIGPIO.gpioSetMode PIN_RELAY_CLOSE, PIGPIO::PI_OUTPUT
      PIGPIO.gpioWrite PIN_RELAY_CLOSE, PIGPIO::PI_LOW
    end

    def set_state value
      if value == :close
        PIGPIO.gpioWrite PIN_RELAY_CLOSE, PIGPIO::PI_HIGH
        PIGPIO.gpioDelay 40_000
        PIGPIO.gpioWrite PIN_RELAY_CLOSE, PIGPIO::PI_LOW

      else
        PIGPIO.gpioWrite PIN_RELAY_OPEN, PIGPIO::PI_HIGH
        PIGPIO.gpioDelay 40_000
        PIGPIO.gpioWrite PIN_RELAY_OPEN, PIGPIO::PI_LOW
      end
    end

  private
    PIN_RELAY_OPEN = 24
    PIN_RELAY_CLOSE = 25
  end

  class RelayMonitor
    def initialize
      PIGPIO.gpioSetMode PIN_RELAY_MONITOR, PIGPIO::PI_INPUT
      PIGPIO.gpioSetPullUpDown PIN_RELAY_MONITOR, PIGPIO::PI_PUD_DOWN
    end

    def get_state
      count = 0

      8.times do
        if PIGPIO.gpioRead(PIN_RELAY_MONITOR) == PIGPIO::PI_LOW
          return :close
        end

        PIGPIO.gpioDelay 1_000
      end

      :open
    end

  private
    PIN_RELAY_MONITOR = 26
  end

  class Thermometer
    def initialize reader
      @reader = reader
    end

    def get_state
      value = (@reader.read ADC_NTC_THERMISTOR, BUFFER_SIZE).sum.to_f / BUFFER_SIZE
      a1, a2, t, d = TEMPERATURES.find { |a1, a2| a1 <= value && a2 > value }
      a1 && (t + d * (a2 - value) )
    end

  private
    TEMPERATURES = [
      [970.9522, 983.8855, -40, 0.3866],
      [954.7044, 970.9522, -35, 0.3077],
      [934.6428, 954.7044, -30, 0.2492],
      [910.3224, 934.6428, -25, 0.2056],
      [881.3102, 910.3224, -20, 0.1723],
      [847.4982, 881.3102, -15, 0.1479],
      [808.8489, 847.4982, -10, 0.1294],
      [765.7709, 808.8489, -5, 0.1161],
      [718.7168, 765.7709, 0, 0.1063],
      [668.7756, 718.7168, 5, 0.1001],
      [616.7252, 668.7756, 10, 0.0961],
      [564.0498, 616.7252, 15, 0.0949],
      [511.5, 564.0498, 20, 0.0951],
      [460.6031, 511.5, 25, 0.0982],
      [411.8889, 460.6031, 30, 0.1026],
      [366.389, 411.8889, 35, 0.1099],
      [324.2295, 366.389, 40, 0.1186],
      [285.9683, 324.2295, 45, 0.1307],
      [251.5068, 285.9683, 50, 0.1451],
      [220.6471, 251.5068, 55, 0.162],
      [193.3163, 220.6471, 60, 0.1829],
      [169.789, 193.3163, 65, 0.2125],
      [148.641, 169.789, 70, 0.2364],
      [130.3298, 148.641, 75, 0.2731],
      [114.4742, 130.3298, 80, 0.3153],
      [100.5473, 114.4742, 85, 0.359],
      [87.8995, 100.5473, 90, 0.3953],
      [77.5287, 87.8995, 95, 0.4821],
      [68.709, 77.5287, 100, 0.5669],
      [60.6294, 68.709, 105, 0.6188],
      [54.25, 60.6294, 110, 0.7838],
      [47.7855, 54.25, 115, 0.7735],
      [43.1149, 47.7855, 120, 1.0705],
      [0.0, 43.1149, 125, 0.0],
    ]

    ADC_NTC_THERMISTOR = 0

    BUFFER_SIZE = 8
  end
end

class LibC_PRCTL
  module LibC
    PR_SET_NAME = 15

    extend FFI::Library
    ffi_lib "c"
    attach_function :prctl, [:int, :string, :long, :long, :long], :int
  end

  class << self
    def set_process_name value
      LibC.prctl LibC::PR_SET_NAME, value, 0, 0, 0
    end
  end
end

module OnFlex
  class OnFlexAbort < RuntimeError
  end

  class OnFlexError < RuntimeError
    attr_reader :error_code

    def initialize error_code: 99
      @error_code = error_code
    end
  end

  class OnFlex
  private
    EventMessage = Struct.new :action, :payload
    PowerMeterValues = Struct.new :voltage, :current, :meter_value

  public
    def initialize logger: nil
      @logger = logger || Logger.new(nil)

      @settings = JSON.load_file File.expand_path("../config/onflex.cfg", __dir__), symbolize_names: true

      @id_tags = {
        calibration_mode: [],
        manufacturer_mode: [],
      }

      begin
        @id_tags.merge! JSON.load_file File.expand_path("../config/id_tags.cfg", __dir__), symbolize_names: true

      rescue
      end
    end

    def run_mode_b
      task = Async do |parent_task|
        @logger.info "OnFlex, Start calibration mode"

        loop do
          @relay.set_state :close

          led_display_task = parent_task.async do |task|
            @led_display.update do
              @led_display.clear
              @led_display.set_leds [:led11, :led21, :led41], :on
              @led_display.set_digits "cal---"
            end

            blinked = nil

            while true
              @led_display.set_leds [:led12], (blinked = !blinked) ? :on : :off

              sleep LED_DISPLAY_BLINK_INTERVAL
            end
          end

          while true
            @id_tag_reader.clear

            if @id_tags[:manufacturer_mode].include? id_tag
              @mode = :mode_c

              parent_task.stop
              parent_task.yield

            elsif !@id_tags[:calibration_mode].include? id_tag
              @mode = :mode_a

              parent_task.stop
              parent_task.yield
            end

            @relay.set_state :close

            led_display_task.stop

            led_display_task = parent_task.async do |task|
              @led_display.update do
                @led_display.clear
                @led_display.set_leds [:led11, :led12, :led13, :led21, :led41], :on
                @led_display.set_digits "cal---"
              end

              blinked = nil

              while true
                @led_display.set_leds [:led14], (blinked = !blinked) ? :on : :off

                sleep LED_DISPLAY_BLINK_INTERVAL
              end
            end

            parent_task.with_timeout RELAY_TIMEOUT do
              until @relay_monitor.get_state == :close
                sleep LOOP_INTERVAL
              end

            rescue Async::TimeoutError
              raise OnFlexError.new error_code: 11
            end

            while @power_meter.receive
            end

            @power_meter.send "CLD\r\n"

            parent_task.with_timeout POWER_METER_TIMEOUT do
              until @power_meter.receive
                sleep LOOP_INTERVAL
              end

            rescue Async::TimeoutError
              raise OnFlexError.new error_code: 21
            end

            sleep 1.0

            @power_meter.send "CLB\r\n"

            parent_task.with_timeout POWER_METER_TIMEOUT do
              until @power_meter.receive
                sleep LOOP_INTERVAL
              end

            rescue Async::TimeoutError
              raise OnFlexError.new error_code: 22
            end

            until @id_tag_queue.empty?
              @id_tag_queue.deqeue
            end

            start_time = Async::Clock.now

            while Async::Clock.now < (start_time + CALIBRATION_TIME)
              @led_display.set_digits "cal-%02d" % (start_time + CALIBRATION_TIME - Async::Clock.now).to_i

              temp_id_tag = @id_tag_queue.dequeue

              until @id_tag_queue.empty?
                temp_id_tag = @id_tag_queue.dequeue
              end

              if id_tag == temp_id_tag
                break
              end

              sleep 1.0
            end

            stop_time = Async::Clock.now

            @power_meter.send "CLS\r\n"

            parent_task.with_timeout POWER_METER_TIMEOUT do
              until @power_meter.receive
                sleep LOOP_INTERVAL
              end

            rescue Async::TimeoutError
              raise OnFlexError.new error_code: 23
            end

            led_display_task.stop

            led_display_task = parent_task.async do |task|
              @led_display.update do
                @led_display.clear
                @led_display.set_leds [:led11, :led12, :led13, :led14, :led21, :led41], :on
                @led_display.set_digits "cal-%02d" % (start_time + CALIBRATION_TIME - stop_time).to_i
              end

              blinked = nil

              while true
                @led_display.set_leds [:led15], (blinked = !blinked) ? :on : :off

                sleep LED_DISPLAY_BLINK_INTERVAL
              end
            end
          end

        rescue
          @logger.error "#{ $!.class.name }, #{ $!.message }\n  - #{ $@.join "\n  - " }"

          led_display_task.stop

          @led_display.update do
            @led_display.clear
            @led_display.set_leds [:led22, :led41], :on

            case $!
            when OnFlexError
              @led_display.set_digits "err-%02d" % $!.error_code

            else
              @led_display.set_digits "err-99"
            end
          end

          sleep ERROR_DISPLAY_TIME
        end

      rescue
        @logger.error "#{ $!.class.name }, #{ $!.message }\n  - #{ $@.join "\n  - " }"

      ensure
        @relay.set_state :open
        @led_display.clear

        @logger.info "OnFlex, Stop calibration mode"
      end

      task.wait
    end

    def run_mode_c
      task = Async do |parent_task|
        @logger.info "OnFlex, Start manufacturer mode"

        loop do
          @relay.set_state :open

          led_display_task = parent_task.async do |task|
            @led_display.update do
              @led_display.clear
              @led_display.set_leds [:led11, :led21, :led41], :on
              @led_display.set_digits "fac---"
            end

            blinked = nil

            while true
              @led_display.set_leds [:led12], (blinked = !blinked) ? :on : :off

              sleep LED_DISPLAY_BLINK_INTERVAL
            end
          end

          while true
            until @id_tag_queue.empty?
              @id_tag_queue.deqeue
            end

            id_tag = @id_tag_queue.dequeue

            until @id_tag_queue.empty?
              id_tag = @id_tag_queue.dequeue
            end

            if @id_tags[:calibration_mode].include? id_tag
              @mode = :mode_b

              parent_task.stop
              parent_task.yield

            elsif !@id_tags[:manufacturer_mode].include? id_tag
              @mode = :mode_a

              parent_task.stop
              parent_task.yield
            end

            start_id_tag = id_tag

            @relay.set_state :close

            start_meter_value = @power_meter_values.meter_value

            led_display_task.stop

            led_display_task = parent_task.async do |task|
              @led_display.update do
                @led_display.clear
                @led_display.set_leds [:led11, :led12, :led13, :led21, :led41], :on
              end

              task.async do |task|
                blinked = nil

                while true
                  @led_display.set_leds [:led14], (blinked = !blinked) ? :on : :off

                  sleep LED_DISPLAY_BLINK_INTERVAL
                end
              end

              types = [:meter_value, :current].cycle

              while true
                task.with_timeout 4.0 do
                  case types.peek
                  when :meter_value
                    while true
                      @led_display.update do
                        @led_display.set_leds [:led32], :off
                        @led_display.set_leds [:led31], :on
                        @led_display.set_digits "%7.2f" % ( (@power_meter_values.meter_value - start_meter_value) / 1_000.0)
                      end

                      sleep LED_DISPLAY_UPDATE_INTERVAL
                    end

                  when :current
                    while true
                      @led_display.update do
                        @led_display.set_leds [:led31], :off
                        @led_display.set_leds [:led32], :on
                        @led_display.set_digits "%7.2f" % (@power_meter_values.current / 1_000.0)
                      end

                      sleep LED_DISPLAY_UPDATE_INTERVAL
                    end
                  end

                rescue Async::TimeoutError
                end

                types.next
              end
            end

            id_tag = nil

            while @id_tag_reader.receive
            end

            while true
              if id_tag = @id_tag_reader.receive
                while temp_id_tag = @id_tag_reader.receive
                  id_tag = temp_id_tag
                end

                break if start_id_tag == id_tag
              end

              sleep ID_TAG_READER_RECEIVE_INTERVAL
            end

            if @id_tags[:calibration_mode].include? id_tag
              @mode = :mode_b

              parent_task.stop
              parent_task.yield

            elsif !@id_tags[:manufacturer_mode].include? id_tag
              @mode = :mode_a

              parent_task.stop
              parent_task.yield
            end

            @relay.set_state :open

            led_display_task.stop

            led_display_task = parent_task.async do |task|
              @led_display.update do
                @led_display.clear
                @led_display.set_leds [:led11, :led12, :led13, :led14, :led21, :led31, :led41], :on
              end

              task.async do |task|
                blinked = nil

                while true
                  @led_display.set_leds [:led15], (blinked = !blinked) ? :on : :off

                  sleep LED_DISPLAY_BLINK_INTERVAL
                end
              end

              while true
                @led_display.set_digits "%7.2f" % ( (@power_meter_values.meter_value - start_meter_value) / 1_000.0)

                sleep LED_DISPLAY_UPDATE_INTERVAL
              end
            end
          end

        rescue
          @relay.set_state :open

          led_display_task.stop

          @led_display.update do
            @led_display.clear
            @led_display.set_leds [:led22, :led41], :on

            case $!
            when OnFlexError
              @led_display.set_digits "err-%02d" % $!.error_code

            else
              @led_display.set_digits "err-99"
            end
          end

          sleep ERROR_DISPLAY_TIME
        end

      rescue
        @logger.error "#{ $!.class.name }, #{ $!.message }\n  - #{ $@.join "\n  - " }"

      ensure
        @relay.set_state :open
        @led_display.clear

        @logger.info "OnFlex, Stop manufacturer mode"
      end

      task.wait
    end

    def run_mode_d
      task = Async do |parent_task|
        @logger.info "OnFlex, Start Mode-D"

        power_meter_values = PowerMeterValues.new 0, 0, 0
        start_meter_value = nil

        display_queue = Async::LimitedQueue.new

        parent_task.async do |task|
          @led_display.set_leds [:led41], :on

          sub_task = nil

          while true
            action, payload = display_queue.dequeue

            sub_task&.stop

            case action
            when :preparing
              sub_task = task.async do |task|
                @led_display.set_leds [:led22, :led23], :off
                @led_display.set_leds [:led21], :on

                task.async do |task|
                  state = nil

                  while true
                    if @cp_status == :a
                      @led_display.set_leds [:led12, :led13, :led14, :led15], :off
                      @led_display.set_leds [:led11], state

                    else
                      @led_display.set_leds [:led13, :led14, :led15], :off
                      @led_display.set_leds [:led11], :on
                      @led_display.set_leds [:led12], state
                    end

                    state = state == :on ? :off : :on

                    sleep LED_DISPLAY_BLINK_INTERVAL
                  end
                end

                while true
                  @led_display.set_leds [:led32, :led33], :off
                  @led_display.set_leds [:led31], :on
                  @led_display.set_digits "%7.2f" % (power_meter_values.meter_value / 1_000.0)

                  sleep LED_DISPLAY_UPDATE_INTERVAL
                end
              end

            when :charging
              sub_task = task.async do |task|
                @led_display.set_leds [:led22, :led23], :off
                @led_display.set_leds [:led21], :on

                task.async do |task|
                  state = nil

                  while true
                    @led_display.set_leds [:led15], :off
                    @led_display.set_leds [:led11, :led12, :led13], :on
                    @led_display.set_leds [:led14], state

                    state = state == :on ? :off : :on

                    sleep LED_DISPLAY_BLINK_INTERVAL
                  end
                end

                types = [:meter_value, :current].cycle

                while true
                  task.with_timeout 4.0 do
                    while true
                      case types.peek
                      when :meter_value
                        @led_display.set_leds [:led32, :led33], :off
                        @led_display.set_leds [:led31], :on
                        @led_display.set_digits "%7.2f" % ( (start_meter_value - power_meter_values.meter_value) / 1_000.0)

                      when :current
                        @led_display.set_leds [:led31, :led33], :off
                        @led_display.set_leds [:led32], :on
                        @led_display.set_digits "%7.2f" % (@power_meter_values.current / 1_000.0)
                      end

                      sleep LED_DISPLAY_UPDATE_INTERVAL
                    end

                  rescue Async::TimeoutError
                    types.next
                  end
                end
              end

            when :finishing
              sub_task = task.async do |task|
                @led_display.set_leds [:led22, :led23], :off
                @led_display.set_leds [:led21], :on

                task.async do |task|
                  state = nil

                  while true
                    @led_display.set_leds [:led11, :led12, :led13, :led14], :on
                    @led_display.set_leds [:led15], state

                    state = state == :on ? :off : :on

                    sleep LED_DISPLAY_BLINK_INTERVAL
                  end
                end

                while true
                  @led_display.set_leds [:led32, :led33], :off
                  @led_display.set_leds [:led31], :on
                  @led_display.set_digits "%7.2f" % ( (start_meter_value - power_meter_values.meter_value) / 1_000.0)

                  sleep LED_DISPLAY_UPDATE_INTERVAL
                end
              end

            when :error
              @led_display.clear
              @led_display.set_digits "err-%02d" % payload[:error_code]
            end
          end
        end

        display_queue.enqueue :preparing

        loop do
          @pwm_controller.set_duty_cycle 100
          @relay.set_state :open

          until @cp_status == :b
            sleep LOOP_INTERVAL
          end

          display_queue.enqueue :preparing

          @pwm_controller.set_current 32

          until @cp_status == :c
            sleep LOOP_INTERVAL
          end

          display_queue.enqueue :charging

          start_meter_value = @power_meter_values.meter_value

          @relay.set_state :close

          until @cp_status == :a || @cp_status == :b
            sleep LOOP_INTERVAL
          end

          display_queue.enqueue :finishing

          raise

        rescue
          @pwm_controller.set_duty_cycle 100
          @relay.set_state :open

          display_queue.enqueue [:error, error_code: 98]

          sleep ERROR_DISPLAY_TIME

          display_queue.enqueue :preparing
        end

      rescue
        logger.error "#{ $!.class.name }, #{ $!.message }\n  - #{ $@.join "\n  - " }"

      ensure
        @relay.set_state :open
        @pwm_controller.set_duty_cycle 100
        @led_display.clear

        @logger.info "OnFlex, Stop Mode-D"
      end

      task.wait
    end

    def run
      task = Async do |parent_task|
        @logger.info "OnFlex, Service started."

        @power_led = PowerLed.new
        @power_led.set_state :green, :on
        @power_led.set_state :red, :off

        adc_reader = ADCReader.new
        adc_reader.open

        @id_tag_reader = IDTagReader.new
        @id_tag_reader.open

        @led_display = LEDDisplay.new
        @led_display.clear
        @led_display.set_brightness 6

        @pwm_controller = PWMController.new

        @power_meter = PowerMeter.new
        @power_meter.open

        @relay = Relay.new
        @relay_monitor = RelayMonitor.new

        @audio_queue = Async::Queue.new
        @cp_status = nil
        @power_meter_values = PowerMeterValues.new 0, 0, 0
        @temperature = nil

        parent_task.async do |task|
          cp_monitor = CPMonitor.new adc_reader

          cp_status = nil

          while true
            @cp_status = cp_monitor.get_state

            unless cp_status == @cp_status
              @logger.info "CPMonitor, Changed (%s -> %s)" % [cp_status, @cp_status]

              cp_status = @cp_status
            end

            sleep CP_MONITOR_READ_INTERVAL
          end
        end

        parent_task.async do |task|
          thermometer = Thermometer.new adc_reader
          notification = Async::Condition.new

          task.async do |task|
            while true
              notification.wait

              @logger.info "ThermometerMonitor, Readed (%.2f°C)" % @temperature

              sleep THERMOMETER_MONITOR_LOG_INTERVAL
            end
          end

          while true
            @temperature = thermometer.get_state

            notification.signal

            sleep THERMOMETER_MONITOR_READ_INTERVAL
          end
        end

        parent_task.async do |task|
          player_task = nil

          while audio_name = @audio_queue.dequeue
            file_name = File.expand_path("../sounds/#{ audio_name.to_s }.wav", __dir__)

            player_task&.stop

            player_task = task.async do |task|
              Async::Process.spawn "aplay", file_name
            end
          end

        ensure
          player_task&.stop
        end

        power_meter_task = nil
        service_task = nil

        @mode = :mode_a

        while true
          service_task&.stop

          case @mode
          when :mode_a, :mode_c, :mode_d
            power_meter_task ||= parent_task.async do |task|
              notification = Async::Condition.new

              task.async do |task|
                while true
                  notification.wait

                  @logger.info "PowerMeter, Received (MeterValue: %.2fkW, Voltage: %.1fV, Current: %.2fA)" % [
                    @power_meter_values.meter_value / 1_000.0,
                    @power_meter_values.voltage / 10.0,
                    @power_meter_values.current / 1_000.0,
                  ]

                  sleep POWER_METER_LOG_INTERVAL
                end
              end

              while true
                @power_meter.send "M30\r\n"

                task.with_timeout POWER_METER_RECEIVE_TIMEOUT do
                  while true
                    if message = @power_meter.receive
                      if match_data = message.match(/^S,ALL,([^,]*),([^,]*),[^,]*,([^,]*),[^,]*,E\s*$/)
                        @power_meter_values.voltage, @power_meter_values.current, @power_meter_values.meter_value = match_data.captures.map &:to_i

                        notification.signal

                        break
                      end
                    end

                    sleep LOOP_INTERVAL
                  end

                rescue Async::TimeoutError
                end

                sleep POWER_METER_SEND_INTERVAL
              end

            ensure
              power_meter_task = nil
            end

          when :mode_b
            power_meter_task&.stop
          end

          case @mode
          when :mode_b
            run_mode_b

          when :mode_c
            run_mode_c

          when :mode_d
            run_mode_d

          else
            service_task = run_mode_a
          end

          service_task&.wait

          sleep LOOP_INTERVAL
        end

      ensure
        service_task&.stop

        @id_tag_reader.close
        @power_meter.close

        adc_reader.close

        @power_led.set_state :green, :off
        @power_led.set_state :red, :off

        @logger.info "OnFlex, Service stopped."
      end

      task.wait
    end

    def self.run ...
      self.new(...).run
    end
  end
end

module OnFlex
  class OCPPClient
    CallErrorMessage = Struct.new :message_id, :error_code, :error_description, :error_details
    CallMessage = Struct.new :message_id, :action, :payload
    CallResultMessage = Struct.new :message_id, :payload

    def initialize settings:, logger: nil
      @logger = logger || Logger.new(nil)

      @settings = settings

      @receive_notification = Async::Notification.new
      @received_messages = []
      @send_message_queue = Async::Queue.new
    end

    def receive_call
      while true
        if index = @received_messages.find_index { |message| message.is_a? CallMessage }
          break @received_messages.delete_at index
        end

        @receive_notification.wait
      end
    end

    def receive message_id
      while true
        if index = @received_messages.find_index { |message| message.message_id == message_id }
          break @received_messages.delete_at index
        end

        @receive_notification.wait
      end
    end

    def request message
      send message
      receive message.message_id
    end

    def send message
      @send_message_queue.enqueue message
    end

    def start
      return if @task

      @task = Async do |parent_task|
        boot_notification_sent = nil
        heartbeat_interval = 60.0

        endpoint = Async::HTTP::Endpoint.parse @settings[:endpoint]

        headers = {
          "Sec-WebSocket-Protocol": "ocpp1.6",
        }

        if @settings[:headers]
          headers.merge! @settings[:headers]
        end

        loop do
          client_task = parent_task.async do |client_task|
            connection = Async::WebSocket::Client.connect endpoint, headers: headers

            @logger.info "OCPPClient, Connected"

            send_message_queue = Async::Queue.new

            receive_task = client_task.async do |receive_task|
              while true
                text_message = connection.read

                @logger.info "OCPPClient, Received (#{ text_message.to_str })"

                message = Protocol::WebSocket::JSONMessage.wrap(text_message).parse

                @received_messages.push(
                  case message[0]
                  when CALL
                    CallMessage.new message[1], message[2].to_sym, message[3]

                  when CALL_RESULT
                    CallResultMessage.new message[1], message[2]

                  when CALL_ERROR
                    CallErrorMessage.new message[1], message[2], message[3], message[4]

                  else
                    raise RuntimeError
                  end
                )

                @receive_notification.signal
              end

            rescue
              client_task.stop

            ensure
              receive_task = nil
            end

            send_task = client_task.async do |send_task|
              while true
                message = send_message_queue.dequeue

                text_message = Protocol::WebSocket::JSONMessage.generate(
                  case message
                  when CallMessage
                    [CALL, message.message_id, message.action, message.payload]

                  when CallResultMessage
                    [CALL_RESULT, message.message_id, message.payload]

                  when CallErrorMessage
                    [CALL_ERROR, message.message_id, message.error_code, message.error_description, message.error_details]
                  end
                )

                connection.write text_message
                connection.flush

                @logger.info "OCPPClient, Sent (#{ text_message.to_str })"
              end

            rescue
              client_task.stop

            ensure
              send_task = nil
            end

            unless boot_notification_sent
              message = client_task.with_timeout RECEIVE_TIMEOUT do
                message_id = OCPPClient.generate_message_id

                send_message_queue.enqueue CallMessage.new message_id, :BootNotification, {
                  chargePointModel: @settings[:model_id],
                  chargePointVendor: @settings[:vendor_id],
                  chargePointSeriaNumber: @settings[:serial_number],
                  firmwareVersion: FIRMWARE_VERSION,
                }

                receive message_id
              end

              unless CallResultMessage === message
                raise RuntimeError
              end

              unless message.payload[:status] == "Accepted"
                raise RuntimeError
              end

              heartbeat_interval = message.payload[:interval]

              boot_notification_sent = true
            end

            client_task.async do |task|
              while true
                sleep heartbeat_interval

                task.with_timeout RECEIVE_TIMEOUT do
                  message_id = OCPPClient.generate_message_id
                  send_message_queue.enqueue CallMessage.new message_id, :Heartbeat, {}
                  receive message_id
                end
              end

            ensure
              client_task.stop
            end

            while true
              send_message_queue.enqueue @send_message_queue.dequeue

              sleep LOOP_INTERVAL
            end

          ensure
            send_task&.stop
            receive_task&.stop

            if connection
              connection.close

              @logger.info "OCPPClient, Disconnected"
            end
          end

          client_task.wait

          sleep RECONNECTION_INTERVAL

        rescue
          @logger.error "OCPPClient, #{ $!.class.name }: #{ $!.message }\n  - #{ $@.join "\n  - " }"

          sleep RECONNECTION_INTERVAL
        end

      ensure
        @task = nil
      end
    end

    def stop
      return unless @task

      @task.stop
    end

    def self.generate_message_id
      SecureRandom.uuid.unpack("a8xa4xa4").join
    end

  private
    CALL = 2
    CALL_RESULT = 3
    CALL_ERROR = 4

    RECEIVE_TIMEOUT = 8.0
    RECONNECTION_INTERVAL = 8.0
  end
end
