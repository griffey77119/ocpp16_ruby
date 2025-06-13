require "logger"

require "async"
require "async/process"

logger = Logger.new "/root/onflex/log/onflex_updater.log", "daily"

Sync do |parent_task|
  parent_task.with_timeout 240.0 do
    exit_status = nil

    until [0, 4].include? exit_status
      command = "cd /root/onflex && bundle add net-ssh"

      logger.add Logger::INFO, "Execute (#{ command })", :OnFlexUpdater

      exit_status = Async::Process.spawn(command).exitstatus

      logger.add Logger::INFO, "ExitCode: #{ exit_status }", :OnFlexUpdater

      sleep 4.0
    end
  end

  command = "echo #{ Shellwords.shellescape <<-EOF.strip } > /etc/systemd/system/onflex_control_server.service"
[Unit]
Description=OnFlexControlServer
After=network-online.target

[Service]
ExecStart=/root/.rbenv/shims/ruby -C /root/onflex -r bundler/setup /root/onflex/app/onflex_control_server.rb
ExecStop=/bin/kill -TERM $MAINPID
RestartSec=60
Restart=always
User=root

[Install]
WantedBy=multi-user.target
  EOF

  logger.add Logger::INFO, "Execute (#{ command })", :OnFlexUpdater

  exit_status = Async::Process.spawn(command).exitstatus

  logger.add Logger::INFO, "ExitCode: #{ exit_status }", :OnFlexUpdater

  command = "systemctl daemon-reload && systemctl enable onflex_control_server && systemctl start onflex_control_server"

  logger.add Logger::INFO, "Execute (#{ command })", :OnFlexUpdater

  exit_status = Async::Process.spawn(command).exitstatus

  logger.add Logger::INFO, "ExitCode: #{ exit_status }", :OnFlexUpdater

  command = "rm -f /root/onflex/app/onflex_updater.rb"

  logger.add Logger::INFO, "Execute (#{ command })", :OnFlexUpdater

  exit_status = Async::Process.spawn(command).exitstatus

  logger.add Logger::INFO, "ExitCode: #{ exit_status }", :OnFlexUpdater

rescue
  logger.add Logger::ERROR, "#{ $!.message[..80] }\n  #{ $!.backtrace.join "\n  " }", :OnFlexUpdater
end
