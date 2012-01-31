# TODO(viet): Should this be JSON? Does it matter?

ROUTERS = ['router0']
CLIENTS = ['client0', 'client1']
WORKERS = ['worker0', 'worker1', 'worker2', 'worker3', 'worker4', 'worker5']

TOPOLOGY = [
  {
    :name => CLIENTS[0]},
    :command => "./hastur-client.rb --router #{ROUTERS[0]} --port 8125"
  },
  {
    :name => CLIENTS[1]},
    :command => "./hastur-client.rb --router #{ROUTERS[0]} --port 8125"
  },
  {
    :name => ROUTERS[0]},
    :command => <<-eos
      ./hastur-router.rb --heartbeat-uri #{WORKERS[0]} --register-uri #{WORKERS[1]} 
                         --notify-uri #{WORKERS[2]} --schedule-uri #{WORKERS[3]}
                         --log-uri #{WORKERS[4]} --error-uri #{WORKERS[5]}
    eos
  },
  {
    :name => WORKERS[0],
    :command => <<-eos
      ./hasturd_cli.rb --router #{ROUTERS[0]} --client #{CLIENTS[0]} --client #{CLIENTS[1]} --method heartbeat
    eos
  },
  {
    :name => WORKERS[1],
    :command => <<-eos
      ./hasturd_cli.rb --router #{ROUTERS[0]} --client #{CLIENTS[0]} --client #{CLIENTS[1]} --method register
    eos
  },
  {
    :name => WORKERS[2],
    :command => <<-eos
      ./hasturd_cli.rb --router #{ROUTERS[0]} --client #{CLIENTS[0]} --client #{CLIENTS[1]} --method notify
    eos
  },
  {
    :name => WORKERS[3],
    :command => <<-eos
      ./hasturd_cli.rb --router #{ROUTERS[0]} --client #{CLIENTS[0]} --client #{CLIENTS[1]} --method schedule
    eos
  },
  {
    :name => WORKERS[4],
    :command => <<-eos
      ./hasturd_cli.rb --router #{ROUTERS[0]} --client #{CLIENTS[0]} --client #{CLIENTS[1]} --method log
    eos
  },
  {
    :name => WORKERS[5],
    :command => <<-eos
      ./hasturd_cli.rb --router #{ROUTERS[0]} --client #{CLIENTS[0]} --client #{CLIENTS[1]} --method error
    eos
  }

]
