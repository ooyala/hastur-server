ROUTERS = ['router0']
CLIENTS = ['client0', 'client1']
WORKERS = ['worker0', 'worker1', 'worker2']

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
    :command => "./hasturd_cli.rb --router #{ROUTERS[0]} --client #{CLIENTS[0]} --client #{CLIENTS[1]} --method heartbeat"
  },
  {
    :name => WORKERS[1],
    :command => "./hasturd_cli.rb --router #{ROUTERS[0]} --client #{CLIENTS[0]} --client #{CLIENTS[1]} --method register"
  },
  {
    :name => WORKERS[2],
    :command => "./hasturd_cli.rb --router #{ROUTERS[0]} --client #{CLIENTS[0]} --client #{CLIENTS[1]} --method notify"
  },
  {
    :name => WORKERS[3],
    :command => "./hasturd_cli.rb --router #{ROUTERS[0]} --client #{CLIENTS[0]} --client #{CLIENTS[1]} --method schedule"
  },
  {
    :name => WORKERS[4],
    :command => "./hasturd_cli.rb --router #{ROUTERS[0]} --client #{CLIENTS[0]} --client #{CLIENTS[1]} --method log"
  },
  {
    :name => WORKERS[5],
    :command => "./hasturd_cli.rb --router #{ROUTERS[0]} --client #{CLIENTS[0]} --client #{CLIENTS[1]} --method error"
  }

]
