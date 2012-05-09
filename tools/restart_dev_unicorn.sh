#!/bin/bash
#
# Set up a config.ru & unicorn.conf on the fly and bounce unicorn as the
# user who is running this script. Not intended for production use in any
# way, shape, or form. The Rakefile uses this to bounce cassandra when you
# use "rake push_dev" to work on the retrieval service.
#

cd "$(dirname $0)/.."

die () {
  echo "$*"
  exit 1
}

[ -n "$USER" ] || die "USER envvar must be set"
[ -n "$HOME" ] || die "HOME envvar must be set"

# detect RVM then RBenv then fall back to /opt/hastur, which should
# be sufficient most of the time
if [ -e "$HOME/.rvm/scripts/rvm" ] ; then
  source "$HOME/.rvm/scripts/rvm"
  rvm use 1.9.3
elif [ -e "$HOME/.rbenv" ] ; then
  export PATH="$HOME/.rbenv/bin:$PATH"
  eval "$(rbenv init -)"
  rbenv shell 1.9.3-p125
fi

UNICORN=$(which unicorn)

# fall back to Hastur
[ -n "$UNICORN" ] || export PATH="/opt/hastur/bin:$PATH"
UNICORN=$(which unicorn)

[ -n "$UNICORN" ] || die "could not find a unicorn or hastur install"

killall -u $USER unicorn
sleep 1

if [ "$USER" == "viet" ] ; then
  PORT=8080
else
  PORT=8888
fi

DIR="${HOME}/hastur-server"
export RUBYLIB="$DIR/lib:$RUBYLIB"

cat > "${DIR}/${USER}-config.ru" <<EOF
\$LOAD_PATH.unshift File.dirname(__FILE__)

require "hastur-server/service/retrieval"

# defined on most of our Hastur boxes via hastur-deploy
cassandra_servers = []
File.foreach("/opt/hastur/conf/cassandra-servers.txt") do |line|
  line.chomp!
  line.gsub(/\s+#.*$/, '')
  cassandra_servers << line unless line.empty?
end

run Rack::URLMap.new("/" => Hastur::Service::Retrieval.new(cassandra_servers))
EOF

cat > "${DIR}/${USER}-unicorn.conf" <<EOF
worker_processes 5
working_directory "$DIR"
listen $PORT, :tcp_nodelay => true
timeout 60
stderr_path "$HOME/unicorn-hastur-error.log"
stdout_path "$HOME/unicorn-hastur-access.log"
EOF

cd $DIR
unicorn -c "${USER}-unicorn.conf" "${USER}-config.ru" -D

