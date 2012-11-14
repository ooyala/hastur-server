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

export PATH="/opt/hastur/bin:/opt/ruby/1.9/bin:/bin:/usr/bin:/usr/local/bin:/opt/local/bin"

if [ -n "$(which unicorn)" ] ; then
  true # ignore rvm/rbenv
elif [ -e "$HOME/.rvm/scripts/rvm" ] ; then
  source "$HOME/.rvm/scripts/rvm"
  rvm use 1.9.3
elif [ -e "$HOME/.rbenv" ] ; then
  export PATH="$HOME/.rbenv/bin:$PATH"
  eval "$(rbenv init -)"
  rbenv shell 1.9.3-p125
fi

UNICORN=$(which unicorn)
PORT=8888

[ -n "$UNICORN" ] || die "could not find a unicorn or hastur install"

killall -u $USER unicorn
sleep 1

DIR="${HOME}/hastur-server"
export RUBYLIB="$DIR/lib:$RUBYLIB"

cat > "${DIR}/${USER}-config.ru" <<EOF
\$LOAD_PATH.unshift File.dirname(__FILE__)

require "multi_json"
require "hastur-server/service/retrieval"
require "hastur-rack"

# defined on most of our Hastur boxes via hastur-deploy
cassandra_servers = []
File.foreach("/opt/hastur/conf/cassandra-servers.txt") do |line|
  line.chomp!
  line.gsub(/\s+#.*$/, '')
  cassandra_servers << line unless line.empty?
end

ENV['CASSANDRA_URIS'] = MultiJson.dump(cassandra_servers, :pretty => false)

use Hastur::Rack, "hastur.retrieval"
run Rack::URLMap.new("/" => Hastur::Service::Retrieval.new)
EOF

cat > "${DIR}/${USER}-unicorn.conf" <<EOF
worker_processes 15
working_directory "$DIR"
listen $PORT, :tcp_nodelay => true
timeout 60
stderr_path "$HOME/unicorn-hastur-error.log"
stdout_path "$HOME/unicorn-hastur-access.log"
EOF

cd $DIR
bundle package
bundle exec unicorn -c "${USER}-unicorn.conf" "${USER}-config.ru" -D

