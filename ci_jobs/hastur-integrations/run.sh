#!/bin/bash

set +e

unset CASSANDRA_HOME

echo "---------------------------------------"
ulimit -a
echo "---------------------------------------"
env
echo "---------------------------------------"

: ${REPO_ROOT:="$WORKSPACE"}
export IS_JENKINS="true"

source $HOME/.rvm/scripts/rvm

export JRUBY_OPTS="-Xcext.enabled=true"

cd $REPO_ROOT/hastur-server
rvm list | grep jruby-1.7.0 || rvm install jruby-1.7.0
rvm --create use jruby-1.7.0@hastur-server
gem uninstall bundler -v 1.1.1
gem install --no-rdoc --no-ri bundler
bundle update   # Update to latest versions since this is a gem
#bundle install
rm hastur-server-*.gem
gem build hastur-server.gemspec
gem install hastur-server-*.gem

gem env
echo "------------ Bundler-ized environment -----"
bundle exec gem env
bundle exec env
echo "-------------------------------------------"


echo "
===============================================================================
===============================================================================
"

COVERAGE=true bundle exec rake test:integrations --trace
