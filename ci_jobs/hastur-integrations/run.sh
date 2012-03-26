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

cd $REPO_ROOT/hastur-server
rvm --create use 1.9.3@hastur-server
gem uninstall bundler -v 1.1.1
gem install --no-rdoc --no-ri bundler
bundle install
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

COVERAGE=true bundle exec rake --trace test:integrations
