#!/bin/bash

set +e

: ${REPO_ROOT:="$WORKSPACE"}
source $HOME/.rvm/scripts/rvm

cd $REPO_ROOT/hastur-server
rvm --create use 1.9.3@hastur-server
gem install --no-rdoc --no-ri bundler
bundle install
gem build hastur-server.gemspec
gem install hastur-server-*.gem

bundle exec rake --trace test:integrations
