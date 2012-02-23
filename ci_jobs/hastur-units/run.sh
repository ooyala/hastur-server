#!/bin/bash

: ${REPO_ROOT:="$WORKSPACE"}
source $HOME/.rvm/scripts/rvm

cd $REPO_ROOT/hastur-server
rvm --create use 1.9.2@hastur-server
gem install --no-rdoc --no-ri bundler
bundle install
bundle exec rake --trace test:units
