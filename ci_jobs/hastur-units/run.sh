#!/bin/bash

: ${REPO_ROOT:="$WORKSPACE"}
source $HOME/.rvm/scripts/rvm

cd $REPO_ROOT/hastur-server
rvm list | grep jruby-1.7.0 || rvm install jruby-1.7.0
rvm --create use jruby-1.7.0@hastur-server
gem uninstall bundler -v 1.1.1
gem install --no-rdoc --no-ri bundler -v 1.1.0
bundle update   # Update to latest versions since this is a gem
#bundle install
COVERAGE=true bundle exec rake test:units:full --trace
