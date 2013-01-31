#!/bin/bash
set -ex

eval "$(rbenv init -)"

# The current working directory is exactly WORKSPACE.
# This is where each repo the test requires is checked out to.
: ${OOYALA_REPO_ROOT:="$WORKSPACE"}

rbenv shell jruby-1.7.0

export JRUBY_OPTS="-Xcext.enabled=true"

gem install --no-rdoc --no-ri bundler

# Move to the project repo
cd $WORKSPACE/hastur-server

# install the necessary gems and execute tests
bundle update

# just in case we installed some executables...
rbenv rehash

rake native_jar

COVERAGE=true bundle exec rake --trace test:units:full
