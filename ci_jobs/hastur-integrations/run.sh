#!/bin/bash

: ${REPO_ROOT:="$WORKSPACE/ooyala"}
source $HOME/.rvm/scripts/rvm

cd $REPO_ROOT/hastur
rvm --create use 1.9.2@hastur
bundle install
rake --trace test:integrations
