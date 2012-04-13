#!/bin/bash
set -e

HASTUR_ROOT=/opt/hastur
HASTUR_RUBY_INST=$HASTUR_ROOT/ruby
HASTUR_RUBY_CMD=$HASTUR_RUBY_INST/bin/ruby
HASTUR_GEM_CMD=$HASTUR_RUBY_INST/bin/gem
HASTUR_BUNDLE_CMD=$HASTUR_RUBY_INST/bin/bundle
HASTUR_FPM_CMD=$HASTUR_RUBY_INST/bin/fpm
HASTUR_SERVER_INST=$HASTUR_ROOT/server
HASTUR_API=$HASTUR_ROOT/api
WORKSPACE=`pwd`

rm -rf $HASTUR_ROOT
mkdir -p $HASTUR_ROOT

# install ruby-build
git clone git://github.com/sstephenson/ruby-build.git
cd ruby-build
PREFIX=$HASTUR_ROOT ./install.sh # install in /opt/hastur instead of /usr/local

ruby-build 1.9.3-p125 $HASTUR_RUBY_INST

$HASTUR_GEM_CMD install bundler

rm -rf $HASTUR_SERVER_INST
git clone --depth 1 ssh://git@git.corp.ooyala.com/hastur-server.git $HASTUR_SERVER_INST
cd $HASTUR_SERVER_INST

# likes to run inside a git repo
$HASTUR_BUNDLE_CMD install
# must run in the git repo
$HASTUR_GEM_CMD build hastur-server.gemspec
$HASTUR_GEM_CMD install hastur*.gem

# make sure HASTUR_SERVER_INST is correct, then blow it away
if [ -d "$HASTUR_SERVER_INST/.git" -a -d "$HASTUR_SERVER_INST/pkg" ] ; then
  rm -rf $HASTUR_SERVER_INST
fi

# remove docs, etc. to save space
rm -rf $HASTUR_RUBY_INST/share $HASTUR_RUBY_INST/lib/ruby/gems/1.9.1/doc

# strip shared libraries
find $HASTUR_ROOT -type f -name "*.so" -exec strip {} \;
strip $HASTUR_RUBY_CMD

# use fpm to create the package
$HASTUR_GEM_CMD install fpm

# get the hastur version
HASTUR_VERSION=$($HASTUR_RUBY_CMD -rhastur-server/version -e "puts Hastur::VERSION")

# build a package with the effing package manager
$HASTUR_FPM_CMD --provides hastur-server \
  -a native \
  -m team-tna@ooyala.com \
  -t deb \
  -n hastur-server \
  -v $HASTUR_VERSION \
  --license MIT \
  --vendor Ooyala \
  -s dir \
  $HASTUR_ROOT

mv hastur-server*.deb $WORKSPACE
cd $WORKSPACE
rm -rf $HASTUR_ROOT