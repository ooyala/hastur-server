#!/bin/bash

# a mod of the stuff in hastur to build a bunch of ruby packages for general use

set -x
set -e

APP_ROOT=/opt/chef
ROOT_ROOT=/snapfs/roots
SNAP_ROOT=/snapfs/snapshots

CHEF_VERSION="10.16.2"
RUBY_VERSION="1.9.3-p327"
ARCHITECTURES="i386 amd64"
DISTRIBUTIONS="hardy lucid precise"

PACKAGE_VERSION=$(date -I |sed 's/-/./g')
SNAP_SUFFIX="chef-$(date +%s)"
RUBY_BUILD="/home/al/src/ruby-build"
EMAIL="al@ooyala.com"

# ruby-build picks up these variables from its environment
[ -e /proc/cpuinfo ] && export MAKEOPTS="-j$(grep vendor_id /proc/cpuinfo |wc -l)"
export CONFIGURE_OPTS="--enable-pthread --enable-shared"

# I have a squid proxy on my box so I don't abuse upstream
if [ -x /etc/rc.d/squid ] ; then
  export http_proxy=http://127.0.0.1:3128
  wget -O /dev/null http://tobert.org/js/prototype.js 2>/dev/null
  if [ $? -ne 0 ] ; then
    /etc/rc.d/squid start
  fi
fi

die () {
  echo "$*"
  exit 1
}

require () {
  if [ $? != 0 ] ; then
    set +x
    echo "FAILED: $*"
    caller -1
    echo "Variables: "
    echo "arch:     $arch"
    echo "dist:     $dist"
    echo "pkg:      $pkg"
    echo "snapshot: $snapshot"
    echo "root:     $root"
    echo "CWD:      $(pwd)"
    exit 1
  fi
}

prepare_snapshot () {
  path=$1
  cp -f /etc/resolv.conf $path/etc
}

build_a_ruby () {
  path=$1
  personality=$2
  archflag=$3

  echo "Copying ruby-build into $path ..."
  rsync -a $RUBY_BUILD $path/tmp
  require "rsync ruby-build"

  export LDFLAGS="-Wl,-rpath -Wl,$APP_ROOT/lib $archflag"
  export CFLAGS="-O2 $archflag -fno-strict-aliasing -mtune=generic -pipe"

  ffi=$(chroot $path apt-cache search libffi[0-9]*-dev |awk '/libffi/{print $1}')

  $personality chroot $path apt-get install --force-yes -y -o "DPkg::Options::=--force-confold" build-essential apt-utils gpgv libssl-dev zlib1g-dev curl git-core lsb-release uuid-dev libreadline-dev $ffi pkg-config
  $personality chroot $path bash -c "cd /tmp/ruby-build && ./install.sh"
  require "install ruby-build inside chroot"
  $personality chroot $path bash -c "/usr/local/bin/ruby-build $RUBY_VERSION $APP_ROOT"
  require "ruby-build $RUBY_VERSION $APP_ROOT"
}

package_a_ruby () {
  path=$1
  personality=$2

  gem="$APP_ROOT/bin/gem"
  fpm="$APP_ROOT/bin/fpm"
  gemopts="--no-ri --no-rdoc"

  $personality chroot $path $gem install $gemopts fpm
  $personality chroot $path $gem install $gemopts pry
  $personality chroot $path $gem install $gemopts bundler
  $personality chroot $path $gem install $gemopts hastur
  $personality chroot $path $gem install $gemopts erubis
  $personality chroot $path $gem install $gemopts chef
  $personality chroot $path $gem install $gemopts minitest-chef-handler

  distro=$(chroot $path lsb_release -c |awk '{print $2}')
  ffi=$(chroot $path apt-cache search libffi[0-9]*-dev |awk '/libffi/{print $1}')
  readline=$(chroot $path apt-cache search libreadline[0-9]*-dev |awk '/libreadline/{print $1}')

  cat > $path/tmp/fpm.sh <<EOF
cd /tmp
$fpm \
    --name chef-$CHEF_VERSION \
    --provides chef \
    --category ruby \
    --version $PACKAGE_VERSION \
    --iteration $distro \
    --description "Chef with embedded ruby $RUBY_VERSION as built by ruby-build" \
    -m $EMAIL \
    -a native \
    -t deb \
    --license MIT \
    --vendor Ooyala \
    --depends zlib1g \
    --depends openssl \
    --depends readline-common \
    -s dir \
    $APP_ROOT
EOF

  # remove docs
  $personality chroot $path rm -rf $APP_ROOT/lib/gems/*/doc $APP_ROOT/share/{doc,man,ri}

  $personality chroot $path bash -ex /tmp/fpm.sh
}

if [ ! -d $RUBY_BUILD ] ; then
  mkdir -p $HOME/src && cd $HOME/src && \
  git clone https://github.com/sstephenson/ruby-build.git
else
  cd $RUBY_BUILD && git pull
fi

for arch in $ARCHITECTURES
do
  for dist in $DISTRIBUTIONS
  do
    root="$ROOT_ROOT/$dist-$arch"
    snapshot="$SNAP_ROOT/$dist-$arch-chef-$CHEF_VERSION-$SNAP_SUFFIX"

    if [ "$arch" == "i386" ] ; then
      personality=$(which linux32)
      archflag="-m32"
      pkg_arch="i386"
      [ -n "$personality" ] || die "could not find 'linux32' utility"
    else
      # don't worry about it if it's not there, don't really need it on 64-bit systems
      personality=$(which linux64)
      archflag="-m64"
      pkg_arch="amd64"
    fi

    btrfs subvolume snapshot $root $snapshot
    require btrfs subvolume snapshot $root $snapshot

    prepare_snapshot $root

    build_a_ruby $snapshot $personality $archflag
    package_a_ruby $snapshot $personality

    mkdir -p /tmp/chef
    cp $(find $snapshot/tmp -name "chef*$CHEF_VERSION*.deb") /tmp/chef

    btrfs subvolume delete $snapshot
  done
done

ls -l /tmp/chef
