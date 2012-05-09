#!/bin/bash

# a mod of the stuff in hastur to build a bunch of ruby packages for general use

set -x
set -e

RUBY_ROOT=/opt/ruby
ROOT_ROOT=/snapfs/roots
SNAP_ROOT=/snapfs/snapshots

RUBY_VERSIONS="1.9.2-p290 1.9.2-p320 1.9.3-p125 1.9.3-p194 1.8.7-p358"
# ree-1.8.7-2012.02 build fails on precise, debug later if anybody cares
ARCHITECTURES="i386 amd64"
DISTRIBUTIONS="hardy lucid precise"

# temp
#ARCHITECTURES="amd64"
#DISTRIBUTIONS="precise"

PACKAGE_VERSION=$(date -I |sed 's/-/./g')
SNAP_SUFFIX="ruby-$(date +%s)"
RUBY_BUILD="$HOME/src/ruby-build"
EMAIL="al@ooyala.com"

# ruby-build picks up these variables from its environment
[ -e /proc/cpuinfo ] && export MAKEOPTS="-j$(grep vendor_id /proc/cpuinfo |wc -l)"
CONFIGURE_OPTS="--enable-pthread --enable-shared"

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
  version=$1
  path=$2
  personality=$3
  archflag=$4
  prefix="$RUBY_ROOT/$version"

  echo "Copying ruby-build into $path ..."
  rsync -a $RUBY_BUILD $path/tmp
  require "rsync ruby-build"

  export LDFLAGS="-Wl,-rpath -Wl,$prefix/lib $archflag"
  export CFLAGS="-O2 $archflag -fno-strict-aliasing -mtune=generic -pipe"

  ffi=$(chroot $path apt-cache search libffi[0-9]*-dev |awk '/libffi/{print $1}')

  $personality chroot $path apt-get install --force-yes -y -o "DPkg::Options::=--force-confold" build-essential apt-utils gpgv libssl-dev zlib1g-dev curl git-core lsb-release uuid-dev libreadline-dev $ffi pkg-config
  $personality chroot $path bash -c "cd /tmp/ruby-build && ./install.sh"
  require "install ruby-build inside chroot"
  $personality chroot $path bash -c "/usr/local/bin/ruby-build $version $prefix"
  require "ruby-build $version $prefix"
}

package_a_ruby () {
  version=$1
  path=$2
  personality=$3

  ruby="$RUBY_ROOT/$version"
  gem="$ruby/bin/gem"
  fpm="$ruby/bin/fpm"

  $personality chroot $path $gem install fpm
  $personality chroot $path $gem install unicorn
  $personality chroot $path $gem install bundler

  distro=$(chroot $path lsb_release -c |awk '{print $2}')
  ffi=$(chroot $path apt-cache search libffi[0-9]*-dev |awk '/libffi/{print $1}')
  readline=$(chroot $path apt-cache search libreadline[0-9]*-dev |awk '/libreadline/{print $1}')

  cat > $path/tmp/fpm.sh <<EOF
cd /tmp
$fpm \
    --name ruby-$version \
    --provides ruby \
    --category ruby \
    --version $PACKAGE_VERSION \
    --iteration $distro \
    --description "Ruby $version as built by ruby-build" \
    -m $EMAIL \
    -a native \
    -t deb \
    --license MIT \
    --vendor Ooyala \
    --depends zlib1g-dev \
    --depends libssl-dev \
    --depends libreadline-dev \
    -s dir \
    $ruby
EOF

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

    for version in $RUBY_VERSIONS
    do
      snapshot="$SNAP_ROOT/$dist-$arch-ruby-$version-$SNAP_SUFFIX"

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

      [ -e "/tmp/rubies/ruby_${version}-${dist}_${pkg_arch}.deb" ] && continue

      btrfs subvolume snapshot $root $snapshot
      require btrfs subvolume snapshot $root $snapshot

      prepare_snapshot $root

      build_a_ruby $version $snapshot $personality $archflag
      package_a_ruby $version $snapshot $personality

      mkdir -p /tmp/rubies
      cp $(find $snapshot/tmp -name "ruby*$version*.deb") /tmp/rubies

      btrfs subvolume delete $snapshot
    done
  done
done

ls -l /tmp/rubies
