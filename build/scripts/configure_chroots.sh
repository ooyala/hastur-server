#!/bin/bash
set -x
#
# Configure freshly-installed debootstrap chroots to the point where they can
# be used to build the hastur packages. This assumes we're building for hardy, lucid,
# and precise.
#
# The system ruby is variously broken on all of them to the point where it doesn't
# make sense to try to unscrew it, so just install 1.9.3 in /opt instead. This will
# make it possible to run the Rakefile in hastur-server.
#
# This only needs to be run once on the roots, then the rest is done in snapshots
# of those roots in rake tasks.
#

ROOTS=/snapfs/roots

cat > $ROOTS/hardy-amd64/etc/apt/sources.list << EOF
deb http://us.archive.ubuntu.com/ubuntu/ hardy main restricted universe multiverse
deb http://us.archive.ubuntu.com/ubuntu/ hardy-updates main restricted universe multiverse
deb http://security.ubuntu.com/ubuntu hardy-security main restricted universe multiverse
EOF

cat > $ROOTS/hardy-i386/etc/apt/sources.list << EOF
deb http://us.archive.ubuntu.com/ubuntu/ hardy main restricted universe multiverse
deb http://us.archive.ubuntu.com/ubuntu/ hardy-updates main restricted universe multiverse
deb http://security.ubuntu.com/ubuntu hardy-security main restricted universe multiverse
EOF

cat > $ROOTS/lucid-amd64/etc/apt/sources.list << EOF
deb http://us.archive.ubuntu.com/ubuntu/ lucid main restricted universe multiverse
deb http://us.archive.ubuntu.com/ubuntu/ lucid-updates main restricted universe multiverse
deb http://security.ubuntu.com/ubuntu lucid-security main restricted universe multiverse
EOF
cat > $ROOTS/lucid-i386/etc/apt/sources.list << EOF
deb http://us.archive.ubuntu.com/ubuntu/ lucid main restricted universe multiverse
deb http://us.archive.ubuntu.com/ubuntu/ lucid-updates main restricted universe multiverse
deb http://security.ubuntu.com/ubuntu lucid-security main restricted universe multiverse
EOF

cd /tmp
[ -d "/tmp/ruby-build" ] || git clone https://github.com/sstephenson/ruby-build.git

RUBY="1.9.3-p194"
RUBY_INST="/opt/ruby-$RUBY"
RUBY_BUILD="/usr/local/bin/ruby-build"

for arch in i386 amd64
do
  for dist in hardy lucid precise
  do
    root="$ROOTS/$dist-$arch"
    chroot $root apt-get update
    chroot $root apt-get install --force-yes -y -o "DPkg::Options::=--force-confold" vim build-essential apt-utils gpgv libssl-dev zlib1g-dev curl git-core lsb-release

    rsync -a /tmp/ruby-build $root/tmp
    [ -x "$root/$RUBY_BUILD" ] || chroot $root bash -c "cd /tmp/ruby-build && bash install.sh"
    [ -x "$root/$RUBY_INST/bin/ruby" ] || chroot $root /usr/local/bin/ruby-build $RUBY $RUBY_INST

    for gem in rake bundler minitest httparty
    do
      chroot $root $RUBY_INST/bin/gem install --no-ri --no-rdoc $gem
    done

    chroot $root update-alternatives --install /usr/bin/ruby ruby $RUBY_INST/bin/ruby 400 \
      --slave /usr/bin/ri ri $RUBY_INST/bin/ri \
      --slave /usr/bin/irb irb $RUBY_INST/bin/irb \
      --slave /usr/bin/gem gem $RUBY_INST/bin/gem \
      --slave /usr/bin/rake rake $RUBY_INST/bin/rake \
      --slave /usr/bin/bundle bundle $RUBY_INST/bin/bundle
  done
done
#
