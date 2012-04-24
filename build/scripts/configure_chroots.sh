#!/bin/bash
set -x

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

for arch in i386 amd64
do
  for dist in hardy lucid precise
  do
    root="$ROOTS/$dist-$arch"
    chroot $root apt-get update
    chroot $root apt-get install -y vim build-essential apt-utils gpgv libssl-dev zlib1g-dev curl
    rsync -a /tmp/ruby-build $root/tmp
    chroot $root bash -c "cd /tmp/ruby-build && bash install.sh"
    chroot $root /usr/local/bin/ruby-build 1.9.3-p194 /opt/ruby-1.9.3-p194

  #  chroot $root update-alternatives --install /usr/bin/ruby ruby $RUBY_INST/bin/ruby 400 \
  #    --slave /usr/bin/ri ri $RUBY_INST/bin/ri \
  #    --slave /usr/bin/irb irb $RUBY_INST/bin/irb \
  #    --slave /usr/bin/gem gem $RUBY_INST/bin/gem \
  #    --slave /usr/lib/ruby/gems/bin gem-bin $RUBY_INST/bin
#
#    chroot $root update-alternatives --config ruby
  done
done
#
