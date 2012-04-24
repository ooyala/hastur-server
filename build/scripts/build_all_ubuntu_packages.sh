#!/bin/bash

set -x
set -e

# this script assumes:
#  * running on Linux
#  * can sudo to root
#  * ubuntu roots are available on btrfs subvolumes
#  * can snapshot / destroy snapshots at will

# setup (what I ran on spaceghost.mtv):
# lvcreate -L 20G -n lv_snapfs vgsg
# mkfs.btrfs /dev/vgsg/lv_snapfs
# mkdir /snapfs
# mount /dev/vgsg/lv_snapfs /snapfs
# btrfs subvolume create /snapfs/snapshots
# btrfs subvolume create /snapfs/roots
# for arch in i386 amd64
# do
#   for dist in hardy lucid precise
#   do
#     btrfs subvolume create /snapfs/roots/$dist-$arch
#   done
# done
# chmod -R 755 /snapfs
#
# Then I copied the debootstrapped roots into snapfs. I could have debootstrapped directly in, but I
# was doing both in parallel.
# Just for giggles, I snapshot each raw image before configuring it, e.g.
# btrfs subvolume snapshot /snapfs/roots/lucid-amd64 /snapfs/snapshots/lucid-amd64-debootstrap-2012-04-23
#
# See configure_chroots.sh to take the debootstraps to the point where they can be snapshotted for builds.
#

ROOT_ROOT=/snapfs/roots
SNAP_ROOT=/snapfs/snapshots
SNAP_SUFFIX=$(date +%s)
HASTUR_ROOT=/opt/hastur
GIT_REPO="ssh://git@git.corp.ooyala.com/hastur-server"

# I manually pre-cached the source tarballs into the roots in /root/Downloads/*.tar.gz
# The Rake task checks that directory for cached files.
export HOME=/root

die () {
  echo "$*"
  exit 1
}

build_script () {
  location=$1
  build=$2
}

setup_schroot () {
  dist=$1
  arch=$2
  snap=$3
  path=$4

  SCHROOT="hastur-$dist-$arch-$snap"

  if [ "$arch" == "i386" ] ; then
    personality="linux32"
  else
    personality="linux64"
  fi

  cat > /etc/schroot/chroot.d/$SCHROOT.conf <<EOF
[$SCHROOT]
description=Hastur Build: $SCHROOT
type=directory
directory=$path
root-groups=root
script-config=/dev/null
personality=$personality
EOF
}

build_hastur () {
  target=$1

  cd /tmp
  git clone $GIT_REPO

  # bundle / rake are placed in the path with update-alternatives in the root setup
  schroot -c $SCHROOT bash -c "cd /tmp/hastur-server && bundle install"
  schroot -c $SCHROOT bash -c "cd /tmp/hastur-server && bundle exec rake $target"
}

[ -x /usr/bin/schroot ] || die "schroot must be installed on the host system"

for arch in i386 amd64
do
  for dist in hardy lucid precise
  do
    root="$ROOT_ROOT/$dist-$arch"

    for pkg in agent server
    do
      snapshot="$SNAP_ROOT/$dist-$arch-$pkg-$SNAP_SUFFIX"
      btrfs subvolume snapshot $root $snapshot || die "Could not create snapshot."

      setup_schroot $dist $arch $SNAP_SUFFIX $snapshot

      build_hastur "hastur:fpm_hastur_$pkg"
      # TODO: cleanup
    done
  done
done
