#!/bin/bash

set -x

# this script assumes:
#  * running on Linux
#  * can sudo to root
#  * ubuntu roots are available on btrfs subvolumes
#  * can snapshot / destroy snapshots at will
#  * you probably want 'Defaults env_keep += "SSH_AUTH_SOCK"' in your sudoers

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

BRANCH_OR_TAG="master" ; [ -n "$1" ] && BRANCH_OR_TAG="$1"
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

build_hastur () {
  target=$1
  path=$2

  cd $path/tmp
  if [ ! -d "$path/tmp/hastur-server" ] ; then
    git clone $GIT_REPO
  else
    cd "$path/tmp/hastur-server"
    git pull
  fi

  # switch to the desired branch/tag
  cd "$path/tmp/hastur-server"
  git checkout -f $BRANCH_OR_TAG

  if [ "$arch" == "i386" ] ; then
    personality=$(which linux32)
    [ -n "$personality" ] || die "could not find 'linux32' utility"
  else
    # don't worry about it if it's not there, don't really need it on 64-bit systems
    personality=$(which linux64)
  fi

  # bundle / rake are placed in the path with update-alternatives in the root setup
  # see configure_chroots.sh
  $personality chroot $path bash -c "cd /tmp/hastur-server && bundle install" # could fail, don't care
  $personality chroot $path bash -c "cd /tmp/hastur-server && rake --trace $target"
  require $personality chroot $path bash -c "cd /tmp/hastur-server && rake --trace $target"
}

for arch in amd64 i386
do
  for dist in lucid precise lucid
  do
    root="$ROOT_ROOT/$dist-$arch"

    for pkg in agent server
    do
      # only build hastur-server for precise
      if [ "$pkg" == "server" -a "$dist" != "precise" ] ; then
        continue
      fi

      snapshot="$SNAP_ROOT/$dist-$arch-$pkg-$SNAP_SUFFIX"
      btrfs subvolume snapshot $root $snapshot
      require btrfs subvolume snapshot $root $snapshot

      build_hastur "hastur:fpm_hastur_$pkg" $snapshot
      # TODO: cleanup
    done
  done
done
