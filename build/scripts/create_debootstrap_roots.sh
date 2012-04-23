#!/bin/bash
#
# I ran this on my Arch machine inside a Lucid chroot to build the chroots.
# That silly configuration aside, this should work fine on a modern Ubuntu machine,
# preferably a Precise machine since it will have all the definitions required
# in debootstrap scripts.
#
# In theory, this should only need to be used once, at which point I capture the images
# in a snapshot and/or tarball. Every build of the package will happen in a fresh chroot
# that is destroyed after extracting the deb.
#

apt-get install -y debootstrap

mkdir /srv

for distro in hardy lucid precise
do
  for arch in amd64 i386
  do
    debootstrap --variant=buildd --arch $arch $distro /srv/$distro-$arch http://archive.ubuntu.com/ubuntu/ >/srv/$distro-$arch.log 2>&1 &
  done
done

