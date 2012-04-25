#!/bin/bash
#
# This is a quick & dirty smoke test. It expects to run on a set of chroots under /snapfs
# like the other build scripts, and will run fine on the snapshots used by the build itself.
# It is destructive and will remove /opt/hastur and any hastur packages before testing the new
# package. It also kills processes on the host.
# That said, I did discover a few quirks in the packages using this and have fixed them
# in the other scripts / tasks already.
#

die () {
  echo "$*"
  exit 1
}

buildid=$1

[ -n "$buildid" ] || die "must specify a buildid (it's a unix timestamp, check out /snapfs/snapshots)"

ps -ef |grep -q '[h]astur-agent.rb'
[ $? -eq 0 ] && die "hastur-agent is running on the host. Kill it before trying this."

ps -ef |grep -q '[b]luepill'
[ $? -eq 0 ] && die "bluepill is running on the host. Kill it before trying this."

cd /snapfs/snapshots
for snap in *-$buildid
do
  root="/snapfs/snapshots/$snap"

  echo $root |grep -q -- -agent-
  [ $? -eq 0 ] || continue

  rm -rf $root/opt/hastur
  file=$(basename $(ls $root/tmp/hastur-server/*.deb))

  echo "#"
  echo "# Build ID: $buildid"
  echo "# Package: $file"
  echo "# Root: $root"
  echo "#"

  # when testing in a chroot, binary testing on Precise will often break due to missing
  # /lib/ld-linux-x86-64.so.2.  this is only a problem on 64-bit precise, which does not
  # appear to create the symlink by default, 32-bit does.
  if [ -x "$root/lib/x86_64-linux-gnu/ld-linux-x86-64.so.2" ] ; then
    chroot $root ln -nfs /lib/x86_64-linux-gnu/ld-linux-x86-64.so.2 /lib
  fi

  rm -f $root/run/hastur*.pid $root/var/run/hastur*.pid
  chroot $root bash -c "dpkg -r hastur-agent ; dpkg -r hastur-server" 2>&1 >/dev/null
  chroot $root dpkg -i /tmp/hastur-server/$file || die "package install of $file failed on snapshot $root"

  echo -n "Waiting up to 30 seconds for hastur-agent.rb to start up ..."
  ran_agent=0
  for i in $(seq 1 30)
  do
    ps -ef |grep -q '[h]astur-agent.rb'
    if [ $? -eq 0 ] ; then
      ran_agent=1
      break
    else
      echo -n "."
    fi
    sleep 1
  done
  echo

  if [ $ran_agent -eq 1 ] ; then
    echo "Successfully installed package in snapshot $snap."
  else
    die "package did not start hastur-agent.rb"
  fi

  chroot $root /etc/init.d/hastur-agent.init stop
  [ $? -eq 0 ] || die "could not stop hastur-agent.rb in $snap"
  chroot $root bash -c "dpkg -r hastur-agent ; dpkg -r hastur-server" 2>&1 >/dev/null

  # kill leaked bluepills
  for pid in $(ps -ef |awk '/[b]luepill/{print $2}')
  do
    proot=$(readlink /proc/$pid/root)
    if [ "$proot" == "$root" ] ; then
      kill $pid
    fi
  done
done
