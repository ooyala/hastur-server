#!/bin/bash
rm config.sqlite
m2sh load --db config.sqlite -config query_server.conf

# Errors here?  You may have already chowned the directory to root.root.
# Sudo would fix this, as would re-chowning.

echo "$PWD/run/mongrel2.pid" > profiles/mongrel2/pid_file

# Set up Mongrel2 run script with absolute paths
echo "#!/bin/sh" > profiles/mongrel2/run
echo "cd $PWD" >> profiles/mongrel2/run
echo "m2sh start -db config.sqlite -host localhost" >> profiles/mongrel2/run

# Set up Worker run script with absolute paths
cat <<EOF >profiles/worker/run
#!/bin/sh
set -e

cd $PWD
# WARNING: on some systems the nohup doesn't work, like OSX
# so we're doing without
./worker.rb > profiles/worker.log &
echo $! > ./profiles/chat/chat.pid
EOF
