#!/bin/bash
set -e
HOSTNAME=apt.us-east-1.ooyala.com
reprepro -Vb /var/www/$HOSTNAME/hastur includedeb hardy $*