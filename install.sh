#!/bin/bash
echo 'Building bender-worker and its dependencies'
echo
cargo build --release

echo

read -e -p "
Copy the compiled binary from target/release/bender-worker to /usr/local/bin/bender-worker? [Y/n] " YN

[[ $YN == "y" || $YN == "Y" || $YN == "" ]] && sudo cp target/release/bender-worker /usr/local/bin/bender-worker


read -e -p "Copy the service file bender-worker.service to /etc/systemd/system? [Y/n] " YN

[[ $YN == "y" || $YN == "Y" || $YN == "" ]] && sudo cp bender-worker.service /etc/systemd/system && echo "You can run the service via \"service bender-worker start\""
