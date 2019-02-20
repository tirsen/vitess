#/bin/sh
find vendor -mindepth 1 -maxdepth 1 -type d | xargs rm -rf
cd vendor
govendor sync