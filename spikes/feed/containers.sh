#!/usr/bin/env bash
# Containers of this spike. Names and ports are unique to it. Throwaway local passwords only.
set -e
case "$1" in
main)
  docker run -d --name hoodie-s3-mongo --cpus 2 --memory 3g -p 27213:27213 mongo:8.0 mongod --replSet rs0 --bind_ip_all --port 27213 --wiredTigerCacheSizeGB 1
  docker run -d --name hoodie-s3-redis --cpus 1 --memory 512m -p 27214:6379 redis:7.0-alpine
  sleep 5
  docker exec hoodie-s3-mongo mongosh --port 27213 --quiet --eval 'rs.initiate({_id:"rs0",members:[{_id:0,host:"127.0.0.1:27213"}]})' ;;
solo)
  docker run -d --name hoodie-s3-solo --cpus 1 --memory 1g -p 27215:27215 mongo:8.0 mongod --bind_ip_all --port 27215 ;;
auth)
  docker run -d --name hoodie-s3-auth --cpus 1 --memory 1g -p 27216:27216 --entrypoint bash mongo:8.0 -c \
   "openssl rand -base64 48 > /tmp/k && chmod 400 /tmp/k && chown mongodb:mongodb /tmp/k && exec gosu mongodb mongod --replSet rs0 --keyFile /tmp/k --bind_ip_all --port 27216"
  sleep 6
  docker exec hoodie-s3-auth mongosh --port 27216 --quiet --eval 'rs.initiate({_id:"rs0",members:[{_id:0,host:"127.0.0.1:27216"}]})'
  sleep 6
  docker exec hoodie-s3-auth mongosh --port 27216 --quiet --eval 'db.getSiblingDB("admin").createUser({user:"root",pwd:"rootpw",roles:["root"]})'
  docker exec hoodie-s3-auth mongosh --port 27216 --quiet -u root -p rootpw --authenticationDatabase admin --eval '
   const a=db.getSiblingDB("admin");
   a.createRole({role:"readOnlyScf",privileges:[{resource:{db:"scf",collection:""},actions:["find"]}],roles:[]});
   a.createRole({role:"csColl",privileges:[{resource:{db:"scf",collection:"e0"},actions:["find","changeStream"]}],roles:[]});
   a.createRole({role:"csDb",privileges:[{resource:{db:"scf",collection:""},actions:["find","changeStream"]}],roles:[]});
   a.createUser({user:"u_find",pwd:"pw",roles:["readOnlyScf"]});
   a.createUser({user:"u_cscoll",pwd:"pw",roles:["csColl"]});
   a.createUser({user:"u_csdb",pwd:"pw",roles:["csDb"]});
   db.getSiblingDB("scf").e0.insertOne({x:1}); db.getSiblingDB("scf").e1.insertOne({x:1});' ;;
rm)
  docker rm -f hoodie-s3-mongo hoodie-s3-redis hoodie-s3-solo hoodie-s3-auth 2>/dev/null || true ;;
esac
