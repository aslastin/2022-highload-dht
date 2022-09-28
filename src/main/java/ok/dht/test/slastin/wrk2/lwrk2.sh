#!/usr/bin/env bash

LGREEN='\033[1;32m'
YELLOW='\033[1;33m'
LCYAN='\033[1;36m'
NC='\033[0m'

delay=$1
lua=$2
sleep=$3

rs=(10000 20000 30000 25000 27500 26000)

for r in "${rs[@]}"; do
  echo -e "${LCYAN}starting -R$r${NC}";
  ~/packages/wrk2/wrk -t1 -c1 -d"$delay" -R"$r" -s "$lua.lua" -L http://localhost:2022 > ../report/wrk2/"$lua-$r.txt"
  echo -e "${LGREEN}finished -R$r${NC}";
  echo -e "${YELLOW}sleeping for $sleep${NC}";
  sleep "$sleep";
  echo "";
done
