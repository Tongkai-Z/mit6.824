#!/bin/bash

for ((i=1;i<=1;i++));
do
go test -run 2B -race > ./2blog/log$i
done