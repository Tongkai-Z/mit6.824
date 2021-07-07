#!/bin/bash
rm ./2blog/*
for ((i=1;i<=10;i++));
do
go test -run 2B -race > ./2blog/log$i
done