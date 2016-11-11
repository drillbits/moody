#!/bin/sh
go run `ls cmd/cloud-app/*.go|grep _test.go -v` $@
