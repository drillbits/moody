#!/bin/sh
go run `ls cmd/local-app/*.go|grep _test.go -v` $@
