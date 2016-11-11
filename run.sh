#!/bin/sh
go run `ls cmd/moody-example/*.go|grep _test.go -v` $@
