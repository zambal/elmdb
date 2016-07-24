#!/usr/bin/env bash

cd `dirname $0`
exec erl -pa $PWD/ebin -pa +B -- $@
