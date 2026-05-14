#!/bin/sh

cd bin
java  -cp .:../common.jar broker.PubSubImpl $*
