#!/bin/bash
git commit -a -m "next"; docker build . -t pdedkov/hlcup && docker tag pdedkov/hlcup stor.highloadcup.ru/travels/sloth_climber && docker push stor.highloadcup.ru/travels/sloth_climber

