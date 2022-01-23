#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: ashish
"""

from kafka import KafkaConsumer

bootstrap_servers = ['localhost:9092']
topicName = 'Media'
consumer = KafkaConsumer (topicName,bootstrap_servers = bootstrap_servers)



for message in consumer:
    print(message)