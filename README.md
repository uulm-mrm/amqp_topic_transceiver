# amqp_topic_transceiver

Contains two nodes:

 - AMQPTopicTransmitter: Subscribes to a ROS topic and sends the serialized message to an AMQP message queue.
 - AMQPTopicReceiver: Consumes messages pushed to an AMQP message queue and publishes them on a ROS topic.

In combination, these nodes can be used to transmit messages on a ROS topic via an AMQP message broker from one ROS master to another ROS master.

## Overview

### License

The source code is not officially released and is only for internal use.

Affiliation: Institute of Measurement, Control and Microtechnology, Ulm University.

### Aduulm Repository Metadata

- last updated: 01/2024
- name: AMQP Topic Transmitter
- category: tooling
- maintainers: Robin Dehler
- license: internal use only
- HW dependencies: none
