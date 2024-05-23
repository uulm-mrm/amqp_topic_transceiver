amqp_topic_transceiver
======================

Contains a node called AMQPTopicTransceiver, which can
 - subscribe to ROS topics and send the serialized messages to an AMQP message queue
 - consume messages from an AMQP message queue and publish them on ROS topics

It can be used to transmit messages on ROS topics via an AMQP message broker from one ROS2 system to another ROS2 system.

License
=======

License: Apache 2.0

Affiliation: Institute of Measurement, Control and Microtechnology, Ulm University.

### Aduulm Repository Metadata

- last updated: 01/2024
- name: AMQP Topic Transmitter
- category: tooling
- maintainers: Robin Dehler
- license: internal use only
- HW dependencies: none

Dependencies
============

* [CMake](https://cmake.org/)
* ROS2
* [aduulm_cmake_tools](https://github.com/uulm-mrm/aduulm_cmake_tools)
* [aduulm_logger](https://github.com/uulm-mrm/aduulm_logger)
* [v2x_amqp_connector_lib](https://github.com/uulm-mrm/v2x_etsi_asn1)
