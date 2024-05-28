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

Acknowledgements
================

The initial version of this project was financially supported by the Federal Ministry of Economic Affairs and Climate Action of Germany within the program "Highly and Fully Automated Driving in Demanding Driving Situations" (project LUKAS, grant number 19A20004F).

Parts of the further developments have been made as part of the PoDIUM project and the EVENTS project, which both are funded by the European Union under grant agreement No. 101069547 and No. 101069614, respectively. Views and opinions expressed are however those of the authors only and do not necessarily reflect those of the European Union or European Commission. Neither the European Union nor the granting authority can be held responsible for them.

Parts of the further developments have been financially supported by the Federal Ministry of Education and Research (project AUTOtech.*agil*, FKZ 01IS22088W).
