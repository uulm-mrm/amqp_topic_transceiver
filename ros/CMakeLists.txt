cmake_minimum_required(VERSION 3.5.1)
project(amqp_topic_transceiver)

find_package(aduulm_cmake_tools REQUIRED)
get_version_string_from_package_xml(.)
show_build_info("ros")

find_package(PkgConfig REQUIRED)
pkg_check_modules(RABBITMQ_C REQUIRED librabbitmq)

find_aduulm_package(aduulm_logger_lib)

find_package(catkin REQUIRED COMPONENTS
  roscpp
  std_msgs
  dynamic_reconfigure
  topic_tools
)

generate_dynamic_reconfigure_options(
  config/AMQPTopicTransmitter_config.cfg
  config/AMQPTopicReceiver_config.cfg
)

catkin_package(
  CATKIN_DEPENDS
    roscpp
    message_runtime
  CFG_EXTRAS
    proj-extras.cmake
)

add_executable(AMQPTopicTransmitter_node
  src/AMQPTopicTransmitter.cpp
  src/AMQPTopicTransmitter_node.cpp
  src/utils.cpp
)
add_executable(AMQPTopicReceiver_node
  src/AMQPTopicReceiver.cpp
  src/AMQPTopicReceiver_node.cpp
  src/utils.cpp
)

foreach(target_node AMQPTopicTransmitter_node AMQPTopicReceiver_node)
  target_include_directories(${target_node}
    PUBLIC
      include
      ${catkin_INCLUDE_DIRS}
      ${RABBITMQ_C_INCLUDE_DIRS}
  )

  add_dependencies(${target_node} ${${PROJECT_NAME}_EXPORTED_TARGETS})
  target_compile_features(${target_node} PRIVATE cxx_std_17)
  target_compile_definitions(${target_node} PRIVATE "-D${package_name}_VERSION=\"${package_version}\"")
  target_compile_options(${target_node} PRIVATE $<$<CXX_COMPILER_ID:GNU>:-Wall>)


  ## Specify libraries to link a library or executable target against
  target_link_libraries(${target_node}
    PUBLIC
      ${catkin_LIBRARIES}
      ${RABBITMQ_C_LIBRARIES}
  )

  # Link aduulm targets
  link_aduulm_package_targets(TARGET ${target_node}
    ACCESS PUBLIC
    PACKAGE_TARGETS
      aduulm_logger_lib::aduulm_logger_lib
  )

  ## Mark executables and/or libraries for installation
  install(TARGETS ${target_node}
    ARCHIVE DESTINATION ${CATKIN_PACKAGE_LIB_DESTINATION}
    LIBRARY DESTINATION ${CATKIN_PACKAGE_LIB_DESTINATION}
    RUNTIME DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
  )
endforeach()

setup_git_hooks()
