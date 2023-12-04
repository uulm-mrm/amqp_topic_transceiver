cmake_minimum_required(VERSION 3.10)
project(amqp_topic_transceiver)

# find dependencies
find_package(ament_cmake_auto REQUIRED)
find_package(PkgConfig REQUIRED)
find_package(ProtonCpp REQUIRED)
find_package(v2x_amqp_connector_lib REQUIRED)
pkg_check_modules(BLOSC REQUIRED blosc)
ament_auto_find_build_dependencies()

ament_auto_add_executable(AMQPTopicTransceiver
  src/AMQPTopicTransceiver.cpp
  src/AMQPTopicTransceiver_node.cpp
)

foreach(target_node AMQPTopicTransceiver)
  target_include_directories(${target_node}
    SYSTEM
    PUBLIC
      ${BLOSC_INCLUDE_DIRS}
  )
  target_compile_features(${target_node} PRIVATE cxx_std_17)

  ## Specify libraries to link a library or executable target against
  target_link_libraries(${target_node}
    # PUBLIC
    Proton::cpp
    -L/usr/local/lib
    ${BLOSC_LDFLAGS}
    v2x_amqp_connector_lib::v2x_amqp_connector_lib
    uuid
  )
endforeach()

install(DIRECTORY
        launch
        DESTINATION share/${PROJECT_NAME}
)

# finish package creation
ament_auto_package()
