cmake_minimum_required(VERSION 3.10)
project(amqp_topic_transceiver)

# find dependencies
find_package(ament_cmake_auto REQUIRED)
find_package(PkgConfig REQUIRED)
find_package(ProtonCpp REQUIRED)
pkg_check_modules(BLOSC REQUIRED blosc)
ament_auto_find_build_dependencies()

ament_auto_add_executable(AMQPTopicTransmitter_node
  src/AMQPTopicTransmitter.cpp
  src/AMQPTopicTransmitter_node.cpp
)
ament_auto_add_executable(AMQPTopicReceiver_node
  src/AMQPTopicReceiver.cpp
  src/AMQPTopicReceiver_node.cpp
)

foreach(target_node AMQPTopicTransmitter_node AMQPTopicReceiver_node)
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
  )
endforeach()

install(DIRECTORY
        launch
        DESTINATION share/${PROJECT_NAME}
)

# finish package creation
ament_auto_package()
