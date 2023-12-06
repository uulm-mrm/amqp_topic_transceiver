cmake_minimum_required(VERSION 3.10)
project(amqp_topic_transceiver)

# find dependencies
find_package(aduulm_cmake_tools REQUIRED)

aduulm_init_ros2_package()
find_package(PkgConfig REQUIRED)
find_package(ProtonCpp REQUIRED)

pkg_check_modules(BLOSC REQUIRED blosc)

aduulm_add_ros2_executable(AMQPTopicTransceiver
  src/AMQPTopicTransceiver.cpp
  src/AMQPTopicTransceiver_node.cpp
)

foreach(target_node AMQPTopicTransceiver)
  target_include_directories(${target_node}
    SYSTEM
    PUBLIC
      ${BLOSC_INCLUDE_DIRS}
  )
  target_compile_features(${target_node} PRIVATE cxx_std_20)

  ## Specify libraries to link a library or executable target against
  target_link_libraries(${target_node}
    PUBLIC
    Proton::cpp
    -L/usr/local/lib
    ${BLOSC_LDFLAGS}
    v2x_amqp_connector_lib::v2x_amqp_connector_lib
    uuid
  )
endforeach()

# install standard ROS launch files
install(DIRECTORY
        launch
        DESTINATION share/${PROJECT_NAME}
)

# install launch files for aduulm_launch
aduulm_install_ros2_python_code("${PROJECT_NAME}_launch" PACKAGE_DIR ${CMAKE_CURRENT_LIST_DIR}/launch)

# Finish the package creation and generate install() commands
aduulm_create_ros2_package()
setup_git_hooks()
