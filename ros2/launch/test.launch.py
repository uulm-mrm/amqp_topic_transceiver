from launch import LaunchDescription
from launch_ros.actions import Node
from ament_index_python import get_package_share_directory
import os

def generate_launch_description():
    return LaunchDescription([
        Node(
            package='amqp_topic_transceiver',
            executable='AMQPTopicTransmitter_node',
            name='transmitter',
            parameters=[{
                "server_url": "127.0.0.1",
                "server_port": 5001,
                "server_user": "mrm",
                "server_password": "mrm",
                "log_level": 4,
                "topics": ['/test_topic']
            }],
            respawn=True,
            respawn_delay=5,
            # arguments=['--ros-args', '--log-level', "DEBUG"]
        ),
        Node(
            package='amqp_topic_transceiver',
            executable='AMQPTopicReceiver_node',
            name='receiver',
            parameters=[{
                "server_url": "127.0.0.1",
                "server_port": 5001,
                "server_user": "mrm",
                "server_password": "mrm",
                "log_level": 4,
                "topic_suffix": "__amqp"
            }],
            respawn=True,
            respawn_delay=5,
            # arguments=['--ros-args', '--log-level', "DEBUG"]
        ),
    ])
