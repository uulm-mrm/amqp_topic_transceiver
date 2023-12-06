from launch import LaunchDescription
from launch_ros.actions import Node

def generate_launch_description():
    return LaunchDescription([
        Node(
            package='amqp_topic_transceiver',
            executable='AMQPTopicTransceiver',
            name='transmitter',
            parameters=[{
                "server_url": "127.0.0.1:5672",
                "server_user": "anonymous",
                "server_password": "",
                "exchange": "detections",
                "log_level": 4,
                "topics_to_transmit": ['/test_topic']
            }],
            respawn=True,
            respawn_delay=5,
            # arguments=['--ros-args', '--log-level', "DEBUG"]
        ),
        Node(
            package='amqp_topic_transceiver',
            executable='AMQPTopicTransceiver',
            name='receiver',
            parameters=[{
                "server_url": "127.0.0.1:5672",
                "server_user": "anonymous",
                "exchange": "detections",
                "server_password": "",
                "log_level": 4,
                "topic_suffix": "__amqp"
            }],
            respawn=True,
            respawn_delay=5,
            # arguments=['--ros-args', '--log-level', "DEBUG"]
        ),
    ])
