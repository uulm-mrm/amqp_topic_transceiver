from dataclasses import dataclass, field
from typing import List
from aduulm_launch_lib_py import LaunchConfig, LogLevel

@dataclass(slots=True)
class AMQPTransceiverParameters:
    host_url: str
    exchange: str
    topics_to_transmit: List[str]
    topics_to_receive: List[str] = field(default_factory=list)
    user: str = 'anonymous'
    password: str = ''
    topic_suffix: str = ''
    name: str = 'amqp'
    use_compression: bool = True
    compression_algorithm: str = 'zstd'
    compression_level: int = 5
    log_level: LogLevel = LogLevel.Warning

def gen_config(config: LaunchConfig, params: AMQPTransceiverParameters):
    config.insert_overrides(params)
    def fix(lst):
        return lst if len(lst) > 0 else [''] # return a list with a dummy value to fix a ROS launch quirk
    node = config.add_node(
        name=params.name,
        package='amqp_topic_transceiver',
        executable='AMQPTopicTransceiver',
        handle_lifecycle=True,
        parameters={
            "server_url": params.host_url,
            "server_user": params.user,
            "server_password": params.password,
            "topic_suffix": params.topic_suffix,
            "exchange": params.exchange,
            "log_level": params.log_level,
            "topics_to_transmit": fix(params.topics_to_transmit),
            "topics_to_receive": fix(params.topics_to_receive),
        },
    )
    for topic in params.topics_to_transmit:
        config.add_subscriber(node, name=topic, topic=topic)
    for topic in params.topics_to_receive:
        config.add_publisher(node, name=topic, topic=topic + params.topic_suffix)
