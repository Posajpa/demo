import socket

# Confluent Cloud credentials
conf = {'bootstrap.servers': 'pkc-57rr02.italynorth.azure.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': '6E6UXX22DIQLMPGL',
        'sasl.password': 'u6QGaZ0UR88hjHHQosWJoHrb5qxYTF3u6Wz6RJOL+I8BSbt4yvpAErmjvAeO98Z5',
        'client.id': socket.gethostname()
  }