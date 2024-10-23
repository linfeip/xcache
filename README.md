server1, server2

server2 join to server1

server2 创建一个client2 dial server1, 发送JOIN协议, 等待ACK

server1 接收到client2的连接, 创建client1 向 server2 发起连接