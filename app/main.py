import socket

class KafkaHandler:
    def __init__(self):
        pass

    @staticmethod
    def read_int32(data, offset):
        """Read a 32-bit integer from data starting at offset."""
        return int.from_bytes(data[offset:offset + 4], byteorder='big', signed=True)

    @staticmethod
    def get_response(correlation_id):
        # Create the response with the received correlation ID
        message_size = (4 + 4).to_bytes(4, byteorder='big')  # 4 bytes for message_size + 4 bytes for correlation_id
        correlation_id_bytes = correlation_id.to_bytes(4, byteorder='big')
        response = message_size + correlation_id_bytes
        return response

def main():
    print("Logs from your program will appear here!")

    # Create the server
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    print("Server is listening on port 9092...")

    while True:
        client, addr = server.accept()  # wait for client
        print(f"Connection from {addr} accepted.")
        
        data = client.recv(1024)  # Receive data from the client
        if data:
            print(f"Received data: {data}")
            
            # Extract the correlation_id from the request
            correlation_id = KafkaHandler.read_int32(data, 8)  # Offset for correlation_id is 8 bytes (4 bytes each for message_size and request_api_key/api_version)

            # Create response using the extracted correlation_id
            response = KafkaHandler.get_response(correlation_id)
            client.sendall(response)
            print(f"Sent response: {response}")
        
        client.close()
        print(f"Connection from {addr} closed.")

if __name__ == "__main__":
    main()
