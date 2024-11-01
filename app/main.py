import socket

class KafkaHandler:
    def __init__(self, correlation_id: int = 7):
        self.correlation_id = correlation_id

    def get_correlation_id_bytes(self):
        correlation_id_bytes = self.correlation_id.to_bytes(4, byteorder="big")
        return correlation_id_bytes

    def get_headers(self):
        correlation_id = self.get_correlation_id_bytes()
        header = correlation_id
        return header

    @staticmethod
    def get_message_size():
        message_size = int(4).to_bytes(4, byteorder="big")
        return message_size

    def get_response(self):
        message_size = self.get_message_size()
        correlation_id = self.get_correlation_id_bytes()
        response = message_size + correlation_id
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
            response = KafkaHandler(correlation_id=7).get_response()
            client.sendall(response)
            print(f"Sent response: {response}")
        
        client.close()
        print(f"Connection from {addr} closed.")

if __name__ == "__main__":
    main()
