import socket

class KafkaHandler:
    # Define supported API versions for ApiVersions
    SUPPORTED_API_VERSIONS = {0, 1, 2, 3, 4}  # Using a set for faster lookups

    @staticmethod
    def read_int16(data, offset):
        """Read a 16-bit integer from data starting at offset."""
        return int.from_bytes(data[offset:offset + 2], byteorder='big', signed=True)

    @staticmethod
    def read_int32(data, offset):
        """Read a 32-bit integer from data starting at offset."""
        return int.from_bytes(data[offset:offset + 4], byteorder='big', signed=True)

    @staticmethod
    def get_response(correlation_id, error_code=0):
        # Create the response
        message_size = (4 + 4 + 2).to_bytes(4, byteorder='big')  # 4 bytes for message_size + 4 bytes for correlation_id + 2 bytes for error_code
        correlation_id_bytes = correlation_id.to_bytes(4, byteorder='big')
        error_code_bytes = error_code.to_bytes(2, byteorder='big')

        response = message_size + correlation_id_bytes + error_code_bytes
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

            # Read the request fields
            correlation_id = KafkaHandler.read_int32(data, 8)  # Offset for correlation_id
            request_api_key = KafkaHandler.read_int16(data, 0)  # Offset for request_api_key
            request_api_version = KafkaHandler.read_int16(data, 2)  # Offset for request_api_version

            # Validate the API version
            if request_api_version not in KafkaHandler.SUPPORTED_API_VERSIONS:
                error_code = 35  # INVALID_API_VERSION
                print(f"Invalid API version: {request_api_version}. Sending error code {error_code}.")
            else:
                error_code = 0  # No error

            # Create response using the extracted correlation_id and error_code
            response = KafkaHandler.get_response(correlation_id, error_code)
            client.sendall(response)
            print(f"Sent response: {response}")
        
        client.close()
        print(f"Connection from {addr} closed.")

if __name__ == "__main__":
    main()
