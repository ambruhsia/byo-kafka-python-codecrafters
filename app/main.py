import socket


class KafkaHandler:
    # Define supported API versions and max versions for each API key
    SUPPORTED_API_VERSIONS = {0, 1, 2, 3, 4}  # Using a set for faster lookups
    API_VERSIONS = {18: (4, 4)}  # API Key 18 with MaxVersion 4

    @staticmethod
    def read_int16(data, offset):
        return int.from_bytes(data[offset:offset + 2], byteorder='big', signed=True)

    @staticmethod
    def read_int32(data, offset):
        return int.from_bytes(data[offset:offset + 4], byteorder='big', signed=True)

    @staticmethod
    def get_response(correlation_id, error_code=0, api_key=None):
        if error_code != 0:
            # Response for error
            message_size = (4 + 4 + 2).to_bytes(4, byteorder='big')
            correlation_id_bytes = correlation_id.to_bytes(4, byteorder='big')
            error_code_bytes = error_code.to_bytes(2, byteorder='big')
            return message_size + correlation_id_bytes + error_code_bytes
        
        # Response for valid requests
        message_size = (4 + 4 + 2 + 6).to_bytes(4, byteorder='big')  # Add space for API Key and MaxVersion
        correlation_id_bytes = correlation_id.to_bytes(4, byteorder='big')
        error_code_bytes = (0).to_bytes(2, byteorder='big')

        response = message_size + correlation_id_bytes + error_code_bytes

        # Add API Key 18 information if applicable
        if api_key == 18:
            max_version = KafkaHandler.API_VERSIONS[18][0]
            response += (18).to_bytes(2, byteorder='big') + (1).to_bytes(2, byteorder='big') + (max_version).to_bytes(2, byteorder='big')

        return response

def main():
    print("Logs from your program will appear here!")
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    print("Server is listening on port 9092...")

    while True:
        client, addr = server.accept()
        print(f"Connection from {addr} accepted.")
        
        data = client.recv(1024)
        if data:
            print(f"Received data: {data}")

            correlation_id = KafkaHandler.read_int32(data, 8)
            request_api_key = KafkaHandler.read_int16(data, 0)
            request_api_version = KafkaHandler.read_int16(data, 2)

            if request_api_version not in KafkaHandler.SUPPORTED_API_VERSIONS:
                error_code = 35  # INVALID_API_VERSION
                print(f"Invalid API version: {request_api_version}. Sending error code {error_code}.")
            else:
                error_code = 0  # No error

                # Check for API Key 18 requirements
                if request_api_key == 18:
                    max_version = KafkaHandler.API_VERSIONS[18][0]
                    if max_version < 4:
                        error_code = 1  # Version not supported

            response = KafkaHandler.get_response(correlation_id, error_code, request_api_key)
            client.sendall(response)
            print(f"Sent response: {response}")
        
        client.close()
        print(f"Connection from {addr} closed.")

if __name__ == "__main__":
    main()
