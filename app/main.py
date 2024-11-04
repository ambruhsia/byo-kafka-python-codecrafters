import socket  # noqa: F401
import threading
def response_api_key_75(correlation_id, cursor, array_length, topic_name):
    tag_buffer = b"\x00"  # Placeholder for tags
    response_header = correlation_id.to_bytes(4, byteorder="big")
    error_code = int(3).to_bytes(2, byteorder="big")  # UNKNOWN_TOPIC_OR_PARTITION error code
    throttle_time_ms = int(0).to_bytes(4, byteorder="big")
    
    # Set topic_name as a COMPACT_STRING, which includes its length as the first byte
    topic_name_length = len(topic_name).to_bytes(1, byteorder="big")  # Length of the topic name
    topic_id = int(0).to_bytes(16, byteorder="big")  # Topic ID as 00000000-0000-0000-0000-000000000000
    partition_count = int(0).to_bytes(1, byteorder="big")  # No partitions for unknown topic

    # Construct the response body
    response_body = (
        throttle_time_ms +
        error_code +
        topic_name_length +  # Length of the topic name
        topic_name +         # The topic name itself
        topic_id +          # Topic ID
        partition_count +    # Number of partitions
        cursor +             # Next cursor (if applicable)
        tag_buffer           # Ending tag buffer
    )

    total_len = len(response_header) + len(response_body)
    return int(total_len).to_bytes(4, byteorder="big") + response_header + response_body

    total_len = len(response_header) + len(response_body)
    return int(total_len).to_bytes(4, byteorder="big") + response_header + response_body
def create_msg(id, api_key: int, error_code: int = 0):
    response_header = id.to_bytes(4, byteorder="big")
    err = error_code.to_bytes(2, byteorder="big")
    api_key_bytes = api_key.to_bytes(2, byteorder="big")
    min_version_api_18, max_version_api_18, min_version_api_75, max_version_api_75 = (
        0,
        4,
        0,
        0,
    )
    tag_buffer = b"\x00"
    num_api_keys = int(3).to_bytes(1, byteorder="big")
    describle_topic_partition_api = int(75).to_bytes(2, byteorder="big")
    throttle_time_ms = 0
    response_body = (
        err
        + num_api_keys
        + api_key_bytes
        + min_version_api_18.to_bytes(2)
        + max_version_api_18.to_bytes(2)
        + tag_buffer
        + describle_topic_partition_api
        + min_version_api_75.to_bytes(2)
        + max_version_api_75.to_bytes(2)
        + tag_buffer
        + throttle_time_ms.to_bytes(4, byteorder="big")
        + tag_buffer
    )
    total_len = len(response_header) + len(response_body)
    return int(total_len).to_bytes(4, byteorder="big") + response_header + response_body
def handle(client):
    try:
        while True:
            req = client.recv(1024)
            if not req:
                break
            asdf = int.from_bytes(req, byteorder="big")
            print(asdf)
            api_key = int.from_bytes(req[4:6], byteorder="big")
            print(api_key)
            api_version = int.from_bytes(req[6:8], byteorder="big")
            print(api_version)
            version = {0, 1, 2, 3, 4}
            error_code = 0 if api_version in version else 35
            Coreleation_ID = int.from_bytes(req[8:12], byteorder="big")
            response = create_msg(Coreleation_ID, api_key, error_code)
            client.sendall(response)
            if api_key == 75:
                client_id_len = int.from_bytes(req[12:14])
                print(client_id_len)
                if client_id_len > 0:
                    cliend_id = req[14 : 14 + client_id_len]
                    tagged = req[14 + client_id_len]
                else:
                    cliend_id = ""
                    tagged = [14]
                array_len_finder = 14 + client_id_len + 1
                array_length = req[array_len_finder]
                topic_name_length = req[array_len_finder + 1]
                topic_name_starter = array_len_finder + 2
                topic_name = bytes(
                    req[
                        topic_name_starter : topic_name_starter
                        + (topic_name_length - 1)
                    ]
                )
                cursor_length = topic_name_starter + topic_name_length + 4
                cursor = req[cursor_length]
                cursor_bytes = int(cursor).to_bytes(1, byteorder="big")
                response = response_api_key_75(
                    Coreleation_ID,
                    cursor_bytes,
                    array_length,
                    topic_name_length,
                    topic_name,
                )
                client.sendall(response)
            else:
                version = {0, 1, 2, 3, 4}
                error_code = 0 if api_version in version else 35
                response = create_msg(Coreleation_ID, api_key, error_code)
                client.sendall(response)
    except Exception as e:
        print(f"Except Error Handling Client: {e}")
    finally:
        client.close()
def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")
    # Uncomment this to pass the first stage
    #
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    while True:
        client, addr = server.accept()
        print(f"Accepted Connection from {addr}")
        client_Thread = threading.Thread(target=handle, args=(client,))
        client_Thread.start()
if __name__ == "__main__":
    main()