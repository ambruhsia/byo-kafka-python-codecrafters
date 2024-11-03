# import socket
# from dataclasses import dataclass
# from enum import Enum, unique


# @unique
# class ErrorCode(Enum):
#     NONE = 0
#     UNSUPPORTED_VERSION = 35


# @dataclass
# class KafkaRequest:
#     api_key: int
#     api_version: int
#     correlation_id: int

#     @staticmethod
#     def from_client(client: socket.socket):
#         data = client.recv(2048)
#         return KafkaRequest(
#             api_key=int.from_bytes(data[4:6]),
#             api_version=int.from_bytes(data[6:8]),
#             correlation_id=int.from_bytes(data[8:12]),
#         )


# def make_response(request: KafkaRequest):
#     response_header = request.correlation_id.to_bytes(4)

#     valid_api_versions = [0, 1, 2, 3, 4]
#     error_code = (
#         ErrorCode.NONE
#         if request.api_version in valid_api_versions
#         else ErrorCode.UNSUPPORTED_VERSION
#     )
#     min_version, max_version = 0, 4
#     throttle_time_ms = 0
#     tag_buffer = b"\x00"
#     response_body = (
#         error_code.value.to_bytes(2) +
#         int(2).to_bytes(1) +
#         request.api_key.to_bytes(2) +
#         min_version.to_bytes(2) +
#         max_version.to_bytes(2) +
#         tag_buffer +
#         throttle_time_ms.to_bytes(4) +
#         tag_buffer
#     )

#     response_length = len(response_header) + len(response_body)
#     return int(response_length).to_bytes(4) + response_header + response_body


# def main():
#     server = socket.create_server(("localhost", 9092), reuse_port=True)
#     client, _ = server.accept()
#     request = KafkaRequest.from_client(client)
#     print(request)
#     client.sendall(make_response(request))


# if __name__ == "__main__":
#     main()



import socket
from dataclasses import dataclass
from enum import Enum, unique


@unique
class ErrorCode(Enum):
    NONE = 0
    UNSUPPORTED_VERSION = 35


@dataclass
class KafkaRequest:
    api_key: int
    api_version: int
    correlation_id: int

    @staticmethod
    def from_client(client: socket.socket):
        data = client.recv(2048)
        return KafkaRequest(
            api_key=int.from_bytes(data[4:6]),
            api_version=int.from_bytes(data[6:8]),
            correlation_id=int.from_bytes(data[8:12]),
        )


def make_response(request: KafkaRequest):
    response_header = request.correlation_id.to_bytes(4)

    valid_api_versions = [0, 1, 2, 3, 4]
    error_code = (
        ErrorCode.NONE
        if request.api_version in valid_api_versions
        else ErrorCode.UNSUPPORTED_VERSION
    )

    # Entries for APIVersions and DescribeTopicPartitions
    api_entries = [
        (18, 0, 4),  # APIVersions: MinVersion 0, MaxVersion 4
        (75, 0, 0)   # DescribeTopicPartitions: MinVersion 0, MaxVersion 0
    ]

    # Prepare API versions response
    api_versions_buffer = b''.join(
        (api_key.to_bytes(2) +
         min_version.to_bytes(2) +
         max_version.to_bytes(2))
        for api_key, min_version, max_version in api_entries
    )

    num_api_keys = len(api_entries)  # This should be 2
    throttle_time_ms = 0

    # Building the response body
    response_body = (
        error_code.value.to_bytes(2) +          # Error code
        num_api_keys.to_bytes(2) +              # Number of API version entries
        api_versions_buffer +                    # API version entries
        throttle_time_ms.to_bytes(4)            # Throttle time
    )

    response_length = len(response_header) + len(response_body)
    return int(response_length).to_bytes(4) + response_header + response_body


def main():
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    client, _ = server.accept()
    request = KafkaRequest.from_client(client)
    print(request)
    client.sendall(make_response(request))


if __name__ == "__main__":
    main()
