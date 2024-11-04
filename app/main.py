import select
import socket
import struct
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import IntEnum
from typing import Dict, List, Optional, Tuple
# Domain Models
@dataclass
class RequestHeader:
    message_size: int
    request_api_key: int
    request_api_version: int
    correlation_id: int
@dataclass
class ResponseHeader:
    correlation_id: int
    message_size: int
@dataclass
class KafkaError:
    code: int
    message: str
    retriable: bool
@dataclass
class ApiVersion:
    api_key: int
    min_version: int
    max_version: int
class ApiKey(IntEnum):
    API_VERSIONS = 18
    TOPIC_PARTITIONS = 75
class ClientConnection:
    def __init__(self, socket: socket.socket, address: Tuple[str, int]):
        self.socket = socket
        self.address = address
        self.buffer = bytearray()
        self.is_active = True
        self.lock = threading.Lock()
    def close(self):
        with self.lock:
            self.is_active = False
            try:
                self.socket.close()
            except Exception:
                pass
class ConnectionRegistry:
    def __init__(self):
        self.connections: Dict[Tuple[str, int], ClientConnection] = {}
        self.lock = threading.Lock()
    def add_connection(self, connection: ClientConnection):
        with self.lock:
            self.connections[connection.address] = connection
    def remove_connection(self, address: Tuple[str, int]):
        with self.lock:
            if address in self.connections:
                self.connections[address].close()
                del self.connections[address]
    def get_active_connections(self) -> List[ClientConnection]:
        with self.lock:
            return [conn for conn in self.connections.values() if conn.is_active]
class VarInt:
    @staticmethod
    def encode(value: int) -> bytes:
        bytes_list = []
        while True:
            byte = value & 0x7F
            value >>= 7
            if value:
                byte |= 0x80
            bytes_list.append(byte)
            if not value:
                break
        return bytes(bytes_list)
    @staticmethod
    def decode(buffer: bytes, offset: int = 0) -> Tuple[int, int]:
        result = 0
        shift = 0
        position = offset
        while position < len(buffer):
            byte = buffer[position]
            result |= (byte & 0x7F) << shift
            position += 1
            if not (byte & 0x80):
                break
            shift += 7
            if shift > 28:
                raise ValueError("Invalid varint")
        return result, position - offset
class TagBuffer:
    def __init__(self):
        self.tags = {}
    def put(self, tag: int, value: bytes):
        self.tags[tag] = value
    def encode(self) -> bytes:
        if not self.tags:
            return VarInt.encode(0)
        result = bytearray()
        result.extend(VarInt.encode(len(self.tags) + 1))
        # result.extend(VarInt.encode(len(self.tags) + 1))
        for _, value in sorted(self.tags.items()):
            result.extend(value)
        return bytes(result)
class KafkaErrors:
    ERROR_CODES: Dict[int, KafkaError] = {
        -1: KafkaError(-1, "UNKNOWN_SERVER_ERROR", False),
        0: KafkaError(0, "NONE", False),
        1: KafkaError(1, "OFFSET_OUT_OF_RANGE", False),
        2: KafkaError(2, "CORRUPT_MESSAGE", True),
        3: KafkaError(3, "UNKNOWN_TOPIC_OR_PARTITION", True),
        4: KafkaError(4, "INVALID_FETCH_SIZE", False),
        5: KafkaError(5, "LEADER_NOT_AVAILABLE", True),
        6: KafkaError(6, "NOT_LEADER_OR_FOLLOWER", True),
        7: KafkaError(7, "REQUEST_TIMED_OUT", True),
        8: KafkaError(8, "BROKER_NOT_AVAILABLE", False),
        9: KafkaError(9, "REPLICA_NOT_AVAILABLE", True),
        10: KafkaError(10, "MESSAGE_TOO_LARGE", False),
        35: KafkaError(35, "UNSUPPORTED_VERSION", False),
    }
    @staticmethod
    def get_error(code: int) -> KafkaError:
        return KafkaErrors.ERROR_CODES.get(
            code, KafkaError(-1, "UNKNOWN_SERVER_ERROR", False)
        )
# Protocol
class IRequestParser(ABC):
    @abstractmethod
    def parse_header(self, message_bytes: bytes) -> Tuple[RequestHeader, int]:
        pass
class IResponseBuilder(ABC):
    @abstractmethod
    def build_response(
        self, correlation_id: int, error: KafkaError, body: bytes, api_version: int
    ) -> bytes:
        pass
class KafkaRequestHeaderParser(IRequestParser):
    def parse_header(self, message_bytes: bytes) -> Tuple[RequestHeader, int]:
        try:
            message_size = struct.unpack(">i", message_bytes[0:4])[0]
            api_key = struct.unpack(">h", message_bytes[4:6])[0]
            api_version = struct.unpack(">h", message_bytes[6:8])[0]
            correlation_id = struct.unpack(">i", message_bytes[8:12])[0]
            client_id_length = struct.unpack(">h", message_bytes[12:14])[0]
            _client_id = message_bytes[14 : 14 + client_id_length]
            return (
                RequestHeader(
                    message_size=message_size,
                    request_api_key=api_key,
                    request_api_version=api_version,
                    correlation_id=correlation_id,
                ),
                14 + client_id_length,
            )
        except struct.error as e:
            raise ValueError(f"Failed to parse header: {e}")


class KafkaResponseBuilder(IResponseBuilder):
    def build_response(
        self, correlation_id: int, error: KafkaError, body: bytes, api_version: int
    ) -> bytes:
        # Pack the correlation ID
        response_header = struct.pack(">i", correlation_id)
        # Pack the error code
        error_code = struct.pack(">h", error.code)

        # Prepare the response body depending on the API version
        if api_version >= 3:
            tag_buffer = TagBuffer()
            tag_buffer.put(0, body)  # Assuming body is the actual response data
            response_body = tag_buffer.encode()
        else:
            api_version_tag = struct.pack(">h", api_version)
            response_body = api_version_tag + body

        # Ensure throttle time is a 4-byte int
        throttle_time = struct.pack(">i", 0)  # Assuming 0 ms for throttle time

        # Construct the complete response
        complete_response = (
            response_header +
            error_code +
            response_body +
            throttle_time +  # Adding the throttle time
            TagBuffer().encode()  # Empty tags
        )

        # Prepend the size of the complete response
        message_size = len(complete_response)
        size_prefix = struct.pack(">i", message_size)

        return size_prefix + complete_response


class ApiVersionRegistry:
    def __init__(self):
        self.supported_versions: Dict[ApiKey, List[int]] = {
            ApiKey.API_VERSIONS: [0, 1, 2, 3, 4, 5],
            ApiKey.TOPIC_PARTITIONS: [0],
        }
        self.api_versions: List[ApiVersion] = [
            ApiVersion(ApiKey.API_VERSIONS, 0, 5),  # Supporting versions 0 to 5 for API_VERSIONS
            ApiVersion(ApiKey.TOPIC_PARTITIONS, 0, 0),  # Supporting only version 0 for TOPIC_PARTITIONS
        ]

    def is_supported(self, api_key: int, version: int) -> bool:
        try:
            api_enum = ApiKey(api_key)
            return version in self.supported_versions.get(api_enum, [])
        except ValueError:
            return False

    def get_api_versions_response(self, api_version: int) -> bytes:
        response = bytearray()
        response.extend(VarInt.encode(len(self.api_versions) + 1))
        for version in self.api_versions:
            response.extend(struct.pack(">h", version.api_key))
            response.extend(struct.pack(">h", version.min_version))
            response.extend(struct.pack(">h", version.max_version))
            if api_version >= 3:
                response.extend(VarInt.encode(0))  # Empty tags
        return bytes(response)


class IRequestHandler(ABC):
    @abstractmethod
    def handle_request(
        self, header: RequestHeader, body: bytes
    ) -> Tuple[KafkaError, bytes]:
        pass
class ApiVersionsRequestHandler(IRequestHandler):
    def __init__(self, version_registry: ApiVersionRegistry):
        self.version_registry = version_registry
    def handle_request(
        self, header: RequestHeader, body: bytes
    ) -> Tuple[KafkaError, bytes]:
        if not self.version_registry.is_supported(
            header.request_api_key, header.request_api_version
        ):
            return KafkaErrors.get_error(35), b""
        response_body = self.version_registry.get_api_versions_response(
            header.request_api_version
        )
        return KafkaErrors.get_error(0), response_body
class KafkaClientHandler:
    def __init__(
        self,
        parser: IRequestParser,
        response_builder: IResponseBuilder,
        request_handler: IRequestHandler,
    ):
        self.parser = parser
        self.response_builder = response_builder
        self.request_handler = request_handler
    def handle_client(self, connection: ClientConnection) -> None:
        try:
            while connection.is_active:
                ready = select.select([connection.socket], [], [], 0.1)
                if ready[0]:
                    data = connection.socket.recv(4096)
                    if not data:
                        break
                    header, length = self.parser.parse_header(data)
                    error, response_body = self.request_handler.handle_request(
                        header, data[length:]
                    )
                    response = self.response_builder.build_response(
                        header.correlation_id,
                        error,
                        response_body,
                        header.request_api_version,
                    )
                    with connection.lock:
                        connection.socket.sendall(response)
        except Exception as e:
            print(f"Error in client handler: {e}")
        finally:
            connection.close()
# Server
class KafkaServer:
    def __init__(self, host: str, port: int, client_handler: KafkaClientHandler):
        self.host = host
        self.port = port
        self.client_handler = client_handler
        self.server: Optional[socket.socket] = None
        self.connection_registry = ConnectionRegistry()
        self.is_running = False
        self.accept_thread: Optional[threading.Thread] = None
    def _handle_client(self, connection: ClientConnection):
        try:
            self.client_handler.handle_client(connection)
        finally:
            self.connection_registry.remove_connection(connection.address)
    def _accept_connections(self):
        while self.is_running:
            try:
                client_socket, address = self.server.accept()
                connection = ClientConnection(client_socket, address)
                self.connection_registry.add_connection(connection)
                # Start new thread for client
                client_thread = threading.Thread(
                    target=self._handle_client, args=(connection,), daemon=True
                )
                client_thread.start()
            except Exception as e:
                if self.is_running:
                    print(f"Error accepting connection: {e}")
    def start(self):
        try:
            self.server = socket.create_server(
                (self.host, self.port),
                reuse_port=True,
                backlog=50,
            )
            self.server.setblocking(True)
            self.is_running = True
            print(f"Server is listening on {self.host}:{self.port}")
            self.accept_thread = threading.Thread(
                target=self._accept_connections, daemon=True
            )
            self.accept_thread.start()
            try:
                while True:
                    threading.Event().wait(1)
            except KeyboardInterrupt:
                print("\nShutting down server...")
        except Exception as e:
            print(f"Server error: {e}")
        finally:
            self.stop()
    def stop(self):
        self.is_running = False
        for connection in self.connection_registry.get_active_connections():
            connection.close()
        if self.server:
            self.server.close()
        if self.accept_thread and self.accept_thread.is_alive():
            self.accept_thread.join(timeout=5.0)
def main():
    version_registry = ApiVersionRegistry()
    parser = KafkaRequestHeaderParser()
    response_builder = KafkaResponseBuilder()
    request_handler = ApiVersionsRequestHandler(version_registry)
    client_handler = KafkaClientHandler(parser, response_builder, request_handler)
    server = KafkaServer("localhost", 9092, client_handler)
    server.start()
if __name__ == "__main__":
    print(int(2).to_bytes(1))
    main()