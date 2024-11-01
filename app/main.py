def make_response(request: KafkaRequest):
    response_header = request.correlation_id.to_bytes(4)

    valid_api_versions = [0, 1, 2, 3, 4]
    error_code = (
        ErrorCode.NONE
        if request.api_version in valid_api_versions
        else ErrorCode.UNSUPPORTED_VERSION
    )

    # Prepare response body based on API key and error code
    if request.api_key == 18:
        # Handle API Key 18 specific logic
        min_version, max_version = 0, 4
        if error_code == ErrorCode.NONE and request.api_version < min_version:
            error_code = ErrorCode.UNSUPPORTED_VERSION
        throttle_time_ms = 0
        tag_buffer = b"\x00"
        response_body = (
            error_code.value.to_bytes(2) +
            int(2).to_bytes(1) +
            request.api_key.to_bytes(2) +
            min_version.to_bytes(2) +
            max_version.to_bytes(2) +
            tag_buffer +
            throttle_time_ms.to_bytes(4) +
            tag_buffer
        )
    else:
        # Handle other API keys (if needed)
        response_body = (
            error_code.value.to_bytes(2) +
            int(2).to_bytes(1) +
            request.api_key.to_bytes(2) +
            b'\x00' * 4  # Dummy response for other API keys
        )

    response_length = len(response_header) + len(response_body)
    return int(response_length).to_bytes(4) + response_header + response_body
