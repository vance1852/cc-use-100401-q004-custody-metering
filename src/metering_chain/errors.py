"""计量监管链服务向 API 和 CLI 暴露的稳定错误。"""


class MeteringError(RuntimeError):
    code = "metering_error"
    status = 400


class NotFound(MeteringError):
    code = "not_found"
    status = 404


class Conflict(MeteringError):
    code = "conflict"
    status = 409


class Forbidden(MeteringError):
    code = "forbidden"
    status = 403


class InvalidState(MeteringError):
    code = "invalid_state"
    status = 409


class ValidationFailed(MeteringError):
    code = "validation_failed"
    status = 422
