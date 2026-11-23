"""Domain errors and their HTTP mapping. Every error body has the same shape."""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


class DomainError(Exception):
    """An error the client can act on. ``code`` is stable; ``message`` is for people."""

    status_code: int = 400
    code: str = "domain_error"

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class InvalidWorkspaceError(DomainError):
    status_code = 400
    code = "invalid_workspace"


class WorkspaceNotFoundError(DomainError):
    status_code = 404
    code = "workspace_not_found"


class RunNotFoundError(DomainError):
    status_code = 404
    code = "run_not_found"


class NoGoldError(DomainError):
    status_code = 404
    code = "no_gold_tables"


class EmptyQuestionError(DomainError):
    status_code = 422
    code = "empty_question"


class SqlRejectedError(DomainError):
    status_code = 422
    code = "sql_rejected"


class MappingInvalidError(DomainError):
    status_code = 422
    code = "mapping_invalid"


class FileInvalidError(DomainError):
    status_code = 422
    code = "file_invalid"


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(DomainError)
    async def _domain_error(_request: Request, exc: DomainError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": {"code": exc.code, "message": exc.message}},
        )
