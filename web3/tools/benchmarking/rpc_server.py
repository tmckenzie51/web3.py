from abc import abstractmethod
import collections
import io
import json
import logging
import pathlib
from typing import Any, Dict, Generic, Mapping, Tuple, TypeVar, cast
import tempfile
import trio

from eth_utils import ValidationError

#from web3.types import RPCRequest, RPCResponse, JSON
from web3.exceptions import DecodingError

NEW_LINE = "\n"

class UnknownMethodHandler():
    async def __call__(self, request):
        return generate_error_response(
            request, f"Unknown RPC method: {request['method']}"
        )


fallback_handler = UnknownMethodHandler()

class Service():
    def __str__(self) -> str:
        return self.__class__.__name__

    @property
    def manager(self) -> "InternalManagerAPI":
        """
        Expose the manager as a property here intead of
        :class:`async_service.abc.ServiceAPI` to ensure that anyone using
        proper type hints will not have access to this property since it isn't
        part of that API, while still allowing all subclasses of the
        :class:`async_service.base.Service` to access this property directly.
        """
        return self._manager

    def get_manager(self):
        try:
            return self._manager
        except AttributeError:
            raise LifecycleError(
                "Service does not have a manager assigned to it.  Are you sure "
                "it is running?"
            )


def ipc_path():
    with tempfile.TemporaryDirectory() as temp_xdg:
        return pathlib.Path(temp_xdg) / "jsonrpc.ipc"

class RPCServer(Service):
    logger = logging.getLogger("alexandria.rpc.RPCServer")
    _handlers: Dict[str, Any]

    def __init__(
        self, ipc_path: pathlib.Path, handlers: Dict[str, Any]
    ) -> None:
        self.ipc_path = ipc_path
        self._handlers = handlers
        self._serving = trio.Event()

    def wait_serving(self) -> None:
         self._serving.wait()

    def run(self) -> None:
        self.manager.run_daemon_task(self.serve, self.ipc_path)
        try:
             self.manager.wait_finished()
        finally:
            self.ipc_path.unlink(missing_ok=True)

    def execute_rpc(self, request: Any) -> str:
        method = request["method"]

        self.logger.debug("RPCServer handling request: %s", method)

        handler = self._handlers.get(method, fallback_handler)
        try:
            response =  handler(request)
        except Exception as err:
            self.logger.error("Error handling request: %s  error: %s", request, err)
            self.logger.debug("Error handling request: %s", request, exc_info=True)
            response = generate_error_response(request, f"Unexpected Error: {err}")

        return json.dumps(response)

    def serve(self, ipc_path: pathlib.Path) -> None:
        self.logger.info("Starting RPC server over IPC socket: %s", ipc_path)

        with trio.socket.socket(trio.socket.AF_UNIX, trio.socket.SOCK_STREAM) as sock:
            sock.bind(str(ipc_path))

            # Allow up to 10 pending connections.
            sock.listen(10)

            self._serving.set()

            while self.manager.is_running:
                conn, addr =  sock.accept()
                self.logger.debug("Server accepted connection: %r", addr)
                self.manager.run_task(self._handle_connection, conn)

    def _handle_connection(self, socket: trio.socket.SocketType) -> None:
        buffer = io.StringIO()

        with socket:
            while True:
                try:
                    request =  read_json(socket, buffer)
                except DecodingError:
                    # If the connection receives bad JSON, close the connection.
                    return

                if not isinstance(request, collections.abc.Mapping):
                    logger.debug("Invalid payload: %s", type(request))
                    write_error(socket, "Invalid Request: not a mapping")
                    continue

                if not request:
                    self.logger.debug("Client sent empty request")
                    write_error(socket, "Invalid Request: empty")
                    continue

                try:
                    validate_request(request)
                except ValidationError as err:
                    write_error(socket, str(err))
                    continue

                try:
                    result =  self.execute_rpc(cast(Any, request))
                except Exception as e:
                    self.logger.exception("Unrecognized exception while executing RPC")
                    write_error(socket, "unknown failure: " + str(e))
                else:
                    if not result.endswith(NEW_LINE):
                        result += NEW_LINE

                    try:
                         socket.send(result.encode("utf8"))
                    except BrokenPipeError:
                        break

if __name__ == "__main__":
     RPCServer(ipc_path(), {'handler': 1}).run()
