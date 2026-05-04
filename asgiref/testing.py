import asyncio
import contextvars
import time
from typing import Optional

from .compatibility import guarantee_single_callable
from .timeout import timeout as async_timeout
from .typing import ASGIApplication, ASGIReceiveEvent, ASGISendEvent, Scope


class ApplicationCommunicator:
    """
    Runs an ASGI application in a test mode, allowing sending of
    messages to it and retrieval of messages it sends.
    """

    def __init__(self, application: ASGIApplication, scope: Scope) -> None:
        self._future: Optional[asyncio.Task[None]] = None
        self.application = guarantee_single_callable(application)
        self.scope = scope
        self._input_queue: Optional[asyncio.Queue[ASGIReceiveEvent]] = None
        self._output_queue: Optional[asyncio.Queue[ASGISendEvent]] = None

    # For Python 3.9 we need to lazily bind the queues, on 3.10+ they bind the
    # event loop lazily.
    @property
    def input_queue(self) -> asyncio.Queue[ASGIReceiveEvent]:
        if self._input_queue is None:
            self._input_queue = asyncio.Queue()
        return self._input_queue

    @property
    def output_queue(self) -> asyncio.Queue[ASGISendEvent]:
        if self._output_queue is None:
            self._output_queue = asyncio.Queue()
        return self._output_queue

    @property
    def future(self) -> asyncio.Task[None]:
        if self._future is None:
            # Clear context - this ensures that context vars set in the testing scope
            # are not "leaked" into the application which would normally begin with
            # an empty context. In Python >= 3.11 this could also be written as:
            # asyncio.create_task(..., context=contextvars.Context())
            self._future = contextvars.Context().run(
                asyncio.create_task,  # type: ignore[arg-type]
                self.application(
                    self.scope, self.input_queue.get, self.output_queue.put
                ),
            )
        return self._future

    async def wait(self, timeout: float = 1) -> None:
        """
        Waits for the application to stop itself and returns any exceptions.
        """
        try:
            async with async_timeout(timeout):
                try:
                    await self.future
                    self.future.result()
                except asyncio.CancelledError:
                    pass
        finally:
            if not self.future.done():
                self.future.cancel()
                try:
                    await self.future
                except asyncio.CancelledError:
                    pass

    def stop(self, exceptions: bool = True) -> None:
        future = self._future
        if future is None:
            return

        if not future.done():
            future.cancel()
        elif exceptions:
            # Give a chance to raise any exceptions
            future.result()

    def __del__(self) -> None:
        # Clean up on deletion
        try:
            self.stop(exceptions=False)
        except RuntimeError:
            # Event loop already stopped
            pass

    async def send_input(self, message: ASGIReceiveEvent) -> None:
        """
        Sends a single message to the application
        """
        # Make sure there's not an exception to raise from the task
        if self.future.done():
            self.future.result()

        # Give it the message
        await self.input_queue.put(message)

    async def receive_output(self, timeout: float = 1) -> ASGISendEvent:
        """
        Receives a single message from the application, with optional timeout.
        """
        # Make sure there's not an exception to raise from the task
        if self.future.done():
            self.future.result()
        # Wait and receive the message
        try:
            async with async_timeout(timeout):
                return await self.output_queue.get()
        except asyncio.TimeoutError as e:
            # See if we have another error to raise inside
            if self.future.done():
                self.future.result()
            else:
                self.future.cancel()
                try:
                    await self.future
                except asyncio.CancelledError:
                    pass
            raise e

    async def receive_nothing(
        self, timeout: float = 0.1, interval: float = 0.01
    ) -> bool:
        """
        Checks that there is no message to receive in the given time.
        """
        # Make sure there's not an exception to raise from the task
        if self.future.done():
            self.future.result()

        # `interval` has precedence over `timeout`
        start = time.monotonic()
        while time.monotonic() - start < timeout:
            if not self.output_queue.empty():
                return False
            await asyncio.sleep(interval)
        return self.output_queue.empty()
