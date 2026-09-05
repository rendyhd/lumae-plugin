"""Cooperative worker control without a database round trip per audio frame."""

import time


class CancellationPoller:
    """Cache cancellation briefly; deadlines remain local and unthrottled.

    Callback failures propagate: losing the ownership database must never let
    a worker continue publishing under an ownership token it cannot verify.
    """

    def __init__(self, callback, *, interval=0.5, clock=time.monotonic):
        self.callback = callback
        self.interval = max(0.05, min(1.0, float(interval)))
        self.clock = clock
        self.next_check = float("-inf")
        self.cancelled = False

    def __call__(self, *, force=False):
        now = self.clock()
        if self.cancelled:
            return True
        if force or now >= self.next_check:
            self.cancelled = bool(self.callback()) if self.callback else False
            self.next_check = now + self.interval
        return self.cancelled
