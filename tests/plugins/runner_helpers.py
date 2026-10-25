"""Running one runner turn in a test (runner plan RN.6)."""
import threading


class Active:
    """What a turn reads of its runner (runner.Runner)."""

    def __init__(self, role="server"):
        self.role = role
        self.stopped = threading.Event()
        self.begun = []

    def begin_action(self, action):
        self.begun.append(action)

    def end_action(self):
        pass


def turn(mod, role="server"):
    """``(Turn, actions)``: one ``run_runner_turn`` of ``role`` and the turn's
    per-action report (``TurnReport.actions``)."""
    reports = []
    real = mod.TurnReport

    class Capture(real):
        def __init__(self):
            super().__init__()
            reports.append(self)

    mod.TurnReport = Capture
    token = mod.runner.ROLE.set(role)  # as runner.Runner._turn does
    try:
        result = mod.run_runner_turn(Active(role))
    finally:
        mod.runner.ROLE.reset(token)
        mod.TurnReport = real
    return result, (reports[0].actions if reports else [])
