"""P3-10 (LUM-021): stored error text is shown without credentials or paths."""

import importlib.util
import pathlib
import signal
import time

import pytest

# Load the module by path: it has no plugin imports, so it needs no host stub.
_SPEC = importlib.util.spec_from_file_location(
    "lumae_redaction",
    pathlib.Path(__file__).resolve().parents[2] / "plugins/LumaeAnalysis/redaction.py",
)
redaction = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(redaction)
redact = redaction.redact_error_text

# Review findings on the first redactor (MEDIUM-1, MEDIUM-2, L1, L2).
NEW_SECRET_SHAPES = [
    # A secret split across lines is matched after whitespace is collapsed.
    (
        "request failed: Authorization:\n  Bearer\n  abc123def456 (401)",
        ["abc123def456"],
        "Authorization: [redacted] (401)",
    ),
    ("GET https://user:\npass1234@host/rest failed", ["pass1234", "user"],
     "https://[redacted]@host/rest failed"),
    ("PGPASSWORD=\n  hunter2 psql", ["hunter2"], "PGPASSWORD= [redacted] psql"),
    # Quoted paths are masked whole, whatever the file name holds.
    (
        "[Errno 2] No such file or directory: '/music/Artist/Album (Deluxe)/01 Song.flac'",
        ["Artist", "Deluxe", "Song.flac"],
        "No such file or directory: '[path]'",
    ),
    (
        "[Errno 13] Permission denied: '/music/Queen/A Night (Remastered)/01 (Live).flac'",
        ["Queen", "Remastered", "Live"],
        "Permission denied: '[path]'",
    ),
    (
        "cannot decode '/music/Band/Hits (2020)/03.mp3': invalid data",
        ["Band", "2020", "03.mp3"],
        "cannot decode '[path]': invalid data",
    ),
    ("No such file: '/music/AC: DC/01.flac'", ["AC", "DC", "01.flac"],
     "No such file: '[path]'"),
    (
        "No such file or directory: \"/music/Guns N' Roses/Appetite/01.flac\"",
        ["Guns", "Roses", "Appetite"],
        "No such file or directory: \"[path]\"",
    ),
    (
        "No such file: '/music/It\\'s \"Live\"/01.flac' (retrying)",
        ["Live", "01.flac"],
        "No such file: '[path]' (retrying)",
    ),
    ("open 'C:\\\\Users\\\\alice\\\\My Music\\\\x (Live).flac' failed", ["alice", "Live"],
     "open '[path]' failed"),
    ("read '~/Music/Some Artist (Live)/x.flac' failed", ["Some Artist", "Live"],
     "read '[path]' failed"),
    # Letters-only credentials after an authentication scheme.
    ("Bearer abcdefghijklmnopqrstu was rejected", ["abcdefghijklmnopqrstu"],
     "Bearer [redacted] was rejected"),
    ("Digest qwertyuiopasdfgh", ["qwertyuiopasdfgh"], "Digest [redacted]"),
    # Escaped JSON, and a secret parameter of a nested URL.
    ('body={\\"token\\": \\"s3cr3t-value\\", \\"ok\\": 1}', ["s3cr3t-value"],
     'body={\\"token\\": [redacted], \\"ok\\": 1}'),
    ("redirect https://a.example/login?next=https://b.example/cb?token=abc123xyz",
     ["abc123xyz"], "?token=[redacted]"),
    # An unclosed quoted value.
    ('password="hunter2 was cut', ["hunter2"], "password=[redacted] was cut"),
]
NEW_READABLE = [
    "Bearer authentication failed",
    "Basic credentials required",
    "cannot decode track 12 (Live): invalid data",
    "HTTP 404 for https://navidrome:4533/rest/stream?id=tr-0000001&format=raw",
]


SECRET_SHAPES = [
        # DSNs: URL form and libpq keyword form, and the password variable.
        (
            "could not connect: postgresql://lumae:Sup3r-S3cret@db.internal:5432/audiomuse",
            ["lumae:", "Sup3r-S3cret"],
            "postgresql://[redacted]@db.internal:5432/audiomuse",
        ),
        (
            "host=db user=alice password=hunter2 dbname=audiomuse",
            ["hunter2"],
            "password=[redacted] dbname=audiomuse",
        ),
        ("PGPASSWORD=hunter2 psql failed", ["hunter2"], "PGPASSWORD=[redacted] psql failed"),
        # Any URL with credentials, e.g. an HTTP proxy or a provider URL.
        (
            "ProxyError: https://bob:pr0xy-pass@proxy.local:3128 refused",
            ["bob", "pr0xy-pass"],
            "https://[redacted]@proxy.local:3128 refused",
        ),
        # Subsonic (Navidrome): token, salt and password query parameters.
        (
            "GET http://navidrome:4533/rest/getSong.view?u=admin&t=26719a1196d2a940705a"
            "&s=c19b2d&v=1.16.1 returned 500",
            ["26719a1196d2a940705a", "c19b2d"],
            "?u=admin&t=[redacted]&s=[redacted]&v=1.16.1 returned 500",
        ),
        (
            "http://navidrome:4533/rest/ping?u=admin&p=enc:68756e74657232",
            ["68756e74657232"],
            "&p=[redacted]",
        ),
        # API keys in URLs.
        (
            "lookup failed: https://api.example.com/v1/items?api_key=AKfake0123456789&limit=5",
            ["AKfake0123456789"],
            "?api_key=[redacted]&limit=5",
        ),
        (
            "https://api.example.com/v1/items?limit=5&access_token=ya29.a0Af",
            ["ya29.a0Af"],
            "&access_token=[redacted]",
        ),
        # Bearer, Basic and cookie headers, and a bare bearer token.
        (
            "401 with Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2ln",
            ["eyJhbGciOiJIUzI1NiJ9", "c2ln"],
            "Authorization: [redacted]",
        ),
        (
            "headers={'authorization': 'Basic YWxpY2U6aHVudGVyMg=='}",
            ["YWxpY2U6aHVudGVyMg=="],
            "[redacted]",
        ),
        ("Bearer abcdef0123456789 was rejected", ["abcdef0123456789"], "Bearer [redacted]"),
        ("Cookie: sessionid=abc123; csrftoken=zzz", ["abc123", "zzz"], "Cookie: [redacted]"),
        # key=value and key: value secrets, as reprs and JSON.
        ("{'password': 'hunter2', 'user': 'alice'}", ["hunter2"], "'password': [redacted]"),
        ('{"client_secret": "zz9-top", "ok": 1}', ["zz9-top"], '"client_secret": [redacted]'),
        ("token=abc.def.ghi expired", ["abc.def.ghi"], "token=[redacted] expired"),
        # Opaque tokens without a key.
        (
            "decode failed for eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2lnbmF0dXJl",
            ["eyJzdWIiOiIxIn0"],
            "decode failed for [redacted]",
        ),
        (
            "key sk-ant-api03-abcdefghijklmnopqrstuvwxyz0123 leaked",
            ["abcdefghijklmnopqrstuvwxyz0123"],
            "key [redacted] leaked",
        ),
        ("AWS AKIAIOSFODNN7EXAMPLE denied", ["AKIAIOSFODNN7EXAMPLE"], "AWS [redacted] denied"),
        (
            "token 9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08 used",
            ["9f86d081884c7d659a2feaa0c55ad015"],
            "token [redacted] used",
        ),
        # File paths, including music file names with spaces.
        (
            "could not open /home/alice/Music/Artist/01 - Song.flac: No such file",
            ["/home/alice", "alice", "Song.flac"],
            "could not open [path]: No such file",
        ),
        (
            "[Errno 2] No such file or directory: '/srv/music/a b/track.mp3'",
            ["/srv/music", "track.mp3"],
            "No such file or directory: '[path]'",
        ),
        (
            'Traceback: File "/usr/lib/python3/dist-packages/requests/api.py", line 3',
            ["/usr/lib"],
            'File "[path]", line 3',
        ),
        ("could not open C:\\Users\\alice\\Music\\song.flac", ["alice"], "could not open [path]"),
        ("share \\\\nas\\music\\alice\\x.flac failed", ["alice"], "share [path]"),
        ("open file:///srv/music/alice.flac", ["alice"], "open [path]"),
        ("cache ~/secret-dir/x.db locked", ["secret-dir"], "cache [path]"),
    *NEW_SECRET_SHAPES,
]


@pytest.mark.parametrize(("text", "secrets", "kept"), SECRET_SHAPES)
def test_each_secret_shape_is_masked(text, secrets, kept):
    result = redact(text)
    for secret in secrets:
        assert secret not in result, (secret, result)
    assert kept in result, result


READABLE = [
        'connection to server at "127.0.0.1", port 5432 failed: Connection refused',
        'duplicate key value violates unique constraint "plugin_lumae_analysis__source_profiles_pkey"'
        " DETAIL: Key (catalog_instance_id, track_id)=(ca8acd69-bd8f-45a9-b31c-80862c8d650a,"
        " tr-0000001) already exists.",
        'password authentication failed for user "lumae"',
        "Basic authentication required",
        "token expired",
        "canceling statement due to statement timeout",
        "HTTPConnectionPool(host='navidrome', port=4533): Read timed out. (read timeout=30)",
        "Catalogue media revision changed",
        "One or more profiles could not be prepared.",
        "The catalogue worker did not attest the plugin version requested by the AudioMuse API.",
    *NEW_READABLE,
]


@pytest.mark.parametrize("text", READABLE)
def test_ordinary_error_text_stays_readable(text):
    assert redact(text) == text


def test_safe_codes_are_shown_unchanged_and_nothing_else_is_trusted():
    codes = {"analysis_timeout", "queue_unavailable"}
    assert redact("analysis_timeout", codes) == "analysis_timeout"
    assert redact("  queue_unavailable ", codes) == "queue_unavailable"
    # A safe code is an exact match, not a prefix that lets text through.
    assert redact("analysis_timeout password=hunter2", codes) == (
        "analysis_timeout password=[redacted]"
    )


def test_whitespace_is_collapsed_and_length_is_capped_after_masking():
    assert redact(None) is None
    assert redact("   ") is None
    assert redact("line one\n\tline two") == "line one line two"
    long = "x" * 1000
    assert len(redact(long)) == redaction.MAX_ERROR_TEXT_CHARS
    assert redact(long).endswith("…")
    # A secret beyond the displayed prefix is masked before the cut.
    tail = "y" * (redaction.MAX_ERROR_TEXT_CHARS - 20) + " password=hunter2 " + "z" * 50
    assert "hunter2" not in redact(tail)
    # The cut never leaves half a mask ("password=[re…").
    for length in range(redaction.MAX_ERROR_TEXT_CHARS - 22, redaction.MAX_ERROR_TEXT_CHARS - 8):
        capped = redact("y" * length + " password=hunter2 tail")
        bracket = capped.rfind("[")
        assert bracket < 0 or "]" in capped[bracket:], capped[-24:]


@pytest.mark.parametrize(
    "text",
    [
        *(row[0] for row in SECRET_SHAPES),
        *READABLE,
        # Cut by the cap, including inside what becomes a mask.
        "x" * 1000,
        "y" * (redaction.MAX_ERROR_TEXT_CHARS - 12) + " password=hunter2 tail",
        "z" * (redaction.MAX_ERROR_TEXT_CHARS - 8) + " /music/a/b.flac: gone",
    ],
)
def test_redaction_is_idempotent(text):
    once = redact(text)
    assert redact(once) == once


# ---------------------------------------------------------------------------
# Cost: every pattern stays linear (review HIGH-1, a ReDoS in the key=value
# pattern: "token." * 4000 took 17 s). Each input is 100 KB, far above the
# 4,000 characters redact_error_text scans, so a super-linear pattern shows.
# ---------------------------------------------------------------------------

ADVERSARIAL_UNITS = [
    "token.", "secret-", "-", ".a", "a.", "-a", "=", "/", "%", ":", "_", ".",
    "token.secret-", "a.-_", "pwd.", "api_key-", "token=", "a=", "=a", "a: ",
    "a:\"b ", "a:'b ", "\\\"a\\\": ", "&token.", "&a=", "?a", "?token=", "sk-",
    "eyJ-", "eyJa.", "Bearer ", "bearer a", "https://", "a://", "a@", "x x",
    " /a", "/a", "'/", "\"/", "\\\\a", "\\", "C:\\", "a:\\", "~/", "file://",
    "Authorization: ", "cookie=", "aA0", "+/", "'", "\"", ":/",
]
ADVERSARIAL_BYTES = 100_000
# The worst measured case is about 17 ms; the bound leaves room for a slow
# CI runner, and a super-linear pattern takes seconds.
PATTERN_BUDGET_S = 0.05


# A super-linear pattern on 100 KB runs for hours; the regex engine checks for
# signals, so an alarm stops it and the test fails instead of hanging.
ABORT_AFTER_S = 2.0


class _Aborted(Exception):
    pass


def _abort(*_args):
    raise _Aborted


def _best_of(runs, action):
    best = float("inf")
    guarded = hasattr(signal, "setitimer")
    previous = signal.signal(signal.SIGALRM, _abort) if guarded else None
    try:
        for _ in range(runs):
            if guarded:
                signal.setitimer(signal.ITIMER_REAL, ABORT_AFTER_S)
            started = time.perf_counter()
            try:
                action()
            except _Aborted:
                return float("inf")
            finally:
                if guarded:
                    signal.setitimer(signal.ITIMER_REAL, 0)
            best = min(best, time.perf_counter() - started)
    finally:
        if guarded:
            signal.signal(signal.SIGALRM, previous)
    return best


# A literal a pattern starts at, then one long run behind it.
ADVERSARIAL_PREFIXED = [
    ("?", "token."), ("&", "secret-"), ("?t", "a"), ("password=", "a."), ('password="', "a"),
    ("token: '", "a "), ("https://", "a."), ("https://", "a "), ("a", "a."),
    ("Bearer ", "a"), ("Authorization: ", "a"), ("cookie=", "a;"), ("'/", "a "),
    ('"/', "a'"), ("'/", "a\\"), ("/", "a/"), ("~/", "a:"), ("C:\\", "a"),
    ("\\\\", "a\\"), ("file://", "a"), ("eyJ", "a-"), ("eyJaaaa.", "a"), ("sk-", "a"),
    ("AKIA", "A"),
]


@pytest.mark.parametrize(
    "text",
    [
        *((unit * (ADVERSARIAL_BYTES // len(unit) + 1))[:ADVERSARIAL_BYTES]
          for unit in ADVERSARIAL_UNITS),
        *((prefix + unit * (ADVERSARIAL_BYTES // len(unit)))[:ADVERSARIAL_BYTES]
          for prefix, unit in ADVERSARIAL_PREFIXED),
    ],
    ids=[*ADVERSARIAL_UNITS, *(prefix + unit for prefix, unit in ADVERSARIAL_PREFIXED)],
)
def test_every_pattern_is_linear_on_adversarial_input(text):
    slow = []
    for index, (pattern, replacement) in enumerate(redaction._PATTERNS):
        seconds = _best_of(3, lambda: pattern.sub(replacement, text))
        if seconds > PATTERN_BUDGET_S:
            slow.append((index, pattern.pattern[:60], round(seconds * 1000, 1)))
    seconds = _best_of(3, lambda: redact(text))
    if seconds > PATTERN_BUDGET_S:
        slow.append(("redact_error_text", "", round(seconds * 1000, 1)))
    assert slow == [], slow
