"""Credential-safe display of stored error text (LUM-021, plan P3-10).

Workflow state tables keep ``last_error`` as free text, often ``str(exc)``:
provider URLs with Subsonic ``t``/``s``/``p`` query parameters, libpq DSNs,
HTTP headers and file paths. ``redact_error_text`` keeps the text readable and
masks those shapes, so a diagnostics page or a log line can show it.

Live query errors do not come here: ``database_state`` replaces them with a
fixed message, an error class and the SQLSTATE, and never shows their text.

This module has no plugin imports so that any module can use it, including
failure diagnostics recorded by analysis workers.
"""

import re


REDACTED = "[redacted]"
PATH = "[path]"
# Characters shown. A secret cut by this cap was already masked, because the
# patterns run on the longer ``_MAX_SCANNED`` prefix first.
MAX_ERROR_TEXT_CHARS = 300
_MAX_SCANNED = 4000

# Key names that carry a secret, as ``key=value``, ``key: value`` or a query
# parameter. A name only has to contain one (``PGPASSWORD``, ``client_secret``).
_SECRET_WORDS = (
    "password", "passwd", "pwd", "passphrase", "secret", "token",
    "api[_-]?key", "access[_-]?key", "private[_-]?key", "auth[_-]?key",
    "credentials?", "signature", "session[_-]?id", "jwt", "authorization",
    "cookie",
)
# Query parameters only: Subsonic (Navidrome) sends the password as ``p``, or
# a token ``t`` with its salt ``s``, on every request.
_QUERY_ONLY = ("t", "s", "p", "pass", "sig", "key", "auth")
_KEY = r"[A-Za-z0-9_.-]*?(?:" + "|".join(_SECRET_WORDS) + r")[A-Za-z0-9_.-]*"
_CREDENTIAL = r"(?=[A-Za-z0-9._~+/=:-]*[0-9._~+/=-])[A-Za-z0-9._~+/=:-]{6,}"
_MASKED = r"\[(?:redacted|path)\]"
# The rest of a file path. Music file names contain spaces, so a path runs to
# the next ": ", quote, " (" or line end rather than to the next space.
_PATH_REST = r"(?:(?! \()[^\"'<>|:\r\n]|:(?![\s]|$))*"

_PATTERNS = (
    # Authorization, Proxy-Authorization and cookie headers: the whole value.
    (
        re.compile(
            r"(?i)\b((?:proxy-)?authorization|set-cookie|cookie)"
            r"(\s*[:=]\s*[\"']?)[^\"'\r\n,;]+"
        ),
        r"\1\2" + REDACTED,
    ),
    # An authentication scheme and its credentials: "Bearer <token>".
    (
        re.compile(r"(?i)\b(bearer|basic|digest)(\s+)" + _CREDENTIAL),
        r"\1\2" + REDACTED,
    ),
    # Userinfo in any URL: scheme://user:password@host -> scheme://[redacted]@host
    (
        re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://)[^\s/?#@\"'<>]+@"),
        r"\1" + REDACTED + "@",
    ),
    # Secret query parameters: ?t=..&s=..&p=.., &token=.., ?api_key=..
    (
        re.compile(
            r"(?i)([?&;](?:" + "|".join(_QUERY_ONLY) + r"|" + _KEY + r")=)"
            r"[^&#\s\"'<>]*"
        ),
        r"\1" + REDACTED,
    ),
    # key=value and key: value pairs whose key names a secret, including libpq
    # keyword DSNs (password=...), environment variables (PGPASSWORD=...) and
    # JSON or dict reprs ('password': '...').
    (
        re.compile(
            r"(?i)(?<![A-Za-z0-9])(" + _KEY + r")([\"']?\s*[:=]\s*)"
            r"(?!" + _MASKED + r")(?:\"[^\"]*\"|'[^']*'|[^\s,;&\"'}\])]+)"
        ),
        r"\1\2" + REDACTED,
    ),
    # file:// URLs.
    (re.compile(r"(?i)\bfile://" + _PATH_REST), PATH),
    # Windows drive and UNC paths.
    (re.compile(r"(?i)\b[a-z]:\\" + _PATH_REST), PATH),
    (re.compile(r"\\\\[^\s\\\"'<>|]+\\" + _PATH_REST), PATH),
    # Home-relative and absolute POSIX paths with at least two segments. A
    # path inside a URL follows its host, so it is not matched here.
    (re.compile(r"(?<![\w.:/~\\-])~/" + _PATH_REST), PATH),
    (re.compile(r"(?<![\w.:/\\-])/[^\s/\"'<>|:]+/" + _PATH_REST), PATH),
    # JSON Web Tokens.
    (
        re.compile(r"\beyJ[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]*"),
        REDACTED,
    ),
    # Well-known API key shapes (sk-/pk- keys, GitHub, GitLab, Slack, AWS).
    (
        re.compile(
            r"\b(?:sk|pk|rk)-[A-Za-z0-9_-]{16,}"
            r"|\b(?:ghp|gho|ghs|ghu|ghr|github_pat|glpat|xox[abprs])[-_][A-Za-z0-9_-]{16,}"
            r"|\bAKIA[0-9A-Z]{16}\b"
        ),
        REDACTED,
    ),
    # Other long opaque tokens: 32+ characters of hex or base64 with letters
    # and digits. Hyphens and underscores split the run, so UUIDs and
    # identifiers stay readable.
    (
        re.compile(
            r"(?<![A-Za-z0-9+/=])(?=[A-Za-z0-9+/=]*[0-9])(?=[A-Za-z0-9+/=]*[A-Za-z])"
            r"[A-Za-z0-9+/=]{32,}"
        ),
        REDACTED,
    ),
)
_WHITESPACE = re.compile(r"\s+")


def redact_error_text(value, safe_codes=()):
    """Return ``value`` as display-safe text, or None when it is empty.

    A value that equals one of ``safe_codes`` (for example a profile retry
    category) is returned unchanged. Anything else has credentials, tokens and
    file paths masked, whitespace collapsed and its length capped.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text in safe_codes:
        return text
    text = text[:_MAX_SCANNED]
    for pattern, replacement in _PATTERNS:
        text = pattern.sub(replacement, text)
    text = _WHITESPACE.sub(" ", text).strip()
    if len(text) > MAX_ERROR_TEXT_CHARS:
        text = text[: MAX_ERROR_TEXT_CHARS - 1].rstrip() + "…"
    return text
