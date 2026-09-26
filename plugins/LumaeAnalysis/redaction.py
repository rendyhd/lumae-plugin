"""Credential-safe display of stored error text (LUM-021, plan P3-10).

Workflow state tables keep ``last_error`` as free text, often ``str(exc)``:
provider URLs with Subsonic ``t``/``s``/``p`` query parameters, libpq DSNs,
HTTP headers and file paths. ``redact_error_text`` keeps the text readable and
masks those shapes, so a diagnostics page or a log line can show it.

Live query errors do not come here: ``database_state`` replaces them with a
fixed message, an error class and the SQLSTATE, and never shows their text.

This module has no plugin imports so that any module can use it, including
failure diagnostics recorded by analysis workers.

Cost. The patterns run on every health poll and settings render, so each one
must stay linear in its input. A pattern starts only at the beginning of a
run of its own characters (a negative lookbehind) or at a literal, every scan
that can fail is bounded (a key is at most 64 characters) or ends at a
character it excludes, and no quantified run is followed by another run over
the same characters that could split it differently. ``tests/plugins/
test_redaction.py`` times every pattern on 100 KB adversarial inputs.
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
_SECRET_WORD = "(?i:" + "|".join(_SECRET_WORDS) + ")"
# Characters of a key name; a key is at most 64 of them.
_KEY = r"[A-Za-z0-9_.-]"
# A key name that contains a secret word, checked ahead of the key itself: the
# look is bounded by the key length, and starts only where a key starts.
_SECRET_KEY_AHEAD = r"(?=" + _KEY + r"{0,64}?" + _SECRET_WORD + r")"
# Query parameters matched exactly: Subsonic (Navidrome) sends the password as
# ``p``, or a token ``t`` with its salt ``s``, on every request.
_QUERY_ONLY = r"(?i:t|s|p|pass|sig|key|auth)="
_MASKED = r"\[(?:redacted|path)\]"
# Words that follow an authentication scheme name in prose, not credentials:
# "Basic authentication required".
_SCHEME_PROSE = (
    r"(?:auth\w*|credentials?|required|realm|challenge|scheme|header|token|"
    r"tokens|expired|invalid|missing)\b"
)
# The rest of an unquoted file path. Music file names contain spaces, so a
# path runs to the next ": ", quote, " (" or the end rather than to the next
# space. A quoted path is matched whole first.
_PATH_REST = r"(?:(?! \()[^\"'<>|:]|:(?!\s|$))*"
_PATH_START = r"(?:/|~/|[A-Za-z]:\\|\\\\|file://)"

_PATTERNS = (
    # Quoted file paths, whole, before anything can split them:
    # FileNotFoundError and PermissionError always quote the path (repr), which
    # may hold spaces, " (", ": " and, inside double quotes, apostrophes. The
    # scan ends at the next unescaped quote of the same kind.
    (
        re.compile(r"'" + _PATH_START + r"(?:[^'\\]|\\.)*'"),
        "'" + PATH + "'",
    ),
    (
        re.compile(r'"' + _PATH_START + r'(?:[^"\\]|\\.)*"'),
        '"' + PATH + '"',
    ),
    # Authorization headers: the scheme and its credentials.
    (
        re.compile(
            r"(?i)\b((?:proxy-)?authorization)(\s*[:=]\s*[\"']?)"
            r"(?:[A-Za-z][A-Za-z0-9-]{0,31} )?[^\s\"',;]+"
        ),
        r"\1\2" + REDACTED,
    ),
    # Cookie headers: the whole value, every cookie of it.
    (
        re.compile(r"(?i)\b(set-cookie|cookie)(\s*[:=]\s*[\"']?)[^\"']+"),
        r"\1\2" + REDACTED,
    ),
    # An authentication scheme and its credentials: "Bearer <token>", any
    # token of 8 characters or more, letters only included.
    (
        re.compile(
            r"(?i)\b(bearer|basic|digest)( )(?!" + _SCHEME_PROSE + r")"
            r"[A-Za-z0-9._~+/=:-]{8,}"
        ),
        r"\1\2" + REDACTED,
    ),
    # Userinfo in any URL: scheme://user:password@host -> scheme://[redacted]@host.
    # One inner space is allowed: the text is whitespace-collapsed, so a line
    # break inside the userinfo ("user:\npass@") became one.
    (
        re.compile(
            r"(?i)(?<![a-z0-9+.-])([a-z][a-z0-9+.-]{0,31}://)"
            r"[^\s/?#@\"'<>]+(?: [^\s/?#@\"'<>]+)?@"
        ),
        r"\1" + REDACTED + "@",
    ),
    # Secret query parameters: ?t=..&s=..&p=.., &token=.., ?api_key=.. A
    # value stops at the next separator or "?", so a parameter after it, or a
    # nested URL's own parameters, are checked too.
    (
        re.compile(
            r"([?&;])(?=" + _QUERY_ONLY + r"|" + _KEY + r"{0,64}?" + _SECRET_WORD + r")"
            r"(" + _KEY + r"{1,64})=[^&;#?\s\"'<>]*"
        ),
        r"\1\2=" + REDACTED,
    ),
    # key=value and key: value pairs whose key names a secret, including libpq
    # keyword DSNs (password=...), environment variables (PGPASSWORD=...), JSON
    # or dict reprs ('password': '...') and escaped JSON (\"token\": \"...\").
    # Only a key naming a secret matches, so "failed: password=x" is found at
    # "password", and an unclosed quoted value is masked up to the next space.
    (
        re.compile(
            r"(?<!" + _KEY + r")" + _SECRET_KEY_AHEAD + r"(" + _KEY + r"{1,64})"
            r"(\\?[\"']?\s*[:=]\s*)(?!" + _MASKED + r")"
            r"(?:\"[^\"]*\"|'[^']*'|\\\"[^\"\\]*\\\"|\\?[\"']?[^\s,;&\"'}\])\\]+)"
        ),
        r"\1\2" + REDACTED,
    ),
    # Unquoted file paths: file:// URLs, Windows drive and UNC paths,
    # home-relative and absolute POSIX paths with at least two segments. A
    # path inside a URL follows its host, so it is not matched here.
    (re.compile(r"(?i)(?<![\w/])file://" + _PATH_REST), PATH),
    (re.compile(r"(?i)(?<![\w\\])[a-z]:\\" + _PATH_REST), PATH),
    (re.compile(r"(?<!\\)\\\\[^\s\\\"'<>|]+\\" + _PATH_REST), PATH),
    (re.compile(r"(?<![\w.:/~\\-])~/" + _PATH_REST), PATH),
    (re.compile(r"(?<![\w.:/\\-])/[^\s/\"'<>|:]+/" + _PATH_REST), PATH),
    # JSON Web Tokens.
    (
        re.compile(
            r"(?<![A-Za-z0-9_-])eyJ[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]*"
        ),
        REDACTED,
    ),
    # Well-known API key shapes (sk-/pk- keys, GitHub, GitLab, Slack, AWS).
    (
        re.compile(
            r"(?<![A-Za-z0-9_-])(?:"
            r"(?:sk|pk|rk)-[A-Za-z0-9_-]{16,}"
            r"|(?:ghp|gho|ghs|ghu|ghr|github_pat|glpat|xox[abprs])[-_][A-Za-z0-9_-]{16,}"
            r"|AKIA[0-9A-Z]{16}(?![A-Za-z0-9_-]))"
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
    category) is returned unchanged. Anything else has its whitespace
    collapsed (a multi-line error becomes one line), then credentials, tokens
    and file paths masked, and its length capped. Redacting redacted text
    changes nothing.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text in safe_codes:
        return text
    # Collapse first, so a secret split across lines is matched whole.
    text = _WHITESPACE.sub(" ", text[:_MAX_SCANNED]).strip()
    for pattern, replacement in _PATTERNS:
        text = pattern.sub(replacement, text)
    return _cap(text)


def _cap(text):
    """Cap the length, never inside a mask (a cut "[redac" would be re-masked)."""
    if len(text) <= MAX_ERROR_TEXT_CHARS:
        return text
    head = text[: MAX_ERROR_TEXT_CHARS - 1]
    bracket = head.rfind("[")
    if bracket >= 0 and any(
        mask.startswith(head[bracket:]) and mask != head[bracket:]
        for mask in (REDACTED, PATH)
    ):
        head = head[:bracket]
    return head.rstrip() + "…"
