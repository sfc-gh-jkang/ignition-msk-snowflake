"""Stream Ignition tag changes into Snowflake with the Snowpipe Streaming REST API.

Runs inside an Ignition 8.1 (or 8.3) gateway with no additional modules: it uses only Jython,
the Java standard library (HttpURLConnection) and Ignition system functions. Tag value-change scripts call enqueue();
a 5-second ticker tag calls flush(), which POSTs one gzip NDJSON batch to the table's Elastic
Channel endpoint.

REST API: https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-rest-api
Key-pair auth: https://docs.snowflake.com/en/user-guide/key-pair-auth

Delivery is at-least-once. Every row carries EVENT_ID (tag path + source timestamp) so the
Dynamic Table downstream can deduplicate.

Configuration comes from environment variables of the gateway process:
  SNOWFLAKE_ACCOUNT        org-account identifier, e.g. MYORG-MYACCOUNT
  SNOWFLAKE_USER           service user with the RSA public key registered
  SNOWFLAKE_DATABASE / SNOWFLAKE_SCHEMA / SNOWFLAKE_TABLE
  SNOWFLAKE_PRIVATE_KEY_FILE  unencrypted PKCS#8 PEM, default /run/secrets/rsa_key.p8
  SNOWSTREAM_SITE / SNOWSTREAM_LINE   values written to the SITE and LINE columns
  SNOWSTREAM_MAX_BUFFER    rows held in memory while Snowflake is unreachable (default 100000)
"""
from java.lang import System as JSystem, String
from java.util import Base64, UUID
from java.util.concurrent import ConcurrentLinkedQueue
from java.util.concurrent.locks import ReentrantLock
from java.io import ByteArrayOutputStream
from java.util.zip import GZIPOutputStream
from java.security import KeyFactory, Signature, MessageDigest
from java.security.spec import PKCS8EncodedKeySpec, X509EncodedKeySpec
from java.security.interfaces import RSAPrivateCrtKey
from java.security.spec import RSAPublicKeySpec
from java.nio.file import Files, Paths
from java.net import URL
from java.lang import Throwable
from java.io import ByteArrayOutputStream as _Buf
import time
import jarray

logger = system.util.getLogger("snowstream")
MAX_REQUEST_BYTES = 4 * 1024 * 1024  # REST limit, measured after compression
TOKEN_TTL_SECONDS = 50 * 60


def _env(name, default=None):
    value = JSystem.getenv(name)
    return value if value else default


def _state():
    """Shared state that survives script-module reloads."""
    g = system.util.getGlobals()
    if "snowstream" not in g:
        g["snowstream"] = {"queue": ConcurrentLinkedQueue(), "lock": ReentrantLock(),
                           "token": None, "token_at": 0, "ingest_host": None, "dropped": 0}
    return g["snowstream"]


def _b64url(raw):
    return Base64.getUrlEncoder().withoutPadding().encodeToString(raw)


def _control_host():
    # TLS certificates do not cover underscores, so the REST host uses dashes.
    return (_env("SNOWFLAKE_ACCOUNT").lower().replace("_", "-") + ".snowflakecomputing.com")


def _jwt():
    account = _env("SNOWFLAKE_ACCOUNT").upper()
    user = _env("SNOWFLAKE_USER").upper()
    pem = String(Files.readAllBytes(Paths.get(_env("SNOWFLAKE_PRIVATE_KEY_FILE", "/run/secrets/rsa_key.p8"))))
    body = "".join(line for line in str(pem).splitlines() if "-----" not in line)
    kf = KeyFactory.getInstance("RSA")
    private_key = kf.generatePrivate(PKCS8EncodedKeySpec(Base64.getDecoder().decode(body)))
    public_key = kf.generatePublic(RSAPublicKeySpec(private_key.getModulus(), private_key.getPublicExponent()))
    fingerprint = "SHA256:" + Base64.getEncoder().encodeToString(
        MessageDigest.getInstance("SHA-256").digest(public_key.getEncoded()))
    now = int(time.time())
    header = _b64url(String('{"alg":"RS256","typ":"JWT"}').getBytes("UTF-8"))
    claims = system.util.jsonEncode({"iss": "%s.%s.%s" % (account, user, fingerprint),
                                     "sub": "%s.%s" % (account, user), "iat": now, "exp": now + 3540})
    payload = _b64url(String(claims).getBytes("UTF-8"))
    signer = Signature.getInstance("SHA256withRSA")
    signer.initSign(private_key)
    signer.update(String(header + "." + payload).getBytes("UTF-8"))
    return header + "." + payload + "." + _b64url(signer.sign())


class _Resp(object):
    def __init__(self, status, text):
        self.statusCode, self.text, self.good = status, text, 200 <= status < 300


def _http(method, url, headers, body=None):
    """HTTP/1.1 via HttpURLConnection. system.net.httpClient was seen to accept a request
    server-side and then fail with "no statuscode in response", which caused resends."""
    conn = URL(url).openConnection()
    conn.setRequestMethod(method)
    conn.setConnectTimeout(15000)
    conn.setReadTimeout(30000)
    for k, v in headers.items():
        conn.setRequestProperty(k, v)
    if body is not None:
        conn.setDoOutput(True)
        out = conn.getOutputStream()
        out.write(body if not isinstance(body, basestring) else String(body).getBytes("UTF-8"))
        out.close()
    status = conn.getResponseCode()
    stream = conn.getInputStream() if status < 400 else conn.getErrorStream()
    buf = _Buf()
    if stream is not None:
        chunk = jarray.zeros(8192, "b")
        n = stream.read(chunk)
        while n > 0:
            buf.write(chunk, 0, n)
            n = stream.read(chunk)
        stream.close()
    return _Resp(status, String(buf.toByteArray(), "UTF-8").toString())


def _scoped_token(state):
    if state["token"] and time.time() - state["token_at"] < TOKEN_TTL_SECONDS:
        return state["token"]
    jwt = _jwt()
    control = _control_host()
    resp = _http("GET", "https://%s/v2/streaming/hostname" % control,
                      headers={"Authorization": "Bearer " + jwt, "Accept": "application/json",
                               "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT"})
    if not resp.good:
        raise Exception("hostname lookup failed: %s %s" % (resp.statusCode, resp.text))
    # The body is plain text or {"hostname": ...} depending on the client; accept both.
    # Underscores must become dashes for TLS.
    text = resp.text.strip()
    ingest = (system.util.jsonDecode(text)["hostname"] if text.startswith("{") else text).replace("_", "-")
    resp = _http("POST", "https://%s/oauth/token" % control,
                       body="grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&scope=" + ingest,
                       headers={"Authorization": "Bearer " + jwt, "Accept": "application/json",
                                "Content-Type": "application/x-www-form-urlencoded"})
    if not resp.good:
        raise Exception("token exchange failed: %s %s" % (resp.statusCode, resp.text))
    state["token"], state["token_at"], state["ingest_host"] = resp.text.strip(), time.time(), ingest
    return state["token"]


def enqueue(tagPath, qv, initialChange=False):
    """Call from a tag value-change script: snowstream.enqueue(tagPath, currentValue, initialChange)."""
    if initialChange:
        return
    state = _state()
    value = qv.value
    if isinstance(value, bool):
        value = 1.0 if value else 0.0
    ts = qv.timestamp.getTime()
    path = str(tagPath)
    row = system.util.jsonEncode({
        "EVENT_ID": "%s|%d" % (path, ts),
        "SITE": _env("SNOWSTREAM_SITE", "DEMO_SITE"),
        "LINE": _env("SNOWSTREAM_LINE", "LINE1"),
        "TAG_PATH": path,
        "VALUE": value,
        "QUALITY": str(qv.quality.name if hasattr(qv.quality, "name") else qv.quality),
        "EVENT_TS_MS": ts,
    })
    queue = state["queue"]
    queue.add(row)
    limit = int(_env("SNOWSTREAM_MAX_BUFFER", "100000"))
    while queue.size() > limit:  # bounded memory during long outages: drop oldest
        queue.poll()
        state["dropped"] += 1


def _gzip(text):
    out = ByteArrayOutputStream()
    gz = GZIPOutputStream(out)
    gz.write(String(text).getBytes("UTF-8"))
    gz.close()
    return out.toByteArray()


def flush():
    """Call from the ticker tag. Sends whatever is buffered; rows stay queued on failure."""
    state = _state()
    lock = state["lock"]
    if not lock.tryLock():
        return  # a previous flush is still running
    try:
        queue = state["queue"]
        while not queue.isEmpty():
            batch = []
            size = 0
            it = queue.iterator()
            while it.hasNext() and size < MAX_REQUEST_BYTES:  # uncompressed cap, so always under the limit
                row = it.next()
                batch.append(row)
                size += len(row) + 1
            body = _gzip("\n".join(batch))
            if not _post(state, body):
                return
            for _ in batch:
                queue.poll()
        if state["dropped"]:
            logger.warn("dropped %d rows while Snowflake was unreachable" % state["dropped"])
            state["dropped"] = 0
    except (Exception, Throwable), e:
        logger.warn("flush failed, will retry on next tick: %s" % e)
    finally:
        lock.unlock()


def _post(state, body):
    request_id = str(UUID.randomUUID())
    for attempt in range(3):
        token = _scoped_token(state)
        url = ("https://%s/v2/streaming/data/databases/%s/schemas/%s/tables/%s/rows?requestId=%s&retryCount=%d"
               % (state["ingest_host"], _env("SNOWFLAKE_DATABASE"), _env("SNOWFLAKE_SCHEMA"),
                  _env("SNOWFLAKE_TABLE"), request_id, attempt))
        try:
            resp = _http("POST", url, body=body, headers={
                "Authorization": "Bearer " + token, "Content-Type": "application/x-ndjson",
                "Accept": "application/json",
                "Content-Encoding": "gzip"})
        except (Exception, Throwable), e:
            logger.warn("append attempt %d failed: %s" % (attempt, e))
            time.sleep(2 ** attempt)
            continue
        if resp.good:
            return True
        if resp.statusCode == 401:
            state["token"] = None  # refresh and retry with the same requestId
        logger.warn("append attempt %d returned %s: %s" % (attempt, resp.statusCode, resp.text[:300]))
        time.sleep(2 ** attempt)
    return False
