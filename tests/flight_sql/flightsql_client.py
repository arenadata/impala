# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Flight SQL over plain pyarrow.flight, for the end-to-end tests.

pyarrow has no Flight SQL client, so the few commands the tests need are encoded here
(Flight SQL messages are small protobufs wrapped in google.protobuf.Any). Using raw
Flight also lets the tests see status codes, headers and tickets that ADBC hides.
"""

import base64
import os
import subprocess

import pyarrow as pa
import pyarrow.flight as flight

_TYPE_PREFIX = "type.googleapis.com/arrow.flight.protocol.sql."

# Flight SQL SqlInfo ids (FlightSql.proto)
SQL_INFO_SERVER_NAME = 0
SQL_INFO_SERVER_VERSION = 1
SQL_INFO_SERVER_ARROW_VERSION = 2
SQL_INFO_SERVER_READ_ONLY = 3


# ---------------------------------------------------------------- protobuf


def _varint(n):
  out = bytearray()
  n &= (1 << 64) - 1
  while True:
    b = n & 0x7F
    n >>= 7
    if n:
      out.append(b | 0x80)
    else:
      out.append(b)
      return bytes(out)


def _bytes_field(number, data):
  if isinstance(data, str):
    data = data.encode("utf-8")
  return _varint(number << 3 | 2) + _varint(len(data)) + data


def _int_field(number, value):
  return _varint(number << 3) + _varint(value)


def message(*fields):
  """A protobuf message from (number, value) pairs; None values are left out.

  str / bytes values are length-delimited fields, bool and int values varints, and a list
  value repeats its field.
  """
  out = b""
  for number, value in fields:
    if value is None:
      continue
    for v in (value if isinstance(value, list) else [value]):
      if isinstance(v, bool):
        out += _int_field(number, int(v))
      elif isinstance(v, int):
        out += _int_field(number, v)
      else:
        out += _bytes_field(number, v)
  return out


def any_message(name, body=b""):
  """google.protobuf.Any of the Flight SQL message 'name' (e.g. 'CommandGetTables')."""
  return message((1, _TYPE_PREFIX + name), (2, body))


def parse(data):
  """Fields of a protobuf message: {number: [values]}, varints as int, others as bytes."""
  fields = {}
  i = 0
  while i < len(data):
    key, i = _read_varint(data, i)
    number, wire = key >> 3, key & 7
    if wire == 0:
      value, i = _read_varint(data, i)
    elif wire == 2:
      length, i = _read_varint(data, i)
      value, i = data[i:i + length], i + length
    elif wire == 1:
      value, i = data[i:i + 8], i + 8
    elif wire == 5:
      value, i = data[i:i + 4], i + 4
    else:
      raise ValueError("unsupported protobuf wire type %d" % wire)
    fields.setdefault(number, []).append(value)
  return fields


def _read_varint(data, i):
  shift = result = 0
  while True:
    b = data[i]
    i += 1
    result |= (b & 0x7F) << shift
    if not b & 0x80:
      return result, i
    shift += 7


def unpack_any(data):
  """(type name without the Flight SQL prefix, value bytes) of an Any."""
  fields = parse(data)
  type_url = fields[1][0].decode()
  return type_url.rsplit(".", 1)[-1], fields.get(2, [b""])[0]


# ---------------------------------------------------------------- authentication


class HeaderCapture(flight.ClientMiddlewareFactory):
  """Remembers the response headers of the last call (the server's bearer token)."""

  def __init__(self):
    self.headers = {}

  def start_call(self, info):
    capture = self

    class _Middleware(flight.ClientMiddleware):
      def received_headers(self, headers):
        # keep what earlier calls returned (a DoGet after GetFlightInfo sends no token)
        capture.headers.update({k.lower(): v for k, v in headers.items()})

    return _Middleware()


def basic_header(user, password):
  token = base64.b64encode(("%s:%s" % (user, password)).encode()).decode()
  return "Basic " + token


class Kerberos:
  """Client side Kerberos against the test server's KDC: kinit from a keytab into a
  private ticket cache, SPNEGO tokens for HTTP/localhost from it."""

  def __init__(self, info, work_dir):
    self.info = info
    self.work_dir = work_dir

  def ticket_cache(self, user):
    cache = os.path.join(self.work_dir, "krb5cc_" + user)
    principal = "%s@%s" % (user, self.info["realm"])
    env = dict(os.environ, KRB5_CONFIG=self.info["krb5_conf"], KRB5CCNAME="FILE:" + cache)
    subprocess.run(["kinit", "-kt", self.info["keytabs"][user], principal], env=env,
                   check=True, capture_output=True, timeout=30)
    return cache

  def negotiate_header(self, user):
    """'Negotiate <token>' of a fresh SPNEGO context for the service."""
    import gssapi  # only needed with Kerberos
    cache = self.ticket_cache(user)
    os.environ["KRB5_CONFIG"] = self.info["krb5_conf"]
    creds = gssapi.Credentials(usage="initiate", store={"ccache": "FILE:" + cache})
    target = gssapi.Name(self.info["service_principal"],
                         name_type=gssapi.NameType.kerberos_principal)
    spnego = gssapi.raw.OID.from_int_seq("1.3.6.1.5.5.2")
    ctx = gssapi.SecurityContext(name=target, creds=creds, mech=spnego, usage="initiate")
    token = ctx.step()
    return "Negotiate " + base64.b64encode(token).decode()


# ---------------------------------------------------------------- client


class FlightSqlClient:
  """Raw Flight SQL client. 'headers' go on every call (authorization or the user header
  of auth 'none'); after login() they hold the bearer token instead."""

  def __init__(self, info, headers=None):
    self.info = info
    self.capture = HeaderCapture()
    scheme = "grpc+tls" if info["tls"] else "grpc+tcp"
    kwargs = {"middleware": [self.capture]}
    if info["tls"]:
      with open(info["ca"], "rb") as f:
        kwargs["tls_root_certs"] = f.read()
    self.client = flight.FlightClient(
        "%s://%s:%d" % (scheme, info["host"], info["port"]), **kwargs)
    self.headers = dict(headers or {})

  def close(self):
    self.client.close()

  def __enter__(self):
    return self

  def __exit__(self, *exc):
    self.close()

  def options(self, timeout=60):
    return flight.FlightCallOptions(
        timeout=timeout,
        headers=[(k.encode(), v.encode()) for k, v in self.headers.items()])

  def login(self):
    """One call with the current headers, then use the bearer token it returned."""
    self.capture.headers = {}
    # a single call: a SPNEGO token is good for one authentication only
    self.info_for("CommandGetSqlInfo", message((1, [SQL_INFO_SERVER_NAME])))
    auth = self.capture.headers.get("authorization")
    if auth:
      value = auth[0] if isinstance(auth, list) else auth
      self.headers = {"authorization": value.decode() if isinstance(value, bytes)
                      else value}
    return self.headers.get("authorization")

  # -- generic

  def info_for(self, name, body=b""):
    descriptor = flight.FlightDescriptor.for_command(any_message(name, body))
    return self.client.get_flight_info(descriptor, self.options())

  def read(self, info, endpoint=0):
    reader = self.client.do_get(info.endpoints[endpoint].ticket, self.options())
    return reader.read_all()

  def read_batches(self, info):
    """Record batches as sent (to count them), and the stream's schema."""
    reader = self.client.do_get(info.endpoints[0].ticket, self.options())
    batches = []
    while True:
      try:
        chunk = reader.read_chunk()
      except StopIteration:
        break
      batches.append(chunk.data)
    return reader.schema, batches

  def fetch(self, name, body=b""):
    return self.read(self.info_for(name, body))

  def action(self, name, body):
    results = list(self.client.do_action(
        flight.Action(name, any_message("Action" + name + "Request", body)),
        self.options()))
    return [unpack_any(r.body.to_pybytes())[1] for r in results]

  # -- statements

  def execute(self, sql):
    return self.info_for("CommandStatementQuery", message((1, sql)))

  def query(self, sql):
    return self.read(self.execute(sql))

  def update(self, sql):
    return self._put(any_message("CommandStatementUpdate", message((1, sql))))

  def _put(self, command):
    descriptor = flight.FlightDescriptor.for_command(command)
    writer, reader = self.client.do_put(descriptor, pa.schema([]), self.options())
    writer.done_writing()
    buf = reader.read()
    writer.close()
    if buf is None:
      return None
    return parse(buf.to_pybytes()).get(1, [0])[0]  # DoPutUpdateResult.record_count

  # -- prepared statements

  def prepare(self, sql):
    """(handle, dataset schema or None) of a new prepared statement."""
    result = parse(self.action("CreatePreparedStatement", message((1, sql)))[0])
    handle = result[1][0]
    schema_bytes = result.get(2, [b""])[0]
    schema = pa.ipc.read_schema(pa.py_buffer(schema_bytes)) if schema_bytes else None
    return handle, schema

  def execute_prepared(self, handle):
    return self.info_for("CommandPreparedStatementQuery", message((1, handle)))

  def update_prepared(self, handle):
    return self._put(any_message("CommandPreparedStatementUpdate", message((1, handle))))

  def close_prepared(self, handle):
    self.action("ClosePreparedStatement", message((1, handle)))

  # -- metadata

  def sql_info(self, ids):
    table = self.fetch("CommandGetSqlInfo", message((1, list(ids))))
    return {row["info_name"]: row["value"] for row in table.to_pylist()}

  def catalogs(self):
    return self.fetch("CommandGetCatalogs")

  def schemas(self, catalog=None, pattern=None):
    return self.fetch("CommandGetDbSchemas", message((1, catalog), (2, pattern)))

  def tables(self, schema=None, table=None, types=None, include_schema=False):
    return self.fetch("CommandGetTables", message(
        (2, schema), (3, table), (4, list(types) if types else None),
        (5, include_schema or None)))

  def table_types(self):
    return self.fetch("CommandGetTableTypes")

  def primary_keys(self, schema, table):
    return self.fetch("CommandGetPrimaryKeys", message((2, schema), (3, table)))

  def imported_keys(self, schema, table):
    return self.fetch("CommandGetImportedKeys", message((2, schema), (3, table)))

  def exported_keys(self, schema, table):
    return self.fetch("CommandGetExportedKeys", message((2, schema), (3, table)))

  def cross_reference(self, pk_schema, pk_table, fk_schema, fk_table):
    return self.fetch("CommandGetCrossReference", message(
        (2, pk_schema), (3, pk_table), (5, fk_schema), (6, fk_table)))

  def xdbc_type_info(self, data_type=None):
    return self.fetch("CommandGetXdbcTypeInfo", message((1, data_type)))
