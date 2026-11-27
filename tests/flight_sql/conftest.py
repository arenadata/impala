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

"""Fixtures of the Flight SQL end-to-end tests.

The tests run against FlightSqlTestServer (fe/src/test/java/.../service/flight): the real
Flight SQL endpoint of impalad with a fake HS2 behind it, so no cluster is needed. Two
servers are started per session: one without authentication, one with LDAP and Kerberos
(in-process directory and KDC). Tests that take the 'client' or 'adbc' fixture run once
per login mode: none (user header), ldap (Basic) and kerberos (SPNEGO).
"""

import json
import os
import subprocess
import tempfile
import time

import pytest

from flightsql_client import FlightSqlClient, Kerberos, basic_header

MODES = ["none", "ldap", "kerberos"]
SERVER_CLASS = "org.apache.impala.service.flight.FlightSqlTestServer"
START_TIMEOUT_S = 120

# The two users of each login mode: (name the server sees, credentials key).
USERS = {
    "none": ["alice", "bob"],
    "ldap": ["Test1Ldap", "Test2Ldap"],
    "kerberos": ["alice", "bob"],
}


def pytest_addoption(parser):
  here = os.path.dirname(os.path.abspath(__file__))
  parser.addoption("--impala-home",
                   default=os.environ.get("IMPALA_HOME",
                                          os.path.dirname(os.path.dirname(here))),
                   help="Impala checkout with a built frontend (fe/target)")
  parser.addoption("--java", default=os.environ.get("FLIGHT_TEST_JAVA", "java"),
                   help="java executable for FlightSqlTestServer")


class Server:
  """A running FlightSqlTestServer: 'info' is its info file (port, CA, users, ...)."""

  def __init__(self, impala_home, java, auth):
    fe = os.path.join(impala_home, "fe", "target")
    cp_file = os.path.join(fe, "test-classpath.txt")
    if not os.path.isfile(cp_file):
      pytest.exit("%s missing: build the frontend first (mvn install in fe)" % cp_file, 2)
    with open(cp_file) as f:
      classpath = ":".join([os.path.join(fe, "classes"),
                            os.path.join(fe, "test-classes"), f.read().strip()])
    # FLIGHT_TEST_WORK_DIR: where the servers' files and logs go (CI keeps them)
    parent = os.environ.get("FLIGHT_TEST_WORK_DIR")
    if parent:
      os.makedirs(parent, exist_ok=True)
    self.work_dir = tempfile.mkdtemp(prefix="flight-sql-%s-" % auth, dir=parent)
    info_file = os.path.join(self.work_dir, "info.json")
    self.log_path = os.path.join(self.work_dir, "server.log")
    self._log = open(self.log_path, "w")
    self.proc = subprocess.Popen(
        [java, "--add-opens=java.base/java.nio=ALL-UNNAMED", "-cp", classpath,
         SERVER_CLASS, "--info=" + info_file, "--auth=" + auth,
         "--work-dir=" + self.work_dir],
        stdin=subprocess.PIPE, stdout=self._log, stderr=subprocess.STDOUT)
    deadline = time.time() + START_TIMEOUT_S
    while not os.path.exists(info_file):
      if self.proc.poll() is not None or time.time() > deadline:
        self.stop()
        raise RuntimeError("FlightSqlTestServer --auth=%s did not start:\n%s"
                           % (auth, self.log_tail()))
      time.sleep(0.2)
    with open(info_file) as f:
      self.info = json.load(f)

  def log_tail(self, lines=60):
    with open(self.log_path) as f:
      return "".join(f.readlines()[-lines:])

  def stop(self):
    if self.proc.poll() is None:
      self.proc.stdin.close()  # the server exits when its stdin closes
      try:
        self.proc.wait(timeout=30)
      except subprocess.TimeoutExpired:
        self.proc.kill()
        self.proc.wait()
    self._log.close()


@pytest.fixture(scope="session")
def servers(request):
  """Server per kind ('none', 'secure'), started on first use."""
  started = {}

  def get(kind):
    if kind not in started:
      started[kind] = Server(request.config.getoption("--impala-home"),
                             request.config.getoption("--java"),
                             "none" if kind == "none" else "all")
    return started[kind]

  yield get
  for server in started.values():
    server.stop()


def server_for(servers, mode):
  return servers("none" if mode == "none" else "secure")


@pytest.fixture(scope="session")
def data(servers):
  """What the fake HS2 serves (FlightTestData.describe())."""
  return servers("none").info["data"]


def login_headers(server, mode, user):
  """Headers of a first call as 'user' in 'mode'."""
  info = server.info
  if mode == "none":
    return {info["user_header"]: user}
  if mode == "ldap":
    return {"authorization": basic_header(user, info["ldap_users"][user])}
  return {"authorization": Kerberos(info, server.work_dir).negotiate_header(user)}


@pytest.fixture(params=MODES)
def mode(request):
  return request.param


@pytest.fixture
def users(mode):
  """(first, second) user of the mode, as the server names them."""
  return USERS[mode]


@pytest.fixture
def connect(servers, mode):
  """connect(user=None, login=True): raw client as 'user' (default: the mode's first)."""
  clients = []

  def make(user=None, login=True):
    server = server_for(servers, mode)
    client = FlightSqlClient(server.info,
                             login_headers(server, mode, user or USERS[mode][0]))
    if login and mode != "none":
      client.login()
    clients.append(client)
    return client

  yield make
  for c in clients:
    c.close()


@pytest.fixture
def client(connect):
  return connect()


@pytest.fixture
def adbc(servers, mode):
  """adbc(user=None): ADBC Flight SQL DB-API connection as 'user'."""
  import adbc_driver_flightsql.dbapi as dbapi

  connections = []

  def make(user=None):
    server = server_for(servers, mode)
    info = server.info
    user = user or USERS[mode][0]
    scheme = "grpc+tls" if info["tls"] else "grpc+tcp"
    kwargs = {}
    if info["tls"]:
      with open(info["ca"]) as f:
        kwargs["adbc.flight.sql.client_option.tls_root_certs"] = f.read()
    if mode == "none":
      kwargs["adbc.flight.sql.rpc.call_header." + info["user_header"]] = user
    elif mode == "ldap":
      kwargs["username"] = user
      kwargs["password"] = info["ldap_users"][user]
    else:
      kwargs["adbc.flight.sql.authorization_header"] = \
          login_headers(server, mode, user)["authorization"]
    conn = dbapi.connect("%s://%s:%d" % (scheme, info["host"], info["port"]),
                         db_kwargs=kwargs)
    connections.append(conn)
    return conn

  yield make
  for c in connections:
    c.close()
