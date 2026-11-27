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

"""Login, bearer tokens, identity and per-user isolation of tickets."""

import pyarrow as pa
import pyarrow.flight as flight
import pytest

from conftest import server_for
from flightsql_client import FlightSqlClient, basic_header

IDENTITY = "select user(), effective_user()"


def effective_user(client):
  return client.query(IDENTITY).column(1)[0].as_py()


def test_login_user_is_the_effective_user(client, users):
  assert effective_user(client) == users[0]


@pytest.mark.parametrize("mode", ["ldap", "kerberos"])
def test_login_returns_a_bearer_token(connect, users):
  client = connect(login=False)
  token = client.login()
  assert token and token.startswith("Bearer "), token
  # the token alone authenticates later calls
  assert client.headers == {"authorization": token}
  assert effective_user(client) == users[0]
  assert effective_user(client) == users[0]


@pytest.mark.parametrize("mode", ["ldap", "kerberos"])
def test_each_user_gets_their_own_identity(connect, users):
  first, second = connect(users[0]), connect(users[1])
  assert effective_user(first) == users[0]
  assert effective_user(second) == users[1]
  assert first.headers != second.headers


def _secure(servers):
  return server_for(servers, "ldap").info


def test_missing_credentials_are_rejected(servers):
  with FlightSqlClient(_secure(servers)) as c:
    with pytest.raises(flight.FlightUnauthenticatedError,
                       match="Missing Flight SQL authorization header"):
      c.query(IDENTITY)


def test_invalid_bearer_token_is_rejected(servers):
  with FlightSqlClient(_secure(servers),
                       {"authorization": "Bearer not-a-token"}) as c:
    with pytest.raises(flight.FlightUnauthenticatedError):
      c.query(IDENTITY)


def test_wrong_ldap_password_is_rejected(servers):
  with FlightSqlClient(_secure(servers),
                       {"authorization": basic_header("Test1Ldap", "wrong")}) as c:
    with pytest.raises(flight.FlightUnauthenticatedError):
      c.query(IDENTITY)


def test_unknown_ldap_user_is_rejected(servers):
  with FlightSqlClient(_secure(servers),
                       {"authorization": basic_header("nobody", "12345")}) as c:
    with pytest.raises(flight.FlightUnauthenticatedError):
      c.query(IDENTITY)


def test_invalid_kerberos_token_is_rejected(servers):
  with FlightSqlClient(_secure(servers),
                       {"authorization": "Negotiate bm90LWEtdG9rZW4="}) as c:
    with pytest.raises(flight.FlightUnauthenticatedError):
      c.query(IDENTITY)


@pytest.mark.parametrize("mode", ["kerberos"])
def test_replayed_kerberos_token_is_rejected(connect):
  client = connect(login=False)
  negotiate = dict(client.headers)
  client.login()  # uses the token once
  with FlightSqlClient(client.info, negotiate) as replay:
    with pytest.raises(flight.FlightUnauthenticatedError):
      replay.query(IDENTITY)


def test_none_mode_without_user_is_anonymous(servers):
  with FlightSqlClient(server_for(servers, "none").info) as c:
    assert effective_user(c) == "anonymous"


@pytest.mark.parametrize("mode", ["none", "ldap"])
def test_ticket_is_single_use(client):
  info = client.execute(IDENTITY)
  assert client.read(info).num_rows == 1
  with pytest.raises(pa.ArrowException, match="already fetched"):
    client.read(info)


def test_ticket_of_another_user_is_not_served(connect, users):
  owner, other = connect(users[0]), connect(users[1])
  info = owner.execute(IDENTITY)
  with pytest.raises(pa.ArrowException, match="Unknown, expired or already fetched"):
    other.read(info)
  # still there for its owner
  assert owner.read(info).column(1)[0].as_py() == users[0]


def test_adbc_login(adbc, users):
  with adbc().cursor() as cur:
    cur.execute(IDENTITY)
    assert cur.fetchone()[1] == users[0]
