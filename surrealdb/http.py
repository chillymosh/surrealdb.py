"""
Copyright © SurrealDB Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.

You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from types import TracebackType
from typing import Any

import aiohttp

__all__ = ("SurrealHTTP",)


class SurrealException(Exception):
    """Base exception for SurrealDB client library."""


@dataclass(frozen=True)
class SurrealResponse:
    """Represents a http response from a SurrealDB server.

    Attributes:
        time: The time the request was processed.
        status: The status of the request.
        result: The result of the request.
    """

    time: str
    status: str
    result: list[dict[str, Any]]


# ------------------------------------------------------------------------
# Surreal library methods - exposed to user


class SurrealHTTP:
    """Represents a http connection to a SurrealDB server.

    Args:
        url: The URL of the SurrealDB server.
        namespace: The namespace to use for the connection.
        database: The database to use for the connection.
        username: The username to use for the connection.
        password: The password to use for the connection.
        session: An optional existing aiohttp ClientSession.
    """

    def __init__(
        self,
        url: str,
        namespace: str,
        database: str,
        username: str,
        password: str,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        self._url = url
        self._namespace = namespace
        self._database = database
        self._username = username
        self._password = password

        if session is None:
            self._session = aiohttp.ClientSession()
            self._should_close_session = True
        else:
            self._session = session
            self._should_close_session = False

    def _generate_headers_and_auth(self) -> tuple[dict[str, str], aiohttp.BasicAuth]:
        headers = {
            "NS": self._namespace,
            "DB": self._database,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        auth = aiohttp.BasicAuth(login=self._username, password=self._password)
        return headers, auth

    async def __aenter__(self) -> SurrealHTTP:
        """Connect to the http client when entering the context manager."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        """Disconnect from the http client when exiting the context manager."""
        await self.close()

    async def close(self) -> None:
        """Close the persistent connection to the database."""
        if self._should_close_session:
            await self._session.close()

    async def _request(
        self,
        method: str,
        uri: str,
        data: str | None = None,
        params: Any | None = None,
    ) -> Any:
        headers, auth = self._generate_headers_and_auth()
        async with self._session.request(
            method=method,
            url=self._url + uri,
            data=data,
            params=params,
            headers=headers,
            auth=auth,
        ) as surreal_response:
            surreal_data = await surreal_response.text()
            return json.loads(surreal_data)

    async def signup(self, vars: dict[str, Any]) -> str:
        """Sign this connection up to a specific authentication scope.

        Args:
            vars: Variables used in a signup query.

        Examples:
            await db.signup({"user": "bob", "pass": "123456"})
        """
        return await self._request(method="POST", uri="/signup", data=json.dumps(vars))

    async def signin(self, vars: dict[str, Any]) -> str:
        """Sign this connection in to a specific authentication scope.

        Args:
            vars: Variables used in a signin query.

        Examples:
            await db.signin({"user": "root", "pass": "root"})
        """
        return await self._request(method="POST", uri="/signin", data=json.dumps(vars))

    async def query(
        self, sql: str, vars: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Run a set of SurrealQL statements against the database.

        Args:
            sql: Specifies the SurrealQL statements.
            vars: Assigns variables which can be used in the query.

        Returns:
            The records.

        Examples:
            Assign the variable on the connection
                result = await db.query('create person; select * from type::table($tb)', {'tb': 'person'})

            Get the first result from the first query
                result[0]['result'][0]

            Get all of the results from the second query
                result[1]['result']
        """
        return await self._request(method="POST", uri="/sql", data=sql, params=vars)

    async def select(self, thing: str) -> list[dict[str, Any]]:
        """Select all records in a table (or other entity),
        or a specific record, in the database.

        This function will run the following query in the database:
        select * from $thing

        Args:
            thing: The table or record ID to select.

        Returns:
            The records.

        Examples:
            Select all records from a table (or other entity)
                people = await db.select('person')

            Select a specific record from a table (or other entity)
                person = await db.select('person:h5wxrf2ewk8xjxosxtyc')
        """
        table, record_id = thing.split(":") if ":" in thing else (thing, None)
        response = await self._request(
            method="GET",
            uri=f"/key/{table}/{record_id}" if record_id else f"/key/{table}",
        )
        if not response and record_id is not None:
            raise SurrealException(f"Key {record_id} not found in table {table}")
        return response[0]["result"]

    async def create(self, thing: str, data: dict[str, Any] | None = None) -> str:
        """Create a record in the database.

        This function will run the following query in the database:
        create $thing content $data

        Args:
            thing: The table or record ID.
            data: The document / record data to insert.

        Examples:
            Create a record with a random ID
                person = await db.create('person')

            Create a record with a specific ID
                record = await db.create('person:tobie', {
                    'name': 'Tobie',
                    'settings': {
                        'active': true,
                        'marketing': true,
                        },
                })
        """
        table, record_id = thing.split(":") if ":" in thing else (thing, None)
        response = await self._request(
            method="POST",
            uri=f"/key/{table}/{record_id}" if record_id else f"/key/{table}",
            data=json.dumps(data, ensure_ascii=False),
        )
        if not response and record_id is not None:
            raise SurrealException(f"Key {record_id} not found in table {table}")

        return response[0]["result"]

    async def update(self, thing: str, data: Any) -> dict[str, Any]:
        """Update all records in a table, or a specific record, in the database.

        This function replaces the current document / record data with the
        specified data.

        This function will run the following query in the database:
        update $thing content $data

        Args:
            thing: The table or record ID.
            data: The document / record data to insert.

        Examples:
            Update all records in a table
                person = await db.update('person')

            Update a record with a specific ID
                record = await db.update('person:tobie', {
                    'name': 'Tobie',
                    'settings': {
                        'active': true,
                        'marketing': true,
                        },
                })
        """
        table, record_id = thing.split(":") if ":" in thing else (thing, None)
        response = await self._request(
            method="PUT",
            uri=f"/key/{table}/{record_id}" if record_id else f"/key/{table}",
            data=json.dumps(data, ensure_ascii=False),
        )

        return response[0]["result"]

    async def patch(self, thing: str, data: Any) -> dict[str, Any]:
        """Apply JSON Patch changes to all records, or a specific record, in the database.

        This function patches the current document / record data with
        the specified JSON Patch data.

        This function will run the following query in the database:
        update $thing patch $data

        Args:
            thing: The table or record ID.
            data: The data to modify the record with.

        Examples:
            Update all records in a table
                people = await db.patch('person', [
                    { 'op': "replace", 'path': "/created_at", 'value': str(datetime.datetime.utcnow()) }])

            Update a record with a specific ID
            person = await db.patch('person:tobie', [
                { 'op': "replace", 'path': "/settings/active", 'value': False },
                { 'op': "add", "path": "/tags", "value": ["developer", "engineer"] },
                { 'op': "remove", "path": "/temp" },
            ])
        """
        table, record_id = thing.split(":") if ":" in thing else (thing, None)
        response = await self._request(
            method="PATCH",
            uri=f"/key/{table}/{record_id}" if record_id else f"/key/{table}",
            data=json.dumps(data, ensure_ascii=False),
        )
        return response[0]["result"]

    async def delete(self, thing: str) -> list[dict[str, Any]]:
        """Delete all records in a table, or a specific record, from the database.

        This function will run the following query in the database:
        delete * from $thing

        Args:
            thing: The table name or a record ID to delete.

        Examples:
            Delete all records from a table
                await db.delete('person')
            Delete a specific record from a table
                await db.delete('person:h5wxrf2ewk8xjxosxtyc')
        """
        table, record_id = thing.split(":") if ":" in thing else (thing, None)
        return await self._request(
            method="DELETE",
            uri=f"/key/{table}/{record_id}" if record_id else f"/key/{table}",
        )
