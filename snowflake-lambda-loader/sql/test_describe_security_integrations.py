"""Offline test of DESCRIBE_SECURITY_INTEGRATIONS' Python body.

Runs the handler from describe_security_integrations.sql against a fake
Snowpark session, so logic bugs are caught before an admin runs anything.
Also checks preflight.sql's dry-run body is identical to the deploy body.

    python3 snowflake-lambda-loader/sql/test_describe_security_integrations.py

No dependencies. Does NOT prove Snowflake accepts the DDL or that real
SHOW/DESC output matches the fixtures; preflight.sql step 5 covers that.
"""
import datetime
import json
import pathlib
import re

HERE = pathlib.Path(__file__).parent
_BODY = re.compile(r"\nAS\s*\n\$\$\n(.*?)\n\$\$", re.S)


def _body(path: pathlib.Path) -> str:
    match = _BODY.search(path.read_text())
    assert match, f"no $$ body found in {path.name}"
    return match.group(1)


class Row:
    """Shape of snowflake.snowpark.Row as far as the handler uses it."""

    def __init__(self, d):
        self._d = d

    def as_dict(self):
        return dict(self._d)


class FakeSQLException(Exception):
    pass


# SHOW output: Snowpark returns SHOW columns lowercase and sometimes quoted.
SHOW = [
    {'"name"': "EXAMPLE_EXTERNAL_OAUTH", '"type"': "EXTERNAL_OAUTH - OKTA", '"category"': "SECURITY",
     '"enabled"': "true", '"comment"': None, '"created_on"': datetime.datetime(2026, 8, 6, 12, 29)},
    {"name": 'WEIRD"NAME', "type": "OAUTH - CUSTOM", "category": "SECURITY",
     "enabled": "false", "comment": "has a quote", "created_on": datetime.datetime(2026, 1, 1)},
    {"name": "SNOWFLAKE$LOCAL_APPLICATION", "type": "OAUTH - SNOWFLAKE$LOCAL_APPLICATION",
     "category": "SECURITY", "enabled": "true", "comment": None, "created_on": None},
    {"name": "AAA_SAML", "type": "SAML2", "category": "SECURITY",
     "enabled": "true", "comment": None, "created_on": None},
]

# DESC output keyed by the exact quoted identifier the handler must send.
# Column names match what the fork's SnowflakeClient.describe() reads.
DESC = {
    '"EXAMPLE_EXTERNAL_OAUTH"': [
        {"property": "EXTERNAL_OAUTH_ISSUER", "property_type": "String",
         "property_value": "https://example.okta.com/oauth2/x", "property_default": ""},
        {"property": "EXTERNAL_OAUTH_ANY_ROLE_MODE", "property_type": "String",
         "property_value": "ENABLE", "property_default": "DISABLE"},
        {"property": "OAUTH_CLIENT_SECRET", "property_type": "String",
         "property_value": "LEAK-1", "property_default": ""},
        {"property": "SAML2_SP_PRIVATE_KEY", "property_type": "String",
         "property_value": "LEAK-2", "property_default": ""},
        {"property": "SOME_PASSWORD", "property_type": "String",
         "property_value": "LEAK-3", "property_default": ""},
    ],
    '"WEIRD""NAME"': [
        {"property": "ENABLED", "property_type": "Boolean",
         "property_value": False, "property_default": False},
    ],
    '"AAA_SAML"': [
        {"property": "SAML2_X509_CERT", "property_type": "String",
         "property_value": "MIIC-public-cert", "property_default": ""},
    ],
}


class Result:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return [Row(r) for r in self._rows]


class FakeSession:
    def __init__(self):
        self.statements = []

    def sql(self, statement):
        self.statements.append(statement)
        if statement == "SHOW SECURITY INTEGRATIONS":
            return Result(SHOW)
        match = re.fullmatch(r'DESC SECURITY INTEGRATION (".*")', statement)
        assert match, f"handler ran an unexpected statement: {statement!r}"
        if match.group(1) not in DESC:
            raise FakeSQLException("Insufficient privileges to operate on integration")
        return Result(DESC[match.group(1)])


def main():
    deploy = _body(HERE / "describe_security_integrations.sql")
    preflight = _body(HERE / "preflight.sql")
    assert deploy == preflight, "preflight.sql dry-run body has drifted from the deploy script"

    namespace: dict = {}
    exec(compile(deploy, "describe_security_integrations", "exec"), namespace)
    session = FakeSession()
    out = namespace["run"](session)

    text = json.dumps(out)  # VARIANT return must be JSON-serialisable
    by_name = {i["name"]: i for i in out["integrations"]}

    # Shape
    assert out["version"] == 1
    assert len(out["integrations"]) == len(SHOW), "one entry per SHOW row"
    names = [i["name"] for i in out["integrations"]]
    assert names == sorted(names), "sorted by name"

    # Read-only: only SHOW and DESC were executed
    assert session.statements[0] == "SHOW SECURITY INTEGRATIONS"
    assert all(s.startswith("DESC SECURITY INTEGRATION ") for s in session.statements[1:])

    # Values
    okta = by_name["EXAMPLE_EXTERNAL_OAUTH"]
    assert okta["error"] is None
    assert okta["type"] == "EXTERNAL_OAUTH - OKTA", "quoted SHOW keys normalised"
    assert okta["created_on"] == "2026-08-06 12:29:00", "timestamp stringified"
    assert okta["properties"]["EXTERNAL_OAUTH_ANY_ROLE_MODE"] == {
        "type": "String", "value": "ENABLE", "default": "DISABLE"}
    assert by_name["AAA_SAML"]["properties"]["SAML2_X509_CERT"]["value"] == "MIIC-public-cert"

    # Secrets filtered
    assert "LEAK-" not in text, "secret-named property leaked"

    # Identifier quoting: embedded double quote is escaped, not injected
    assert 'DESC SECURITY INTEGRATION "WEIRD""NAME"' in session.statements
    assert by_name['WEIRD"NAME']["properties"]["ENABLED"]["value"] == "False"

    # One failure doesn't sink the rest
    local = by_name["SNOWFLAKE$LOCAL_APPLICATION"]
    assert local["properties"] is None
    assert local["error"].startswith("FakeSQLException: Insufficient privileges")

    print(f"PASS: {len(out['integrations'])} integrations, {len(session.statements)} statements")


if __name__ == "__main__":
    main()
