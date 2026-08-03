"""`POST /v1/profiles` — resolve a person by email or source-native id.

    POST /v1/profiles   200 · 404 unknown email · 400 bad value_type
                        400 value_type=id without a source

The only non-admin write on identity, and the second independent proof of the
identity chain in this suite. `/v1/subchart` proves it CALLER-derived — the
session resolves to a person. This proves it by LOOKUP — the address the seed
used resolves to the UUID the manifest recorded. Either could pass with the
other broken.

The 401 half is in `test_gateway.py`, swept over every operation at once.
"""

from __future__ import annotations

from insight_stand import Manifest, PersonaSession, identity_path

from ..schemas import ProblemDocument, Profile

PROFILES = identity_path("/v1/profiles")


def test_resolve_by_email_200(lead_session: PersonaSession, stand_manifest: Manifest) -> None:
    """A seeded address resolves to the person the manifest names, in this tenant."""
    expected = stand_manifest.fixture("dev_lead")
    response = lead_session.client.post(
        PROFILES, json_body={"value_type": "email", "value": expected.email}
    )
    assert response.status_code == 200, f"status={response.status_code} {response.text[:300]}"

    profile = response.parse(Profile)
    assert str(profile.person_id) == expected.uuid, (
        f"{expected.email} resolved to {profile.person_id}, but the manifest says {expected.uuid}"
    )
    assert str(profile.insight_tenant_id) == stand_manifest.tenant, (
        "the profile came back under a different tenant than the manifest declares"
    )


def test_resolve_by_email_404_unknown(lead_session: PersonaSession) -> None:
    response = lead_session.client.post(
        PROFILES, json_body={"value_type": "email", "value": "nobody@example.com"}
    )
    assert response.status_code == 404, f"status={response.status_code} {response.text[:300]}"
    assert response.parse(ProblemDocument).status == 404


def test_resolve_400_unknown_value_type(lead_session: PersonaSession) -> None:
    """`value_type` is a closed set, and an unknown one is rejected as an argument.

    A canonical 400 rather than one of Axum's plain-text extractor rejections:
    the body IS the request type, so the handler is reached and the complaint is
    about the value.
    """
    response = lead_session.client.post(
        PROFILES, json_body={"value_type": "not-a-value-type", "value": "x"}
    )
    assert response.status_code == 400, f"status={response.status_code} {response.text[:300]}"
    assert response.parse(ProblemDocument).status == 400


def test_resolve_by_id_400_without_a_source(
    lead_session: PersonaSession, stand_manifest: Manifest
) -> None:
    """`value_type: "id"` needs the source that issued the id.

    A conditional requirement — `insight_source_type` and `insight_source_id`
    are optional on the request type and mandatory for this one value_type — so
    it is exactly the kind of rule a schema cannot state and only a test can.
    Omitting them must fail rather than resolve against an arbitrary source.
    """
    response = lead_session.client.post(
        PROFILES,
        json_body={"value_type": "id", "value": stand_manifest.fixture("dev_lead").uuid},
    )
    assert response.status_code == 400, f"status={response.status_code} {response.text[:300]}"
    assert response.parse(ProblemDocument).status == 400
