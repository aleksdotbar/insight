import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@/api/fetch-with-auth", () => ({ fetchWithAuth: vi.fn() }));

import { fetchWithAuth } from "@/api/fetch-with-auth";

import { getPerson, IdentityApiError } from "./identity-client";

const mockFetch = fetchWithAuth as unknown as ReturnType<typeof vi.fn>;

function response(body: unknown, init?: { ok?: boolean; status?: number }): Response {
  return {
    ok: init?.ok ?? true,
    status: init?.status ?? 200,
    json: async () => body,
  } as unknown as Response;
}

beforeEach(() => {
  mockFetch.mockReset();
});

describe("getPerson", () => {
  it("POSTs /profiles with an {email} body and maps the profile", async () => {
    mockFetch.mockResolvedValueOnce(
      response({
        person_id: "p-1",
        insight_tenant_id: "t-1",
        email: "bob.park@example.com",
        display_name: "Bob Park",
        job_title: "Lead",
        supervisor_email: "ceo@example.com",
      }),
    );

    const person = await getPerson("bob.park@example.com");

    const [url, init] = mockFetch.mock.calls[0];
    expect(url).toBe("/api/identity/v1/profiles");
    expect(init).toMatchObject({ method: "POST" });
    expect(JSON.parse((init as RequestInit).body as string)).toEqual({
      value_type: "email",
      value: "bob.park@example.com",
    });

    expect(person.person_id).toBe("p-1");
    expect(person.email).toBe("bob.park@example.com");
    expect(person.job_title).toBe("Lead");
    expect(person.supervisor_email).toBe("ceo@example.com");
    // Omitted optional strings default to ""; omitted parent fields stay null.
    expect(person.department).toBe("");
    expect(person.parent_id).toBeNull();
    expect(person.parent_email).toBeNull();
  });

  it("maps subordinates recursively and drops any without an email", async () => {
    mockFetch.mockResolvedValueOnce(
      response({
        person_id: "p-lead",
        insight_tenant_id: "t-1",
        email: "lead@example.com",
        subordinates: [
          { person_id: "p-2", insight_tenant_id: "t-1", email: "ic1@example.com" },
          // no email -> dropped (would be a broken link + duplicate "" key)
          { person_id: "p-3", insight_tenant_id: "t-1" },
          { person_id: "p-4", insight_tenant_id: "t-1", email: "  " },
        ],
      }),
    );

    const person = await getPerson("lead@example.com");

    expect(person.subordinates.map((s) => s.email)).toEqual(["ic1@example.com"]);
  });

  it("throws IdentityApiError with the status + body on a non-ok response", async () => {
    mockFetch.mockResolvedValueOnce(
      response({ error: "not_found" }, { ok: false, status: 404 }),
    );

    await expect(getPerson("ghost@example.com")).rejects.toMatchObject({
      name: "IdentityApiError",
      status: 404,
      body: { error: "not_found" },
    });
  });

  it("throws IdentityApiError(invalid_json) when the body is not JSON", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => {
        throw new SyntaxError("Unexpected token");
      },
    } as unknown as Response);

    await expect(getPerson("bob@example.com")).rejects.toMatchObject({
      status: 200,
      body: { error: "invalid_json" },
    });
  });

  it("rejects a profile missing the required email", async () => {
    mockFetch.mockResolvedValueOnce(
      response({ person_id: "p-1", insight_tenant_id: "t-1" }),
    );

    const err = await getPerson("bob@example.com").catch((e: unknown) => e);
    expect(err).toBeInstanceOf(IdentityApiError);
    expect((err as IdentityApiError).body).toEqual({ error: "missing_email" });
  });
});
