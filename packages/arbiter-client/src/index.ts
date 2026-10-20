// The arbiter's client (docs/api.md, "The clients and the harness plugin"): the types openapi-typescript
// generates from docs/openapi.json, and openapi-fetch's typed calls over them, each route by its path. One
// client reaches one program: the master, or the node arbiter on the caller's own machine.

import createClient, { type Client } from "openapi-fetch";

import type { components, paths } from "./schema.ts";

export type { components, operations, paths } from "./schema.ts";

type Schemas = components["schemas"];

export type Health = Schemas["Health"];
export type Event = Schemas["Event"];
export type EventKind = Schemas["EventKind"];
export type EventPage = Schemas["EventPage"];
export type Cluster = Schemas["Cluster"];
export type Placement = Schemas["Placement"];
export type PlacementAsk = Schemas["PlacementAsk"];
export type Description = Schemas["Description"];
export type Card = Schemas["Card"];
export type Ticket = Schemas["Ticket"];
export type TicketAsk = Schemas["TicketAsk"];
export type CardUnit = Schemas["CardUnit"];
export type CardUnitPage = Schemas["CardUnitPage"];
export type CardUnitChanges = Schemas["CardUnitChanges"];
export type WireError = Schemas["Error"];

export type ArbiterClient = Client<paths>;

/** The program at `baseUrl`, asked with `token` on every call where one is given. */
export function createArbiterClient(baseUrl: string, token?: string, fetcher?: typeof fetch): ArbiterClient {
  return createClient<paths>({
    baseUrl,
    ...(token ? { headers: { Authorization: `Bearer ${token}` } } : {}),
    ...(fetcher ? { fetch: fetcher } : {}),
  });
}
