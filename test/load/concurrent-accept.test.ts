import { INestApplication } from "@nestjs/common";
import request from "supertest";
import { Keypair } from "@stellar/stellar-sdk";
import { createTestApp } from "../utils/create-test-app";
import { InMemoryIntentsRepository, isVersionConflict } from "../../src/intents/intents.repository";
import { IntentsService } from "../../src/intents/intents.service";
import { IntentsSweeperService } from "../../src/intents/intents-sweeper.service";
import { SolverRegistryService } from "../../src/soroban/solver-registry.service";
import { SEED_SOLVER_KEYPAIRS } from "../../src/solvers/solvers.seed";
import { buildAcceptMessage, buildCancelMessage, buildFillMessage } from "../../src/common/stellar-signature";

const SOLVERS = [SEED_SOLVER_KEYPAIRS.ALPHA, SEED_SOLVER_KEYPAIRS.BETA, SEED_SOLVER_KEYPAIRS.GAMMA];
const USER = Keypair.random();

function sign(kp: Keypair, message: string): string {
  return kp.sign(Buffer.from(message, "utf8")).toString("base64");
}

const validCreateBody = {
  user: USER.publicKey(),
  srcChain: "ethereum",
  srcTokenAddress: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
  srcTokenSymbol: "USDC",
  srcTokenDecimals: 6,
  srcAmount: "1000000",
  dstTokenContract: "CBIELTK6YBZJU5UP2WWQEUCYKLPU6AUNZ2BQ4WWFEIE3USCIHMXQDAMA",
  dstTokenSymbol: "USDC",
  dstTokenDecimals: 7,
  minDstAmount: "990000",
};

describe("Concurrent accept / fill race load test", () => {
  let app: INestApplication;
  let baseUrl: string;

  beforeAll(async () => {
    app = await createTestApp();
    // Bind once: handing supertest an unbound server makes it open a new
    // ephemeral listener per request, which resets under 20-way concurrency.
    await app.listen(0, "127.0.0.1");
    baseUrl = (await app.getUrl()).replace("[::1]", "127.0.0.1");
  });

  afterAll(async () => {
    await app.close();
  });

  const http = () => request(baseUrl);

  async function createIntent(): Promise<string> {
    const res = await http().post("/api/v1/intents").send(validCreateBody).expect(201);
    return res.body.intentId as string;
  }

  function accept(intentId: string, solver: Keypair) {
    return http()
      .post(`/api/v1/intents/${intentId}/accept`)
      .send({ solver: solver.publicKey(), signature: sign(solver, buildAcceptMessage(intentId, solver.publicKey())) });
  }

  function fill(intentId: string, solver: Keypair) {
    return http()
      .post(`/api/v1/intents/${intentId}/fill`)
      .send({
        solver: solver.publicKey(),
        fillAmount: "995000",
        txHash: `tx-${Math.random().toString(16).slice(2)}`,
        signature: sign(solver, buildFillMessage(intentId, solver.publicKey())),
      });
  }

  function cancel(intentId: string) {
    return http()
      .post(`/api/v1/intents/${intentId}/cancel`)
      .send({ user: USER.publicKey(), signature: sign(USER, buildCancelMessage(intentId)) });
  }

  it("only one solver wins when N concurrent accept() calls race on the same intent", async () => {
    const intentId = await createIntent();

    const results = await Promise.all(Array.from({ length: 20 }, (_, i) => accept(intentId, SOLVERS[i % SOLVERS.length])));

    expect(results.filter((r) => r.status === 201)).toHaveLength(1);
    expect(results.filter((r) => r.status !== 201).every((r) => r.status === 409)).toBe(true);

    const intent = (await http().get(`/api/v1/intents/${intentId}`).expect(200)).body;
    expect(intent.state).toBe("accepted");
    expect(SOLVERS.map((s) => s.publicKey())).toContain(intent.solver);
  });

  it("only one fill wins when N concurrent fill() calls race on the same accepted intent", async () => {
    const intentId = await createIntent();
    await accept(intentId, SEED_SOLVER_KEYPAIRS.ALPHA).expect(201);

    const results = await Promise.all(Array.from({ length: 20 }, () => fill(intentId, SEED_SOLVER_KEYPAIRS.ALPHA)));

    expect(results.filter((r) => r.status === 201)).toHaveLength(1);
    const intent = (await http().get(`/api/v1/intents/${intentId}`).expect(200)).body;
    expect(intent.state).toBe("filled");
    expect(intent.fillAmount).toBe("995000");
  });

  it("mixed solvers racing for different intents all resolve with exactly one winner each", async () => {
    const intentIds = await Promise.all(Array.from({ length: 3 }, () => createIntent()));

    const results = await Promise.all(intentIds.map((id, i) => accept(id, SOLVERS[i % SOLVERS.length])));

    expect(results.every((r) => r.status === 201)).toBe(true);
    for (const id of intentIds) {
      expect((await http().get(`/api/v1/intents/${id}`).expect(200)).body.state).toBe("accepted");
    }
  });

  it("unit-level: acceptIfOpen rejects concurrent calls on the same intent", () => {
    const repo = new InMemoryIntentsRepository();
    const [open] = repo.findByState("open");

    const results = Array.from({ length: 50 }, (_, i) =>
      repo.acceptIfOpen(open.intentId, `SOLVER_${i}`, Math.floor(Date.now() / 1000) + 300),
    );

    const winners = results.filter((r) => r !== null);
    expect(winners).toHaveLength(1);
    expect(winners[0]).toMatchObject({ state: "accepted", version: 1 });
  });

  it("unit-level: fillIfAccepted rejects concurrent calls on the same intent", () => {
    const repo = new InMemoryIntentsRepository();
    const [open] = repo.findByState("open");

    repo.acceptIfOpen(open.intentId, "SOLVER_X", Math.floor(Date.now() / 1000) + 300);

    const results = Array.from({ length: 50 }, () =>
      repo.fillIfAccepted(open.intentId, "SOLVER_X", {
        fillAmount: "995000",
        txHash: "race-hash",
        filledAt: Math.floor(Date.now() / 1000),
      }),
    );

    const winners = results.filter((r) => r !== null);
    expect(winners).toHaveLength(1);
    expect(winners[0]).toMatchObject({ state: "filled", version: 2 });
  });

  it("race inventory #473: accept past deadline loses even when it beats the sweeper to the write", async () => {
    const repo = new InMemoryIntentsRepository();
    const [open] = repo.findByState("open");
    const pastDeadline = Math.floor(Date.now() / 1000) - 10;
    const expired = { ...open, deadline: pastDeadline };
    // Simulate an intent whose deadline elapsed before the accept write lands.
    repo.save({ ...expired });
    const now = Math.floor(Date.now() / 1000);
    const result = await repo.acceptIfOpen(open.intentId, "SOLVER_LATE", now + 300, now);
    expect(result).toBeNull();
  });

  it("race inventory #473: fill past the accept-extended deadline loses (sweeper slash wins)", async () => {
    const repo = new InMemoryIntentsRepository();
    const [open] = repo.findByState("open");
    const now = Math.floor(Date.now() / 1000);
    await repo.acceptIfOpen(open.intentId, "SOLVER_X", now + 1, now - 100);
    // Advance past the fill window before the fill write lands.
    const late = await repo.fillIfAccepted(
      open.intentId,
      "SOLVER_X",
      { fillAmount: "995000", txHash: "late", filledAt: now + 60 },
      now + 60,
    );
    expect(late).toBeNull();
  });

  it("race inventory #473: cancel vs accept — exactly one terminal path wins", async () => {
    const repo = new InMemoryIntentsRepository();
    const [open] = repo.findByState("open");
    const now = Math.floor(Date.now() / 1000);
    const accepted = await repo.acceptIfOpen(open.intentId, "SOLVER_RACE", now + 300, now);
    const cancelled = await repo.cancelIfOpen(open.intentId);
    // Exactly one of the two conditional writes may succeed.
    expect(Number(accepted !== null) + Number(cancelled !== null)).toBeLessThanOrEqual(1);
  });

  // ── Issue #405: optimistic concurrency, ETag / If-Match, zero lost updates ──

  describe("optimistic concurrency (issue #405)", () => {
    it("exposes the version as an ETag and advances it on every mutation", async () => {
      const intentId = await createIntent();

      const read = await http().get(`/api/v1/intents/${intentId}`).expect(200);
      expect(read.headers.etag).toBe('"0"');
      expect(read.body.version).toBe(0);

      const accepted = await accept(intentId, SEED_SOLVER_KEYPAIRS.ALPHA).set("If-Match", '"0"').expect(201);
      expect(accepted.headers.etag).toBe('"1"');

      const filled = await fill(intentId, SEED_SOLVER_KEYPAIRS.ALPHA).set("If-Match", '"1"').expect(201);
      expect(filled.headers.etag).toBe('"2"');
    });

    it("rejects a stale If-Match with 412 and leaves the intent untouched", async () => {
      const intentId = await createIntent();
      await http().post(`/api/v1/intents/${intentId}/requote`).expect(201); // version 0 → 1

      const res = await cancel(intentId).set("If-Match", '"0"').expect(412);
      expect(res.body).toMatchObject({ currentVersion: 1, currentETag: '"1"' });
      expect((await http().get(`/api/v1/intents/${intentId}`)).body.state).toBe("open");
    });

    it("rejects a malformed If-Match with 400", async () => {
      const intentId = await createIntent();
      await cancel(intentId).set("If-Match", "not-an-etag").expect(400);
    });

    it("lets exactly one of N clients holding the same ETag win; the rest get 412 or 409", async () => {
      const intentId = await createIntent();
      const { etag } = (await http().get(`/api/v1/intents/${intentId}`)).headers;

      const results = await Promise.all(
        Array.from({ length: 20 }, (_, i) => accept(intentId, SOLVERS[i % SOLVERS.length]).set("If-Match", etag)),
      );

      expect(results.filter((r) => r.status === 201)).toHaveLength(1);
      expect(results.filter((r) => r.status !== 201).every((r) => [409, 412].includes(r.status))).toBe(true);
    });

    it("loses zero updates when N writers do read-modify-write with bounded retries", async () => {
      const intents = app.get(IntentsService);
      const intentId = await createIntent();
      const writers = 25;

      const results = await Promise.all(
        Array.from({ length: writers }, () =>
          intents.mutateWithRetry(
            intentId,
            (current) =>
              intents.update(
                intentId,
                { quotedDstAmount: String(Number(current.quotedDstAmount ?? "0") + 1) },
                current.version,
              ),
            writers, // enough budget for every writer to eventually win
          ),
        ),
      );

      expect(results.some(isVersionConflict)).toBe(false);
      const final = (await http().get(`/api/v1/intents/${intentId}`).expect(200)).body;
      expect(final.quotedDstAmount).toBe(String(writers));
      expect(final.version).toBe(writers);
    });

    it("a late sweeper never slashes fills that landed after it read the intents", async () => {
      const intents = app.get(IntentsService);
      const sweeper = app.get(IntentsSweeperService);
      const registry = app.get(SolverRegistryService);
      const slashSpy = jest.spyOn(registry, "slashSolver");

      const ids = await Promise.all(Array.from({ length: 10 }, () => createIntent()));
      await Promise.all(ids.map((id) => accept(id, SEED_SOLVER_KEYPAIRS.ALPHA).expect(201)));

      // The sweeper's snapshot: every intent accepted and (as far as it knows)
      // overdue. Then every fill lands before its writes do.
      const past = Math.floor(Date.now() / 1000) - 1;
      const snapshot = (await intents.getMany(ids)).map((i) => ({ ...i, deadline: past }));
      await Promise.all(ids.map((id) => fill(id, SEED_SOLVER_KEYPAIRS.ALPHA).expect(201)));

      const realGetByState = intents.getByState.bind(intents);
      const getByState = jest
        .spyOn(intents, "getByState")
        .mockImplementation(async (state) => (state === "accepted" ? snapshot : realGetByState(state)));

      const result = await sweeper.sweep();
      getByState.mockRestore();

      expect(result.slashedCount).toBe(0);
      expect(slashSpy).not.toHaveBeenCalledWith(expect.objectContaining({ intentId: expect.stringMatching(ids.join("|")) }));
      for (const intent of await intents.getMany(ids)) {
        expect(intent.state).toBe("filled");
      }
    });
  });
});
