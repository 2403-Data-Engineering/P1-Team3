"""Ordered required-signal detection (T0.4).

Production Neo4j uses GDS Louvain/degree in ``required_signals.cypher``.
Tests run the same step order on an in-memory fixture graph. Stretch signals
are not executed. Account score property is ``risk_score``.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from contracts.transaction_event import GRAPH_SCORE_PROPERTY

# Re-export for detection.__init__
__all_score__ = GRAPH_SCORE_PROPERTY

CYPHER_PATH = Path(__file__).with_name("required_signals.cypher")

REQUIRED_STEP_ORDER = (
    "reset",
    "project",
    "degree_out",
    "fan_out_flag",
    "degree_in",
    "fan_in_flag",
    "drain",
    "transfer_cashout",
    "louvain",
    "dense_community",
    "cycles",
    "guilt",
    "risk_score",
)

SCORE_WEIGHTS = {
    "fan_out_flag": 10,
    "fan_in_flag": 10,
    "drain_behavior_flag": 15,
    "transfer_cashout_flag": 15,
    "dense_community_flag": 20,
    "in_cycle_flag": 10,
    "guilt_by_association_flag": 10,
    "similar_to_flagged_flag": 10,
}


@dataclass
class Txn:
    orig: str
    dest: str
    type: str
    amount: float
    step: int
    newbalanceOrig: float = 0.0
    txn_id: str | None = None
    transfer_cashout_flag: int | None = None


@dataclass
class Account:
    id: str
    properties: dict[str, Any] = field(default_factory=dict)


def parse_cypher_steps(cypher_text: str | None = None) -> list[tuple[str, str]]:
    text = cypher_text if cypher_text is not None else CYPHER_PATH.read_text(encoding="utf-8")
    steps: list[tuple[str, str]] = []
    current_name: str | None = None
    current_body: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("// STEP "):
            if current_name is not None:
                steps.append((current_name, "\n".join(current_body).strip()))
            current_name = stripped[len("// STEP ") :].strip()
            current_body = []
        elif current_name is not None:
            current_body.append(line)
    if current_name is not None:
        steps.append((current_name, "\n".join(current_body).strip()))
    return steps


def cypher_step_names() -> list[str]:
    return [name for name, body in parse_cypher_steps() if body]


def _flag(value: Any) -> int:
    return 1 if value == 1 else 0


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    xs = sorted(values)
    if len(xs) == 1:
        return float(xs[0])
    k = (len(xs) - 1) * p
    lo = int(k)
    hi = min(lo + 1, len(xs) - 1)
    return float(xs[lo] + (xs[hi] - xs[lo]) * (k - lo))


def _connected_components(account_ids: Iterable[str], txns: list[Txn]) -> dict[str, int]:
    parent = {aid: aid for aid in account_ids}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for txn in txns:
        union(txn.orig, txn.dest)
    roots: dict[str, int] = {}
    community: dict[str, int] = {}
    next_id = 0
    for aid in account_ids:
        root = find(aid)
        if root not in roots:
            roots[root] = next_id
            next_id += 1
        community[aid] = roots[root]
    return community


def _ensure_account(accounts: dict[str, dict[str, Any]], account_id: str) -> dict[str, Any]:
    if account_id not in accounts:
        accounts[account_id] = {"id": account_id}
    return accounts[account_id]


def apply_required_detection(
    txns: list[Txn], account_ids: Iterable[str] | None = None
) -> tuple[dict[str, dict[str, Any]], list[Txn]]:
    """In-memory required signals. Louvain is approximated by connected components."""
    accounts: dict[str, dict[str, Any]] = {}
    ids = set(account_ids or [])
    for txn in txns:
        ids.add(txn.orig)
        ids.add(txn.dest)
    for aid in ids:
        accounts[aid] = {
            "id": aid,
            GRAPH_SCORE_PROPERTY: 0,
            "similar_to_flagged_flag": None,
        }

    out_degree: dict[str, int] = defaultdict(int)
    in_degree: dict[str, int] = defaultdict(int)
    for txn in txns:
        out_degree[txn.orig] += 1
        in_degree[txn.dest] += 1
    for aid, node in accounts.items():
        node["outDegree"] = out_degree.get(aid, 0)
        node["inDegree"] = in_degree.get(aid, 0)

    p99_out = _percentile([float(node["outDegree"]) for node in accounts.values()], 0.99)
    for node in accounts.values():
        if node["outDegree"] >= p99_out and node["outDegree"] >= 2:
            node["fan_out_flag"] = 1

    p99_in = _percentile([float(node["inDegree"]) for node in accounts.values()], 0.99)
    for node in accounts.values():
        if node["inDegree"] >= p99_in and node["inDegree"] >= 2:
            node["fan_in_flag"] = 1
            node["unusualFanIn"] = 1

    for txn in txns:
        if txn.transfer_cashout_flag == 1:
            txn.transfer_cashout_flag = None

    for first in txns:
        for second in txns:
            if first.dest != second.orig:
                continue
            if second.step - first.step <= 3 and second.newbalanceOrig < 50:
                accounts[first.dest]["drain_behavior_flag"] = 1

    for first in txns:
        if first.type != "TRANSFER" or first.amount <= 0:
            continue
        for second in txns:
            if second.type != "CASH_OUT" or second.orig != first.dest:
                continue
            if second.step - first.step > 2:
                continue
            if abs(first.amount - second.amount) / first.amount < 0.1:
                first.transfer_cashout_flag = 1
                second.transfer_cashout_flag = 1
                accounts[first.dest]["transfer_cashout_flag"] = 1

    communities = _connected_components(accounts.keys(), txns)
    for aid, cid in communities.items():
        accounts[aid]["community_id"] = cid

    internal_count: dict[int, int] = defaultdict(int)
    internal_amount: dict[int, float] = defaultdict(float)
    leaving_amount: dict[int, float] = defaultdict(float)
    size: dict[int, int] = defaultdict(int)
    for node in accounts.values():
        size[int(node["community_id"])] += 1
    for txn in txns:
        orig_c = int(accounts[txn.orig]["community_id"])
        dest_c = int(accounts[txn.dest]["community_id"])
        if orig_c == dest_c:
            internal_count[orig_c] += 1
            internal_amount[orig_c] += txn.amount
        else:
            leaving_amount[orig_c] += txn.amount
    for node in accounts.values():
        cid = int(node["community_id"])
        node["community_transaction_count"] = internal_count.get(cid, 0)
        node["community_total_amount"] = internal_amount.get(cid, 0.0)
        if (
            size[cid] >= 3
            and size[cid] <= 15
            and internal_count.get(cid, 0) >= size[cid]
            and leaving_amount.get(cid, 0.0) < internal_amount.get(cid, 0.0) * 0.1
        ):
            node["dense_community_flag"] = 1

    by_orig: dict[str, list[Txn]] = defaultdict(list)
    for txn in txns:
        by_orig[txn.orig].append(txn)
    for a_id, first_hops in by_orig.items():
        for t1 in first_hops:
            b_id = t1.dest
            if b_id == a_id:
                continue
            for t2 in by_orig.get(b_id, []):
                c_id = t2.dest
                if c_id in {a_id, b_id} or t2.step < t1.step:
                    continue
                for t3 in by_orig.get(c_id, []):
                    if t3.dest == a_id and t3.step >= t2.step:
                        accounts[a_id]["in_cycle_flag"] = 1
                        accounts[b_id]["in_cycle_flag"] = 1
                        accounts[c_id]["in_cycle_flag"] = 1

    neighbors: dict[str, set[str]] = defaultdict(set)
    for txn in txns:
        neighbors[txn.orig].add(txn.dest)
        neighbors[txn.dest].add(txn.orig)

    def _is_bad(node: dict[str, Any]) -> bool:
        return any(
            node.get(flag) == 1
            for flag in (
                "dense_community_flag",
                "drain_behavior_flag",
                "unusualFanIn",
                "fan_out_flag",
                "transfer_cashout_flag",
            )
        )

    for aid, node in accounts.items():
        if _is_bad(node):
            continue
        bad = sum(1 for nb in neighbors[aid] if _is_bad(accounts[nb]))
        if bad >= 2:
            node["guilt_by_association_flag"] = 1

    for node in accounts.values():
        score = 0
        for flag, weight in SCORE_WEIGHTS.items():
            score += weight * _flag(node.get(flag))
        node[GRAPH_SCORE_PROPERTY] = score

    return accounts, txns


def fixture_graph() -> list[Txn]:
    """Small unlabeled graph covering each required signal."""
    return [
        Txn("R1", "R2", "TRANSFER", 500.0, 1, 0.0),
        Txn("R2", "R3", "TRANSFER", 500.0, 2, 0.0),
        Txn("R3", "R1", "TRANSFER", 500.0, 3, 0.0),
        Txn("HUB", "D1", "PAYMENT", 10.0, 1, 0.0),
        Txn("HUB", "D2", "PAYMENT", 10.0, 1, 0.0),
        Txn("HUB", "D3", "PAYMENT", 10.0, 1, 0.0),
        Txn("HUB", "D4", "PAYMENT", 10.0, 1, 0.0),
        Txn("S1", "COL", "PAYMENT", 5.0, 1, 0.0),
        Txn("S2", "COL", "PAYMENT", 5.0, 1, 0.0),
        Txn("S3", "COL", "PAYMENT", 5.0, 1, 0.0),
        Txn("SRC", "DRAIN", "TRANSFER", 1000.0, 10, 0.0),
        Txn("DRAIN", "SINK", "CASH_OUT", 990.0, 12, 10.0),
        Txn("ORIG", "MULE", "TRANSFER", 2000.0, 20, 100.0),
        Txn("MULE", "CASHDEST", "CASH_OUT", 2000.0, 21, 0.0),
        Txn("GUILT", "DRAIN", "PAYMENT", 1.0, 30, 0.0),
        Txn("GUILT", "MULE", "PAYMENT", 1.0, 31, 0.0),
        Txn("INN", "MERCH", "PAYMENT", 20.0, 1, 0.0),
    ]


def run_cypher_pipeline(session: Any) -> list[str]:
    executed: list[str] = []
    for name, body in parse_cypher_steps():
        if not body:
            continue
        for statement in _split_statements(body):
            session.run(statement)
        executed.append(name)
    return executed


def _split_statements(body: str) -> list[str]:
    statements: list[str] = []
    buf: list[str] = []
    for line in body.splitlines():
        if line.strip().startswith("//"):
            continue
        buf.append(line)
        if line.rstrip().endswith(";"):
            stmt = "\n".join(buf).strip().rstrip(";")
            if stmt:
                statements.append(stmt)
            buf = []
    tail = "\n".join(buf).strip().rstrip(";")
    if tail:
        statements.append(tail)
    return statements


def main() -> None:
    from neo4j import GraphDatabase

    from database_creation_script import neo4j_connect_args
    from dotenv import load_dotenv

    load_dotenv()
    uri, user, password = neo4j_connect_args()
    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session() as session:
            executed = run_cypher_pipeline(session)
        print("Detection steps:", ", ".join(executed))
    finally:
        driver.close()


if __name__ == "__main__":
    main()
