from __future__ import annotations

from contracts.transaction_event import GRAPH_SCORE_PROPERTY
from detection.pipeline import (
    REQUIRED_STEP_ORDER,
    SCORE_WEIGHTS,
    apply_required_detection,
    cypher_step_names,
    fixture_graph,
    parse_cypher_steps,
    run_cypher_pipeline,
)


def test_cypher_pipeline_order_puts_louvain_before_dense_community():
    names = cypher_step_names()
    assert names == list(REQUIRED_STEP_ORDER)
    assert names.index("louvain") < names.index("dense_community")
    assert names.index("fan_out_flag") < names.index("risk_score")
    assert names[-1] == "risk_score"


def test_cypher_sets_fan_out_flag_and_risk_score():
    steps = dict(parse_cypher_steps())
    assert "SET n.fan_out_flag = 1" in steps["fan_out_flag"]
    assert "SET a.risk_score" in steps["risk_score"]
    assert "fraud_score" not in steps["risk_score"]
    assert "gds.louvain.write" in steps["louvain"]
    assert "nodeSimilarity" not in "".join(steps.values())


def test_fixture_graph_sets_required_flags_and_risk_score():
    accounts, txns = apply_required_detection(fixture_graph())

    assert accounts["HUB"].get("fan_out_flag") == 1
    assert accounts["COL"].get("fan_in_flag") == 1
    assert accounts["COL"].get("unusualFanIn") == 1
    assert accounts["DRAIN"].get("drain_behavior_flag") == 1
    assert accounts["MULE"].get("transfer_cashout_flag") == 1
    assert any(txn.transfer_cashout_flag == 1 for txn in txns)

    for ring in ("R1", "R2", "R3"):
        assert accounts[ring].get("dense_community_flag") == 1
        assert accounts[ring].get("in_cycle_flag") == 1
        assert accounts[ring]["community_id"] == accounts["R1"]["community_id"]

    assert accounts["GUILT"].get("guilt_by_association_flag") == 1
    assert accounts["INN"].get("fan_out_flag") is None
    assert accounts["INN"][GRAPH_SCORE_PROPERTY] == 0

    for node in accounts.values():
        assert GRAPH_SCORE_PROPERTY in node
        expected = sum(
            SCORE_WEIGHTS[flag] for flag in SCORE_WEIGHTS if node.get(flag) == 1
        )
        assert node[GRAPH_SCORE_PROPERTY] == expected

    assert accounts["HUB"][GRAPH_SCORE_PROPERTY] >= 10
    assert accounts["MULE"][GRAPH_SCORE_PROPERTY] >= 15
    assert accounts["R1"][GRAPH_SCORE_PROPERTY] >= 30


def test_similar_to_flagged_is_not_fired():
    accounts, _ = apply_required_detection(fixture_graph())
    assert all(node.get("similar_to_flagged_flag") != 1 for node in accounts.values())


def test_run_cypher_pipeline_executes_steps_in_order():
    executed: list[str] = []

    class FakeSession:
        def run(self, statement: str):
            executed.append(statement)
            return self

    names = run_cypher_pipeline(FakeSession())
    assert names == list(REQUIRED_STEP_ORDER)
    assert any("fan_out_flag" in stmt for stmt in executed)
    assert any("risk_score" in stmt for stmt in executed)
    louvain_at = next(i for i, stmt in enumerate(executed) if "louvain.write" in stmt)
    reset_at = next(i for i, stmt in enumerate(executed) if "a.dense_community_flag = null" in stmt)
    dense_at = next(i for i, stmt in enumerate(executed) if "SET a.dense_community_flag = 1" in stmt)
    assert reset_at < louvain_at < dense_at
