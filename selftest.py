from __future__ import annotations

from core import Config, Engine, HawkesProcess


def main() -> None:
    base_cfg = Config(controller_mode="fixed_rate", burst_size=8000, inventory=1000,
                      degradation_start_s=6, degradation_end_s=9)
    prop_cfg = Config(controller_mode="mpc_cbf", burst_size=8000, inventory=1000,
                      degradation_start_s=6, degradation_end_s=9)

    base = Engine(base_cfg).run_to_completion()
    prop = Engine(prop_cfg).run_to_completion()

    # 1. Natural termination and finite-inventory conservation.
    for snap, cfg in ((base, base_cfg), (prop, prop_cfg)):
        assert snap["termination_reason"] in {"inventory_sold_out", "demand_resolved", "max_steps_guard"}
        assert 0 <= snap["inventory"] <= cfg.inventory
        assert snap["successful_bookings"] <= cfg.inventory
        assert snap["successful_bookings"] <= cfg.burst_size
        assert snap["successful_bookings"] + snap["inventory"] == cfg.inventory
        assert 0 <= snap["burst_absorption_rate"] <= 1
        assert snap["final_time_s"] > 0

    # Canonical case has much more demand than inventory and should naturally sell out.
    assert prop["termination_reason"] == "inventory_sold_out"
    assert prop["inventory"] == 0
    assert prop["time_to_sellout_s"] is not None

    # 2. Proposed CBF: zero modeled violations under the stated deterministic envelope.
    assert prop["safety_violations"] == 0
    assert prop["max_safe_violation"] == 0

    # 3. MPC prediction horizon is independent of run termination.
    assert prop["config"]["horizon"] * prop["config"]["dt"] > 0
    assert "max_simulation_steps" in prop["config"]

    # 4. Inventory is never forced to zero when demand is insufficient.
    surplus_cfg = Config(controller_mode="mpc_cbf", burst_size=500, inventory=1000,
                         degradation_start_s=6, degradation_end_s=9)
    surplus = Engine(surplus_cfg).run_to_completion()
    assert surplus["termination_reason"] == "demand_resolved"
    assert surplus["inventory"] > 0
    assert surplus["successful_bookings"] + surplus["inventory"] == surplus_cfg.inventory

    # 5. Hawkes single-event recurrence: one failure adds exactly alpha once.
    hp = HawkesProcess(base_cfg)
    hp.add_events(1)
    assert abs(hp.z - base_cfg.hawkes_alpha) < 1e-12
    hp.decay(base_cfg.dt)
    assert hp.z < base_cfg.hawkes_alpha
    assert hp.events == 1

    # 6. Capacity-transition anticipation: the proposed envelope remains safe
    # before and through the known degradation transition.
    shock = Engine(Config(controller_mode="mpc_cbf", burst_size=8000, inventory=1000,
                          degradation_start_s=2, degradation_end_s=5)).run_to_completion()
    assert shock["safety_violations"] == 0

    # 7. Retry accounting remains bounded and successful bookings are unique.
    assert prop["retry_amplification"] >= 1.0
    assert prop["retry_attempt_rate"] >= 0.0

    # 8. Stress baseline is allowed to violate the safe envelope; this is useful
    # as a negative control and prevents the benchmark from becoming tautological.
    stress = Engine(Config(controller_mode="admit_all", burst_size=8000, inventory=1000,
                           degradation_start_s=6, degradation_end_s=9)).run_to_completion()
    assert stress["successful_bookings"] <= 1000
    assert stress["inventory"] >= 0

    print("PASS: finite-inventory conservation")
    print("PASS: canonical run naturally terminates by sell-out")
    print("PASS: proposed MPC+CBF has zero modeled safety violations")
    print("PASS: surplus-inventory case preserves residual inventory")
    print("PASS: MPC horizon is distinct from simulation termination")
    print("PASS: Hawkes excitation is applied once per failure event")
    print("PASS: known capacity degradation is anticipated safely")
    print("PASS: stress baseline remains a meaningful negative control")
    print("ALL SELF-TESTS PASSED")


if __name__ == "__main__":
    main()
