from __future__ import annotations

import inspect
from dataclasses import is_dataclass, fields as dc_fields
from typing import Any, Dict, Optional, List

import random
#   python -m NPT_TOOL.smoke_test
from .. import matching_engine as me
from ..matching_engine import MatchingEngine
from ..data_models import NPRPart, InventoryPart  # adjust if your names differ


# -----------------------------
# Generic object factory
# -----------------------------
def _default_for_param(name: str, anno: Any) -> Any:
    # reasonable defaults for common fields
    n = name.lower()
    if "desc" in n or "description" in n:
        return ""
    if "mpn" in n or "pn" in n or "part" in n:
        return ""
    if "qty" in n or "quantity" in n:
        return 1
    if "parsed" in n or "fields" in n:
        return {}
    return None


def make_instance(cls, **kwargs):
    """
    Create an instance of a dataclass/regular class even if you don't know the constructor.
    Fills required args with defaults, then applies kwargs.
    """
    try:
        sig = inspect.signature(cls)
        init_kwargs = {}
        for p in sig.parameters.values():
            if p.name == "self":
                continue
            if p.name in kwargs:
                init_kwargs[p.name] = kwargs[p.name]
            elif p.default is not inspect._empty:
                # has default
                pass
            else:
                init_kwargs[p.name] = _default_for_param(p.name, p.annotation)

        obj = cls(**init_kwargs)

    except Exception:
        # fallback: dataclass field init or empty init
        try:
            if is_dataclass(cls):
                init_kwargs = {}
                for f in dc_fields(cls):
                    if f.name in kwargs:
                        init_kwargs[f.name] = kwargs[f.name]
                    else:
                        init_kwargs[f.name] = _default_for_param(f.name, f.type)
                obj = cls(**init_kwargs)
            else:
                obj = cls()
        except Exception as e:
            raise RuntimeError(f"Could not instantiate {cls}: {e}")

    # Apply kwargs as attributes too (helps if class ignores unknown init args)
    for k, v in kwargs.items():
        try:
            setattr(obj, k, v)
        except Exception:
            pass

    return obj




def build_synthetic_inventory(
    *,
    n_wrong: int = 60,          # how many "insanely wrong" distractors
    n_near_caps: int = 40,      # how many "close-but-wrong caps"
    seed: int = 1337,
) -> List[InventoryPart]:
    """
    Build a synthetic inventory:
      - 2 correct-ish CAP candidates (100pF 50V X7R 0603)
      - many near-miss caps (wrong value/package/dielectric/voltage)
      - many wildly incorrect parts (resistors, inductors, diodes, ICs, connectors, etc.)
    """
    rng = random.Random(seed)
    inv: List[InventoryPart] = []

    # --- Ground truth / "correct" candidates ---
    inv.append(
        InventoryPart(
            itemnum="21-INV-0001",
            desc="CAP CER 100PF 50V 10% X7R 0603",
            mfgid="M1",
            mfgname="MURATA",
            vendoritem="GRM1885C1H101JA01D",
            parsed={"type": "CAP", "value": "100PF", "voltage": "50V", "dielectric": "X7R", "package": "0603", "tol": "10%"},
        )
    )
    inv.append(
        InventoryPart(
            itemnum="21-INV-0005",
            desc="MLCC 100 pF 50 Volt X7R 0603",
            mfgid="M2",
            mfgname="TDK",
            vendoritem="C1608X7R1H101K080AA",
            parsed={"type": "CAP", "value": "100PF", "voltage": "50V", "dielectric": "X7R", "package": "0603"},
        )
    )

    # --- Near-miss caps (lexically similar but wrong spec) ---
    cap_values_pf = [
        "10PF", "12PF", "15PF", "22PF", "33PF", "47PF",
        "82PF", "120PF", "150PF", "180PF", "220PF",
        "330PF", "470PF", "680PF", "1000PF", "2200PF",
        "0.01UF", "0.1UF"
    ]
    cap_pkgs = ["0201", "0402", "0603", "0805", "1206"]
    cap_dielectrics = ["X7R", "X5R", "C0G", "NP0", "Y5V"]
    cap_voltages = ["6.3V", "10V", "16V", "25V", "50V", "100V"]

    for k in range(n_near_caps):
        val = rng.choice(cap_values_pf)
        pkg = rng.choice(cap_pkgs)
        diel = rng.choice(cap_dielectrics)
        volt = rng.choice(cap_voltages)

        # Bias towards looking similar to the query while still wrong:
        # - keep "CAP CER" token
        # - often keep X7R or 0603 but mutate value/voltage
        if rng.random() < 0.45:
            diel = "X7R"
        if rng.random() < 0.45:
            pkg = "0603"
        if rng.random() < 0.35:
            volt = "50V"
        # Ensure many are wrong: if we accidentally match all fields, force wrong value
        if val in ("100PF",) and diel == "X7R" and pkg == "0603" and volt == "50V":
            val = rng.choice([v for v in cap_values_pf if v != "100PF"])

        itemnum = f"21-CAP-NM-{k:03d}"
        desc = f"CAP CER {val} {volt} 10% {diel} {pkg}"
        inv.append(
            InventoryPart(
                itemnum=itemnum,
                desc=desc,
                mfgid="MC",
                mfgname=rng.choice(["MURATA", "TDK", "AVX", "KEMET", "YAGEO"]),
                vendoritem=f"CAPNM{k:04d}",
                parsed={"type": "CAP", "value": val, "voltage": volt, "dielectric": diel, "package": pkg, "tol": "10%"},
            )
        )

    # --- Insanely wrong distractors (different families) ---
    # Lots of tokens that might confuse dense embedding but should be rejected by gates/rerank.
    resistor_values = ["1OHM", "10OHM", "100OHM", "1K", "10K", "100K", "1M"]
    resistor_pkgs = ["0402", "0603", "0805", "1206"]
    diode_types = ["ZENER", "TVS", "SCHOTTKY", "RECTIFIER", "SIGNAL"]
    diode_pkgs = ["SOD-123", "SOD-323", "SMA", "SMB", "SOT-23"]
    inductor_values = ["1UH", "2.2UH", "10UH", "47UH", "100UH"]
    ic_types = ["EEPROM", "MCU", "OPAMP", "REGULATOR", "LOGIC"]
    conn_types = ["USB-C", "HDR", "TERMINAL", "FFC", "RJ45"]

    for k in range(n_wrong):
        kind = rng.choice(["RES", "DIODE", "IND", "IC", "CONN", "MECH", "LED"])
        itemnum = f"99-WRONG-{k:03d}"

        if kind == "RES":
            val = rng.choice(resistor_values)
            pkg = rng.choice(resistor_pkgs)
            desc = f"RES SMD {val} 1% 1/10W {pkg}"
            parsed = {"type": "RES", "value": val, "package": pkg, "tol": "1%", "power": "0.1W"}

        elif kind == "DIODE":
            dt = rng.choice(diode_types)
            pkg = rng.choice(diode_pkgs)
            volt = rng.choice(["5.1V", "12V", "15V", "24V", "36V", "58V"])
            desc = f"DIODE {dt} {volt} {pkg}"
            parsed = {"type": "DIODE", "value": volt, "package": pkg}

        elif kind == "IND":
            val = rng.choice(inductor_values)
            pkg = rng.choice(["0402", "0603", "0805", "1210"])
            desc = f"INDUCTOR {val} {pkg}"
            parsed = {"type": "IND", "value": val, "package": pkg}

        elif kind == "IC":
            it = rng.choice(ic_types)
            pkg = rng.choice(["SOT-23", "SOIC-8", "TSSOP-14", "QFN-32"])
            desc = f"IC {it} {pkg}"
            parsed = {"type": "OTHER", "package": pkg}  # leave OTHER to avoid hard-gating unknown ICs

        elif kind == "CONN":
            ct = rng.choice(conn_types)
            pins = rng.choice(["2POS", "4POS", "8POS", "10POS", "20POS"])
            desc = f"CONNECTOR {ct} {pins}"
            parsed = {"type": "OTHER"}

        elif kind == "LED":
            color = rng.choice(["RED", "GREEN", "BLUE", "WHITE", "AMBER"])
            pkg = rng.choice(["0402", "0603", "0805"])
            desc = f"LED {color} {pkg}"
            parsed = {"type": "LED", "package": pkg}

        else:  # MECH etc.
            desc = rng.choice([
                "SCREW M3 PAN HEAD",
                "WASHER M3",
                "STANDOFF M3 10MM",
                "HEATSINK TO-220",
                "FUSE 2A 250V",
            ])
            parsed = {"type": "OTHER"}

        inv.append(
            InventoryPart(
                itemnum=itemnum,
                desc=desc,
                mfgid="X",
                mfgname=rng.choice(["GENERIC", "VISHAY", "TI", "NXP", "ST", "SAMTEC"]),
                vendoritem=f"WRONG{k:04d}",
                parsed=parsed,
            )
        )

    # Shuffle so order doesn't “help” you
    rng.shuffle(inv)

    return inv


def build_query() -> NPRPart:
    return NPRPart(
        partnum="C_TEST",
        desc="CAP CER 100PF 50V X7R 0603",
        mfgname="",
        mfgpn="",
        supplier="",
        parsed={"type": "CAP", "value": "100PF", "voltage": "50V", "dielectric": "X7R", "package": "0603"},
    )


def run_once(primary: str) -> None:
    print("\n" + "=" * 80)
    print(f"SMOKE TEST: ENG_PRIMARY_RETRIEVER = {primary!r}")
    print("=" * 80)

    me.ENG_PRIMARY_RETRIEVER = primary

    inv = build_synthetic_inventory(n_wrong=120, n_near_caps=80, seed=1337)

    eng = MatchingEngine(inv, config=None, cache_dir=".npr_semantic_cache")

    npr = build_query()
    mr = eng._engineering_match(npr)

    if not mr:
        print("NO MATCH RESULT")
        return

    win = mr.inventory_part
    print("WINNER:")
    print("  itemnum:", getattr(win, "itemnum", None))
    print("  desc:", getattr(win, "desc", None))
    print("  _pc_seed:", getattr(win, "_pc_seed", None))
    print("  _pc_score:", getattr(win, "_pc_score", None))
    print("  confidence:", getattr(mr, "confidence", None))
    print("  notes:", getattr(mr, "notes", None))

    print("\nTOP CANDIDATES:")
    for i, c in enumerate(mr.candidates, 1):
        print(
            f"{i:>2}. itemnum={getattr(c,'itemnum',None)} "
            f"seed={getattr(c,'_pc_seed',None)} score={getattr(c,'_pc_score',None)} "
            f"desc={getattr(c,'desc',None)}"
        )

    if mr.explain:
        print("\nEXPLAIN:")
        print("  primary:", mr.explain.get("primary"))
        print("  secondary:", mr.explain.get("secondary"))
        print("  counts:", mr.explain.get("counts"))
        print("  sparse_model:", mr.explain.get("sparse_model"))
        print("  reranker_model:", mr.explain.get("reranker_model"))


def main():
    # Ensure toggles are on
    me.ENG_DEBUG = 1
    me.ENG_USE_DENSE = 1
    me.ENG_USE_SPARSE = 1
    me.ENG_USE_RERANK = 1
    me.ENG_USE_FUZZY_FALLBACK = 1

    run_once("dense")
    run_once("sparse")


if __name__ == "__main__":
    main()
