from sentence_transformers import SentenceTransformer
import numpy as np
import time

MODELS = [
    "intfloat/e5-base-v2",
    "BAAI/bge-base-en-v1.5",
    "BAAI/bge-small-en-v1.5",
    "nomic-ai/nomic-embed-text-v1",
    "sentence-transformers/all-MiniLM-L6-v2",
]

#  Positive and Negative test cases
TEST_PAIRS = [
    # --- Positive pairs ---
    ("RES TF SM 1/10W 47K 1% 0603 RoHS",
     "Resistor Thin Film 47K 0603 1%", "Resistor phrasing (positive)"),
    ("DIODE, Dual Schottky Rectifier 30PIV 200mA, SOT-23  Marked L44",
     "DIODE SCHOTTKY BAT54S-7-F 30V 200MA SOT23 RoHS", "Schottky diode same spec (positive)"),
    ("CONN RA 28 Pos SMT ZIF Bottom",
     "CONN FPC BOTTOM 4POS 1MM RA RoHS", "Connector similar phrasing (positive)"),
    ("CAP 0.1uF 50V 20% AX",
     "CAP CER 0.1UF 20% 50V Z5U AXIAL TH RoHS", "Capacitor similar phrasing (positive)"),
    # --- Negative pairs ---
    ("RES TF SM 1/10W 47K 1% 0603 RoHS",
     "RES TF SM 1/10W 390R 5% 0805 RoHS", "Resistors, wrong value (negative)"),
    ("CAP EL 470uF 50V 20% SMD",
     "RES TF SM 1/10W 47K 1% 0603 RoHS", "Capacitor vs resistor (negative)"),
    ("CONN RA 28 Pos SMT ZIF Bottom",
     "DIODE SCHOTTKY BAT54S-7-F 30V 200MA SOT23 RoHS", "Connector vs diode (negative)"),
    ("CONN HEADER 2x6 STR 1MM",
     "CONN HEADER 2x6 STR 2.54MM", "Connectors diff pitch (negative)"),
    ("CAP CER 0.1UF 20% 50V",
     "CAP TANT 10UF 10% 16V 1206 RoHS", "Caps diff dielectric/value (negative)"),
]

print("\n🔍 Semantic Model Comparison — Positives + Negatives\n")

for mname in MODELS:
    print(f"▶ Testing model: {mname}")
    start_time = time.time()
    try:
        model = SentenceTransformer(mname, trust_remote_code=True)
    except Exception as e:
        print(f"    Failed to load {mname}: {e}")
        continue

    results = []
    for desc1, desc2, label in TEST_PAIRS:
        t0 = time.time()
        embs = model.encode([desc1, desc2], normalize_embeddings=True)
        sim = float(np.dot(embs[0], embs[1]))
        results.append((label, sim, time.time() - t0))

    print("   ─ Results ─")
    for label, sim, pair_time in results:
        status = " POS" if "positive" in label.lower() else "❌ NEG"
        print(f"   {status} | {label:<60} → Cosine: {sim:.4f}")

    print(f"   Total model run time: {time.time() - start_time:.2f}s\n")
print(" Done testing all models.\n")
