from runner.rule_runner import RuleRunner
import pandas as pd, os

os.makedirs("input", exist_ok=True)

# Create a simple fake BOM file
fake_bom = pd.DataFrame({
    "Part ID": ["R001", "C002", "U003"],
    "Description": ["10k 1% 0603 RES", "100nF 16V CAP", "LM358D IC OPAMP"],
})
fake_bom.to_excel("./input/fake_bom.xlsx", index=False)

# Run the pipeline
runner = RuleRunner(r"C:\PersonalProjects\PartChecker\input_based_testing\pipeline.yaml")
payload = runner.run()

print("\n✅ Final Output:")
for row in payload.data:
    print(row)
