from client import AgentToolDAGPrefetcher
import json

dag = AgentToolDAGPrefetcher()
print("=== AGENT TOOL DAG PREFETCHER BENCHMARK ===")
res = dag.run_dag_benchmark()
print(json.dumps(res, indent=2))
