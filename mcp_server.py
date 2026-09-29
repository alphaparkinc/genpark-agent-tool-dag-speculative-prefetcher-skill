import sys, json
from client import AgentToolDAGPrefetcher

def handle_mcp():
    dag = AgentToolDAGPrefetcher()
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        print(json.dumps(dag.run_dag_benchmark(), indent=2))
        return

    for line in sys.stdin:
        if not line.strip(): continue
        try:
            req = json.loads(line)
            method = req.get("method")
            msg_id = req.get("id")
            
            if method == "initialize":
                resp = {"jsonrpc": "2.0", "id": msg_id, "result": {
                    "protocolVersion": "2024-11-05",
                    "serverInfo": {"name": "genpark-agent-tool-dag-speculative-prefetcher-skill", "version": "1.0.0"},
                    "capabilities": {"tools": {}}
                }}
            elif method == "tools/list":
                resp = {"jsonrpc": "2.0", "id": msg_id, "result": {"tools": [
                    {"name": "register_task", "description": "Register DAG task.", "inputSchema": {"type": "object", "properties": {"task_id": {"type": "string"}, "tool_name": {"type": "string"}, "depends_on": {"type": "array"}}}},
                    {"name": "build_topological_plan", "description": "Compute topological parallel waves.", "inputSchema": {"type": "object"}},
                    {"name": "simulate_dag_execution", "description": "Execute DAG pipeline.", "inputSchema": {"type": "object"}},
                    {"name": "run_dag_benchmark", "description": "Run DAG benchmark.", "inputSchema": {"type": "object"}}
                ]}}
            elif method == "tools/call":
                tname = req.get("params", {}).get("name")
                args = req.get("params", {}).get("arguments", {})
                if tname == "register_task":
                    res = dag.register_task(args.get("task_id", "t"), args.get("tool_name", ""), args.get("arguments"), args.get("depends_on"))
                elif tname == "build_topological_plan":
                    res = dag.build_topological_plan()
                elif tname == "simulate_dag_execution":
                    res = dag.simulate_dag_execution()
                else:
                    res = dag.run_dag_benchmark()
                resp = {"jsonrpc": "2.0", "id": msg_id, "result": {"content": [{"type": "text", "text": json.dumps(res, indent=2)}]}}
            else:
                resp = {"jsonrpc": "2.0", "id": msg_id, "error": {"code": -32601, "message": "Method not found"}}
            
            sys.stdout.write(json.dumps(resp) + "\n")
            sys.stdout.flush()
        except Exception as e:
            sys.stdout.write(json.dumps({"jsonrpc": "2.0", "error": {"code": -32000, "message": str(e)}}) + "\n")
            sys.stdout.flush()

if __name__ == "__main__":
    handle_mcp()
